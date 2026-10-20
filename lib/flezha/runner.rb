# frozen_string_literal: true

require "json"

require_relative "dispatch"
require_relative "executor"
require_relative "graph"
require_relative "logger"
require_relative "native"
require_relative "relay"

module Flezha
  # The runner: what an author starts, and everything it owns while running.
  #
  # Classes are registered explicitly, by naming them. There is no autoload, no
  # load-path walk and no package scan: what runs is what the author said runs,
  # and a class that is defined but not registered never reaches the core.
  #
  # ## Startup order
  #
  #   declare classes -> warm up -> (fork, if the user forks) -> start
  #
  # `Process.warmup` compacts the heap and promotes the declared object graph
  # to the old generation, which is what makes a subsequent fork
  # copy-on-write-friendly. It runs after the last declaration and before the
  # first `poll`. The core must NOT be started before a fork: it owns native
  # threads that `fork(2)` does not carry, and `Core#guard!` raises naming that
  # rule rather than letting a child serve nothing.
  class Runner
    attr_reader :namespace

    def initialize(namespace)
      @namespace = namespace
      @classes = []
      @registry = Registry.new
      @core = nil
      @executor = nil
      @executor_kind = (ENV["FLEZHA_RUBY_EXECUTOR"] || "threads").to_sym
      @executor_options = {}
      @yjit = nil
      @stopped = Thread::Queue.new
      @stopping = false
      @closing = Mutex.new
      @teardown = []
    end

    # Register a relay class, short form or full. After start the class is
    # declared at once: the executor serves it and the controller learns of it.
    def register_factory(klass)
      # A tasklet is registered by the relay that wires it.
      unless klass.is_a?(Class) && klass < Relay
        raise Error.new("topology.no_relay_exported", module: klass.to_s)
      end

      klass.check_declaration!
      Graph.declare(klass.spec, @core, @registry) if @core
      @classes << klass
      self
    end

    # Undo a registration — the inverse of {#register_factory}, taking what it
    # took. After start the core refuses a class with live instances on this
    # runner; otherwise the executor stops serving it and the controller
    # learns of it.
    def unregister_factory(klass)
      factory = klass.respond_to?(:factory_id) && klass.factory_id || klass.to_s
      raise Error.new("runtime.class_not_registered", factory: factory) unless @classes.include?(klass)

      if @core
        @core.undeclare_class(factory)
        @registry.remove(factory)
      end
      @classes.delete(klass)
      self
    end

    # Which executor dispatches: `:threads` (the default) or `:fibers`.
    def executor(kind, **options)
      @executor_kind = kind.to_sym
      @executor_options = options
      self
    end

    def logger(logger)
      Flezha.log = logger
      self
    end

    # Administer this namespace from ordinary application code: start and stop
    # relays, enable and disable plugins, read what is running.
    #
    # The controller judges every call as the identity this connection proved,
    # so no credential is held here and nothing expires. A user grants a
    # namespace's applications the authority once, with
    # `flz role bind runner@<namespace> app <namespace>`; until then every call
    # is refused.
    def control = Control.new(@core || Core.context_free)

    # Send `message` to the entry channel `channel` of `relay_id`, through
    # the controller under the same role check as `control`, answering its
    # verdict: `{"to" => relay, "verdict" => "delivered" | "absorbed" |
    # "suspended"}`. `msg_id:` is stamped as `flz.msg_id`, so a relay whose
    # input log still holds that id absorbs the repeat; `key:` reaches the
    # partition group member whose range owns it.
    #
    # `local: true`: in process, without the controller: the message is on no
    # disk, is lost if the runner stops before handling it, and a relay's
    # input log does not cover it. It answers `nil`.
    #
    # Named `send_to` because `send` is every Ruby object's own.
    def send_to(relay_id, message, channel, msg_id: nil, key: nil, local: false)
      JSON.parse((@core || Core.context_free).send_message(relay_id, channel, message.b, msg_id, key, local))
    end

    # Call the entry channel `channel` of `relay_id` with `value`, through the
    # controller under the same role check as `send_to`, answering what the
    # relay answers: a JSON value, a String for text, binary bytes for the
    # binary formats, or `{"accepted" => true}` from a channel no output
    # answers. `value` is encoded as an output naming no codec encodes it.
    def call(relay_id, channel, value)
      payload = [Codec.plain_payload(value, "a call to '#{relay_id}'")].pack("m0")
      args = JSON.generate([relay_id, channel, payload, { "payload" => "base64" }])
      reply = JSON.parse((@core || Core.context_free).admin_call("Relay.call_channel", args))
      return { "accepted" => true } if reply["accepted"]

      %w[json text].include?(reply["answer_codec"]) ? reply["answer"] : reply["answer"].unpack1("m0")
    end

    # The ids of the relay instances bound on this runner now, read without
    # the controller. Refused with `runtime.runner_not_started` before `boot`.
    def relays = (@core || raise(Error.new("runtime.runner_not_started"))).relays

    # Override the YJIT decision. `nil` (the default) means "decide", `false`
    # means "leave it off".
    def yjit(enabled)
      @yjit = enabled
      self
    end

    # Declare, warm up, connect and start serving — everything up to and
    # including the first poll, and nothing that blocks.
    #
    # Separate from `run` so a host with its own supervision tree can own the
    # waiting, and so the startup ORDER is a thing a test can observe rather
    # than a comment.
    def start(core: nil)
      jit = resolve_yjit

      # Before anything else: a native library built against a different
      # activation schema is refused here, with no controller connection
      # attempted.
      @core = core || Core.create

      declare_all
      # After the last declaration and before the first poll.
      Process.warmup

      # The core's termination handler also runs the handler installed before
      # it. Ruby's default raises in the main thread and ends the process before
      # any state is shipped, so it gives way to the system default, which is
      # never run; a trap the application set is kept.
      %w[TERM INT].each do |signal|
        previous = Signal.trap(signal, "SYSTEM_DEFAULT")
        Signal.trap(signal, previous) unless previous == "DEFAULT"
      end
      begin
        @core.start(@namespace)
      rescue StandardError
        # The core holds what it said while connecting until a take; no
        # supervisor is left to take it.
        Flezha.log_notices(@core.take_notices)
        raise
      end
      observe_shutdown

      @executor = build_executor
      @executor.start

      Flezha.log.info(
        "runner up on namespace #{@namespace}: #{@executor.name} executor, " \
        "#{@executor.size} workers, yjit #{jit}"
      )
      self
    end

    # Declare, warm up, connect and serve until shutdown: `start`, then
    # `await_termination`.
    def run(core: nil)
      start(core: core)
      await_termination
    end

    # Wait for shutdown, then close. A host that called `start` can work
    # between the two, making control calls for one, and still stop the way
    # `run` does.
    def await_termination
      @stopped.pop
      close
      self
    end

    # Stop fetching, drain what is in flight, release the core and wake every
    # thread in `await_termination`. Idempotent: the waiter it wakes finds the
    # core already released. Not for a trap handler, since it takes a lock.
    def close
      @closing.synchronize { release_core }
      stop
      nil
    end

    private

    # Wakes `await_termination`. Trap-safe: a queue push and no lock.
    def stop
      return if @stopping

      @stopping = true
      @stopped.push(:stop)
    end

    def release_core
      return unless @core

      Flezha.log.info("draining")
      @core.shutdown
      undrained = @executor ? @executor.stop : 0
      @teardown.each(&:call)
      @teardown.clear
      # The context is released only when nothing is still running against it.
      # A worker the bounded drain gave up on is inside user code and may yet
      # call the core; freeing the context under it would be a use-after-free
      # in native code. The process is on its way out either way, so the
      # kernel reclaims what this skips.
      @core.free if undrained.zero?
      @core = nil
    end

    def declare_all
      # The verdict before the registration: the core judges the record and
      # this SDK renders it, so an author meets a declaration refusal rather
      # than a native failure at the first connection.
      @classes.each { |klass| Graph.declare(klass.spec, @core, @registry) }
      raise Error.new("runtime.no_relays") if @registry.empty?
    end

    def build_executor
      deps = DispatchDeps.new(registry: @registry, bridge: @core)
      case @executor_kind
      when :threads then ThreadExecutor.new(@core, deps, **@executor_options)
      when :fibers then FiberExecutor.new(@core, deps, **@executor_options)
      else
        raise Error.new("usage.unknown_word", noun: "executor", value: @executor_kind.to_s,
                                              allowed: %w[threads fibers])
      end
    end

    # Shutdown is observed three ways, because the harness, a user and the
    # controller each have their own: end of file on standard input, a signal,
    # and the core's own stop — which every worker sees as `:shutting_down`
    # from its next poll.
    #
    # The first two are the core's, on a thread that holds no GVL and no Ruby
    # object: it installs the SIGINT and SIGTERM handler when the runner starts,
    # and the watch below is the same exit by another name. The harness manages
    # a runner as an Erlang Port and stops it by closing the port, which is this
    # process's standard input.
    def observe_shutdown
      @core.watch_stdin_eof unless $stdin.tty?
    end

    # YJIT is on by default because handler bodies are the one thing the GVL
    # bounds, so the JIT's gain lands exactly where the ceiling is. The
    # user's decision wins, and the resulting state is reported in the
    # startup line so a measurement can never be ambiguous about it.
    def resolve_yjit
      return "disabled (runner override)" if @yjit == false
      return "unavailable (this Ruby was built without it)" unless defined?(RubyVM::YJIT)
      return "enabled (already on)" if RubyVM::YJIT.enabled?
      return "disabled (RUBY_YJIT_ENABLE set)" if ENV.key?("RUBY_YJIT_ENABLE")
      return "disabled (FLEZHA_YJIT=0)" if ENV["FLEZHA_YJIT"] == "0"

      RubyVM::YJIT.enable
      RubyVM::YJIT.enabled? ? "enabled" : "unavailable (enable refused)"
    end
  end

  # Start here: `Flezha.create_runner("default").register_factory(MyRelay).run`.
  def self.create_runner(namespace) = Runner.new(namespace)
end
