# frozen_string_literal: true

module Flezha
  # One value a graph released on a declared output. `metadata` is what a
  # deployed run would have put on the wire, `flz.*` stamps included.
  LocalOutput = Struct.new(:channel, :payload, :metadata, keyword_init: true)

  # Running a declared relay graph in this process, with no controller.
  #
  # `LocalRelay.start` takes the relay class you deploy and runs the
  # `channels` graph it already declares — no local-mode member on the class,
  # no second declaration. What runs is the deployed machinery: entry decode,
  # edge transforms and keying, one Cell per resolved key with its own
  # mailbox, the retry ladder, and inter-tasklet delivery performed by the
  # runtime.
  #
  #   run = Flezha::LocalRelay.start(WordCount)
  #   run.on_output("totals") { |output| puts output.payload }
  #   run.feed("lines", "the quick brown fox")
  #   run.run_until_idle(5_000)
  #   run.close
  #
  # State is held in memory and nothing is durable. Durability, plugins,
  # placement and migration, the dead-letter queue, observability export,
  # authz and multi-runner scale all need a controller; the run says so on
  # stderr every time it starts. `docs/quickstart.md` walks through it.
  class LocalRelay
    # Bounded so a stopped run's drain thread leaves promptly.
    OUTPUT_POLL_MS = 100

    # Register +klass+'s declared graph and start it locally. Raises naming
    # the capability when the declaration needs a controller.
    def self.start(klass)
      core = Core.create
      begin
        spec = klass.spec
        registry = Registry.new
        Graph.declare(spec, core, registry)
        # Not `start`, which is the one call that dials a controller.
        core.local_start(spec.factory_id, "#{spec.factory_id}-local")
        deps = DispatchDeps.new(registry: registry, bridge: core)
        executor = ThreadExecutor.new(core, deps)
        executor.start
      rescue Exception # rubocop:disable Lint/RescueException
        core.free
        raise
      end
      new(core, executor)
    end

    def initialize(core, executor)
      @core = core
      @executor = executor
      @sinks = {}
      @closed = false
      @drain = Thread.new { drain_outputs }
      @drain.name = "flz-local-output"
    end

    # Deliver every value released on the output +channel+ to the block, in
    # release order. An output channel takes the producing tasklet's name
    # unless the declaration named it.
    def on_output(channel, &callback)
      # Registered with the core first: a channel the declaration does not
      # carry is refused there, naming it, rather than silently accepted.
      @core.local_on_sink(channel)
      @sinks[channel] = callback
      self
    end

    # Feed one value's wire bytes to a declared entry channel.
    def feed(channel, payload)
      @core.local_feed(channel, payload.to_s)
      self
    end

    # Block until the graph has nothing left to do. A budget that elapses
    # first is reported, never passed off as done.
    def run_until_idle(budget_ms)
      @core.local_run_until_idle(budget_ms)
    end

    # End the run and release its Cells. Idempotent.
    def close
      return if @closed

      @closed = true
      begin
        # `join` re-raises whatever the drain thread died of, so the teardown
        # below is an `ensure`: a failed delivery must not strand the context.
        @drain.join(OUTPUT_POLL_MS / 1000.0 * 20)
        # Stop the run first, then take what it released and the drain thread
        # had not reached: a value can be queued for up to one poll interval,
        # and ending the run while it sits there is how an author loses output
        # the graph did produce. Stopping drops the sender, so this ends at the
        # first empty read rather than waiting.
        @core.local_stop
        while (output = @core.local_next_output(0))
          deliver(output)
        end
      ensure
        # Not `Core#shutdown`: that stops the process-wide poll core, and a
        # process may start another local run after this one. The workers
        # leave on the flag instead, which is why it is raised before the
        # drain rather than after it.
        @executor.stop_fetching
        undrained = @executor.stop
        # As the runner does: a worker the bounded drain gave up on may yet
        # call the core, and freeing the context under it is a use-after-free.
        @core.free if undrained.zero?
      end
    end

    private

    # One released value per pass, handed to whoever registered its channel.
    def drain_outputs
      until @closed
        begin
          output = @core.local_next_output(OUTPUT_POLL_MS)
        rescue StandardError
          # The run ended under us; the close path owns the teardown.
          return
        end
        deliver(output) if output
      end
    end

    def deliver(output)
      unless output[:target] == :plugin
        # The core queues what an output released and nothing else; an edge
        # output is an in-process mailbox push that never leaves.
        raise "local mode released a #{output[:target]} output"
      end

      channel = output[:plugin_channel]
      callback = @sinks[channel]
      return if callback.nil?

      callback.call(LocalOutput.new(channel: channel, payload: output[:payload],
                                   metadata: (output[:metadata] || []).to_h))
    end
  end
end
