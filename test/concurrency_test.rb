# frozen_string_literal: true

require_relative "test_helper"

# The concurrency requirements as behaviour, not prose: the pool the core's
# governor asks for, grown seats retiring afterwards, a dead worker replaced,
# the GVL released around a blocking core call, a refused post-fork call, and
# identical ordering under both executors.
class ConcurrencyTest < Minitest::Test
  class Echo < Flezha::Tasklet
    tasklet_name "echo"
    def run(input, ctx) = ctx.emit(input)
  end

  class EchoRelay < Flezha::Relay
    factory_id "echo_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(Echo).output("out", encode: ->(v) { v.to_s.b })
    end
  end

  def deps(core) = Flezha::TestSupport.deps(EchoRelay, core)

  def activation(token, payload)
    Flezha::TestSupport.activation(
      factory: "echo_op", token: token, relay: "op",
      items: [Flezha::TestSupport.input_item(payload, now: 0)]
    )
  end

  def wait_until(timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    sleep(0.01) until yield || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
    yield
  end

  # -- sizing ---------------------------------------------------------------

  def with_env(values)
    previous = values.keys.to_h { |key| [key, ENV[key]] }
    values.each { |key, value| ENV[key] = value }
    yield
  ensure
    previous.each { |key, value| ENV[key] = value }
  end

  # The policy one context's pool opens with; a context holds one pool.
  def pool_opened(default_base)
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)
    policy = Flezha::Native.pool_open(default_base)
    refused = assert_raises(Flezha::Error) { Flezha::Native.pool_open(default_base) }
    assert_equal "runtime.runner_already_running", refused.code
    policy[0, 2]
  ensure
    Flezha::Native.context_free
  end

  # The core's governor reads both knobs by the one rule, over this host's default.
  def test_the_pool_is_sized_by_the_core_over_this_hosts_default
    skip "needs the compiled veneer" unless Flezha::TestSupport.native?

    with_env("FLEZHA_WORKER_POOL_SIZE" => "3", "FLEZHA_WORKER_POOL_MAX" => "7") do
      assert_equal [3, 7], pool_opened(4)
    end
    with_env("FLEZHA_WORKER_POOL_SIZE" => "0", "FLEZHA_WORKER_POOL_MAX" => "2") do
      assert_equal [4, 4], pool_opened(4),
                   "an unparsed base is unset, and a ceiling under the floor would retire the pool"
    end
  end

  # -- the governor decides, this host spawns -------------------------------

  def test_a_workload_that_never_starves_runs_on_the_base_pool
    core = Flezha::TestSupport::FakeCore.new(activations: Array.new(4) { |i| activation(i, "x") })
    executor = Flezha::ThreadExecutor.new(core, deps(core)).start
    begin
      assert wait_until { core.completed.size == 4 }
      assert_equal 2, executor.size, "no worker is started that the governor did not ask for"
    ensure
      core.shutdown
      executor.stop
    end
  end

  def test_a_grown_seat_serves_and_retires_once_the_work_ends
    release = Thread::Queue.new
    core = Flezha::TestSupport::FakeCore.new(
      activations: Array.new(6) { |i| activation(i, "x") }, base: 1, retire_ms: 500,
      hold: -> { release.pop }
    )
    executor = Flezha::ThreadExecutor.new(core, deps(core)).start
    begin
      2.times { core.decisions.push(Flezha::Executor::POOL_SPAWN_GROWN) }
      assert wait_until { executor.size == 3 }, "each grown seat is a worker"
      6.times { release.push(:go) }
      assert wait_until { core.completed.size == 6 }
      # Two timed-out polls of the core's 250 ms are the 500 ms retire time.
      assert wait_until { executor.size == 1 }, "the grown workers retire and the base stays"
      assert_equal 2, core.seats_left, "every retired worker gives its seat back"
    ensure
      core.shutdown
      executor.stop
    end
  end

  # A worker that dies gives its seat back; the governor's base answer is what
  # replaces it, and the replacement serves.
  def test_a_base_seat_replaces_a_worker_that_died
    dies_once = Class.new(Flezha::TestSupport::FakeCore) do
      def poll(timeout_ms)
        raise "the worker's poll broke" unless @died

        super
      ensure
        @died = true
      end
    end
    core = dies_once.new(base: 1)
    log, previous = Flezha::TestSupport::CapturedLog.install
    executor = Flezha::ThreadExecutor.new(core, deps(core)).start
    begin
      assert wait_until { core.seats_left == 1 && executor.size.zero? }, "the dead worker's seat is back"
      core.decisions.push(Flezha::Executor::POOL_SPAWN_BASE)
      core.offer(activation(1, "x"))
      assert wait_until { core.completed.size == 1 }, "the replacement serves"
      assert(log.lines.any? { |level, line| level == :error && line.include?("the worker's poll broke") })
    ensure
      Flezha.log = previous
      core.shutdown
      executor.stop
    end
  end

  # An idle runner still writes what the core said: the supervisor drains the
  # notices on every answer, idle ones included.
  def test_an_idle_runner_writes_the_cores_notices_on_each_supervisor_answer
    core = Flezha::TestSupport::FakeCore.new(base: 1)
    log, previous = Flezha::TestSupport::CapturedLog.install
    executor = Flezha::ThreadExecutor.new(core, deps(core)).start
    begin
      core.notice(["warn", "flz_core::control_unit", "Connection attempt 2 failed"])
      assert wait_until(timeout: 2) { log.lines.any? { |_, line| line.include?("Connection attempt 2 failed") } }
    ensure
      Flezha.log = previous
      core.shutdown
      executor.stop
    end
  end

  # -- the seam's contract --------------------------------------------------

  def test_both_executors_complete_every_activation_and_keep_item_order
    answers = {}
    %i[threads fibers].each do |kind|
      core = Flezha::TestSupport::FakeCore.new(
        activations: Array.new(8) { |i| activation(i, "item-#{i}") }, base: 3
      )
      executor =
        if kind == :threads
          Flezha::ThreadExecutor.new(core, deps(core))
        else
          Flezha::FiberExecutor.new(core, deps(core))
        end
      executor.start
      begin
        assert wait_until { core.completed.size == 8 }, "#{kind} completed everything"
      ensure
        core.shutdown
        executor.stop
      end
      answers[kind] = core.completed.sort_by { |r| r[:token] }.map do |result|
        [result[:token], result[:effects].map { |e| e[:outputs].map { |o| o[:payload] } }]
      end
    end

    assert_equal answers[:threads], answers[:fibers],
                 "output identity is identical under both executors"
  end

  # The seam's whole contract: an activation is completed only after its
  # dispatch has returned. An executor that completed early would break
  # per-cell ordering, which is the one thing it may never touch.
  ORDER = []
  ORDER_LOCK = Mutex.new

  class Recorder < Flezha::Tasklet
    tasklet_name "recorder"
    def run(input, ctx)
      ORDER_LOCK.synchronize { ORDER << :dispatched }
      ctx.emit(input)
    end
  end

  class RecorderRelay < Flezha::Relay
    factory_id "recorder_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(Recorder).output("out", encode: ->(v) { v.to_s.b })
    end
  end

  def test_an_activation_is_completed_only_after_its_dispatch_returns
    ORDER.clear
    core = Flezha::TestSupport::FakeCore.new(
      activations: [activation(1, "x").merge(factory: "recorder_op")], base: 1,
      hold: -> { ORDER_LOCK.synchronize { ORDER << :completed } }
    )
    executor = Flezha::ThreadExecutor.new(core, Flezha::TestSupport.deps(RecorderRelay, core)).start
    begin
      assert wait_until { ORDER.size == 2 }
      assert_equal %i[dispatched completed], ORDER
    ensure
      core.shutdown
      executor.stop
    end
  end

  # A handler whose I/O yields to the fiber scheduler: a 10 ms sleep.
  class Napper < Flezha::Tasklet
    tasklet_name "napper"
    def run(input, ctx)
      sleep 0.01
      ctx.emit(input)
    end
  end

  class NapperRelay < Flezha::Relay
    factory_id "napper_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(Napper).output("out", encode: ->(v) { v.to_s.b })
    end
  end

  # One activation and then a quiet queue: the handler that yielded must be
  # resumed by the scheduler, not by the next activation or the executor's stop.
  def test_a_handler_that_yields_is_resumed_while_the_queue_is_quiet
    core = Flezha::TestSupport::FakeCore.new(activations: [activation(1, "x").merge(factory: "napper_op")], base: 1)
    executor = Flezha::FiberExecutor.new(core, Flezha::TestSupport.deps(NapperRelay, core)).start
    begin
      assert wait_until(timeout: 2) { core.completed.size == 1 },
             "a 10 ms sleep under the fiber executor was not resumed while the queue was quiet"
      assert_equal [["x"]], core.completed.first[:effects].map { |e| e[:outputs].map { |o| o[:payload] } }
    ensure
      core.shutdown
      executor.stop
    end
  end

  # -- the drain is bounded -------------------------------------------------

  class NeverReturns < Flezha::Tasklet
    tasklet_name "never_returns"
    def run(_input, _ctx) = sleep
  end

  class NeverReturnsRelay < Flezha::Relay
    factory_id "never_returns_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(NeverReturns).output("out", encode: ->(v) { v.to_s.b })
    end
  end

  def never_returns_deps(core) = Flezha::TestSupport.deps(NeverReturnsRelay, core)

  # A handler that never returns must not keep the runner alive forever. The
  # harness stops runners by closing their pipes, so an unbounded drain leaks a
  # process per wedge — and the process that leaks is the one holding the
  # namespace the next test wants.
  def test_a_handler_that_never_returns_does_not_hold_the_drain_open
    core = Flezha::TestSupport::FakeCore.new(
      activations: [activation(1, "x").merge(factory: "never_returns_op")], base: 1, drain_ms: 300
    )
    executor = Flezha::ThreadExecutor.new(core, never_returns_deps(core)).start
    assert wait_until { core.polled_once? }, "the worker picked the activation up"

    elapsed, stderr = timed_stop(core, executor)

    assert_operator elapsed, :<, 0.3 + 3, "the drain waited #{elapsed}s on a handler that never returns"
    assert_reports_one_undrained(stderr)
  end

  def test_the_same_bound_holds_under_the_fiber_executor
    core = Flezha::TestSupport::FakeCore.new(
      activations: [activation(1, "x").merge(factory: "never_returns_op")], base: 1, drain_ms: 300
    )
    executor = Flezha::FiberExecutor.new(core, never_returns_deps(core)).start
    assert wait_until { core.polled_once? }

    elapsed, stderr = timed_stop(core, executor)

    assert_operator elapsed, :<, 0.3 + 3, "the drain waited #{elapsed}s on a fiber that never returns"
    assert_reports_one_undrained(stderr)
  end

  # Shuts the core down and stops the executor; the seconds the stop took and
  # what it wrote to standard error.
  def timed_stop(core, executor)
    core.shutdown
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    _, stderr = capture_io { executor.stop }
    [Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, stderr]
  end

  def assert_reports_one_undrained(stderr)
    assert_equal ["[flezha] 1 activation(s) were still running after the 0.3s drain; " \
                  "the runner is exiting anyway. The core re-hands unanswered work on the next bind — find the " \
                  "handler that does not return.\n"], stderr.lines
  end

  # -- the process rules ----------------------------------------------------

  def test_a_core_call_after_a_fork_is_refused_naming_the_rule
    skip "needs the compiled veneer" unless Flezha::TestSupport.native?

    core = Flezha::Core.allocate
    core.instance_variable_set(:@owner_pid, Process.pid - 1)

    error = assert_raises(Flezha::Error) { core.guard! }
    assert_equal ["runtime.forked_process", { "owner" => Process.pid - 1, "pid" => Process.pid }],
                 [error.code, error.details]
    assert_includes error.message, "start the core in each child"
  end

  def test_a_core_call_in_the_owning_process_is_allowed
    core = Flezha::Core.allocate
    core.instance_variable_set(:@owner_pid, Process.pid)

    assert_nil core.guard!
  end

  # -- Ractor-local state ---------------------------------------------------

  # All per-VM SDK state resolves per Ractor, so a Ractor executor can be added
  # without restructuring. A bare mutable global is exactly what would have to
  # be unpicked, so this is a scan rather than a promise.
  SOURCES = Dir[File.expand_path("../lib/**/*.rb", __dir__)].sort

  def test_no_source_file_holds_a_mutable_global_or_class_variable
    offenders = SOURCES.each_with_object({}) do |path, found|
      File.readlines(path).each_with_index do |line, index|
        next if line.lstrip.start_with?("#")
        next unless line.match?(/(^|[^$\w])\$[a-z_]\w*\s*=(?!=)/) || line.match?(/@@\w+/)

        # `$stdin` / `$stderr` are read, never rebound, by the logger and the
        # tty check; the pattern above only matches an assignment.
        (found[path] ||= []) << index + 1
      end
    end

    assert_empty offenders, "a mutable global holds SDK state at #{offenders.inspect}"
    refute_empty SOURCES
  end

  def test_the_logger_resolves_per_ractor
    assert_same Flezha.log, Ractor.current[:flezha_logger]

    replacement = Flezha::ConsoleLogger.new(:error)
    Flezha.log = replacement
    assert_same replacement, Ractor.current[:flezha_logger]
  ensure
    Ractor.current[:flezha_logger] = nil
  end
end
