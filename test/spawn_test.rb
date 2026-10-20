# frozen_string_literal: true

require_relative "test_helper"

# A tasklet's spawn: the call answers a handle at once and never raises what
# the controller decided, and a read answers the result or raises the kind of
# failure. The double scripts the core's answers; the native tests drive the
# core itself over a local run, where no control plane listens.
class SpawnTest < Minitest::Test
  FakeCore = Flezha::TestSupport::FakeCore

  def ctx(core, replay: false, offset: nil)
    Flezha::Ctx.new(spec: nil, node: Flezha::TaskNode.new(0, "spawner"), outgoing: [], registered: nil, address: nil,
                    cell_key: nil, change_seq: 0, now: 0, replay: replay, state: nil, bridge: core,
                    token: 7, offset: offset)
  end

  def resolved(id, outcome) = JSON.generate({ "id" => id, "outcome" => outcome })

  def failed(code, **details) = Flezha::Error.new(code, **details)

  def test_a_spawn_without_an_id_asks_for_one_and_reads_the_generated_id
    core = FakeCore.new(spawns: [resolved("worker-k7m2qzxw4pd3fn2a", "spawned")])
    handle = ctx(core, offset: 11).spawn_steps("worker", "p1")

    assert_equal [[7, "worker", "", "p1".b, "", false, 11]], core.spawn_requests,
                 "the activation token, an empty id, the payload's own bytes, no policy and the input offset"
    assert_equal Flezha::SpawnResult.new(id: "worker-k7m2qzxw4pd3fn2a", outcome: :spawned), handle.wait
  end

  def test_a_spawn_with_an_id_spawns_and_then_delivers
    core = FakeCore.new(spawns: [resolved("w1", "spawned"), resolved("w1", "delivered")])
    context = ctx(core)
    first = context.spawn_steps("worker", "p1", id: "w1")
    second = context.spawn_steps("worker", "p2", id: "w1")

    assert_equal Flezha::SpawnResult.new(id: "w1", outcome: :spawned), first.wait
    assert_equal Flezha::SpawnResult.new(id: "w1", outcome: :delivered), second.wait
    assert_equal [%w[w1 p1], %w[w1 p2]],
                 core.spawn_requests.map { |request| [request[2], request[3]] }
  end

  # The payload takes the no-codec output rule `complete` applies: a String is
  # its bytes and anything else its JSON, so the spawned steps relay's `text` or
  # `json` entry decodes it as it decodes a send's payload.
  def test_a_spawn_payload_is_what_a_text_or_json_entry_reads
    core = FakeCore.new(spawns: [resolved("a", "spawned"), resolved("b", "spawned")])
    context = ctx(core)
    context.spawn_steps("place", "o-1")
    context.spawn_steps("place", { "symbol" => "AAPL", "quantity" => 10 })

    text, json = core.spawn_requests.map { |request| request[3] }
    assert_equal "o-1".b, text
    assert_equal Encoding::ASCII_8BIT, text.encoding
    assert_equal({ "symbol" => "AAPL", "quantity" => 10 }, JSON.parse(json))
  end

  def test_the_error_policy_is_sent_and_an_existing_instance_raises_already_running
    core = FakeCore.new(spawns: [failed("relay.already_running", relay: "w1")])
    handle = ctx(core).spawn_steps("worker", "p", id: "w1", if_running: "error")

    assert_equal "error", core.spawn_requests.first[4]
    error = assert_raises(Flezha::Error) { handle.wait }
    assert_equal ["relay.already_running", { "relay" => "w1" }], [error.code, error.details]
  end

  def test_a_spawn_naming_a_flow_factory_raises_factory_not_steps
    refusal = failed("relay.factory_not_steps", factory: "prices", namespace: "default")
    handle = ctx(FakeCore.new(spawns: [refusal])).spawn_steps("prices", "p", id: "w1")

    assert_equal "relay.factory_not_steps", assert_raises(Flezha::Error) { handle.wait }.code
  end

  def test_if_running_without_an_id_raises_the_cores_refusal_and_sends_nothing
    core = FakeCore.new
    error = assert_raises(Flezha::Error) { ctx(core).spawn_steps("worker", "p", if_running: "error") }

    assert_equal "spawn.if_running_without_id", error.code
    assert_empty core.spawn_requests
  end

  # A refusal and a timeout among them: the call returns a handle, and only a
  # read raises, each failure as the one error carrying its own code.
  def test_every_failure_is_raised_by_a_read_as_the_one_error_with_its_code
    {
      "relay.factory_unknown" => { factory: "worker", namespace: "default" },
      "authz.no_role_binding" => {},
      "spawn.timeout" => { seconds: 30 },
      "spawn.unavailable" => {}
    }.each do |code, details|
      handle = ctx(FakeCore.new(spawns: [failed(code, **details)])).spawn_steps("worker", "p")

      error = assert_raises(Flezha::Error) { handle.wait }
      assert_equal [code, details.transform_keys(&:to_s)], [error.code, error.details]
    end
  end

  def test_the_non_waiting_read_answers_nil_until_the_answer_arrives
    core = FakeCore.new(spawns: [nil, resolved("w1", "spawned")])
    handle = ctx(core).spawn_steps("worker", "p", id: "w1")

    assert_nil handle.try_result
    assert_equal :spawned, handle.try_result.outcome
    assert_equal [[1, 0], [1, 0]], core.spawn_reads, "a non-waiting read never waits"
  end

  def test_a_resolved_handle_answers_every_later_read_the_same
    core = FakeCore.new(spawns: [failed("authz.no_role_binding")])
    handle = ctx(core).spawn_steps("worker", "p", id: "w1")

    assert_equal "authz.no_role_binding", assert_raises(Flezha::Error) { handle.wait }.code
    assert_equal "authz.no_role_binding", assert_raises(Flezha::Error) { handle.try_result }.code
    assert_equal [[1, Flezha::Native::WAIT_CORE_BOUND], [1, 0]], core.spawn_reads, "every read asks the core"
  end

  def test_a_wait_asks_the_core_once_to_its_own_bound
    core = FakeCore.new(spawns: [resolved("w1", "spawned")])
    ctx(core).spawn_steps("worker", "p", id: "w1").wait

    assert_equal [[1, Flezha::Native::WAIT_CORE_BOUND]], core.spawn_reads
  end

  # The wait is in the core, not in the scheduler, so under the fiber executor
  # it answers on the dispatch thread like any other blocking core call.
  def test_a_wait_returns_under_the_fiber_executor
    item = Flezha::TestSupport.input_item("p")
    core = FakeCore.new(activations: [Flezha::TestSupport.activation(factory: "waiter_op", items: [item])],
                        spawns: [resolved("w1", "spawned")])
    deps = Flezha::TestSupport.deps(WaiterRelay, core)
    executor = Flezha::FiberExecutor.new(core, deps).start
    begin
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
      sleep(0.01) until core.completed.any? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      outputs = core.completed.flat_map { |result| result[:effects] }.flat_map { |effect| effect[:outputs] }
      assert_equal ["spawned"], outputs.map { |output| output[:payload] }
    ensure
      core.shutdown
      executor.stop
    end
  end

  class Waiter < Flezha::Tasklet
    tasklet_name "waiter"
    def run(input, ctx) = ctx.emit(ctx.spawn_steps("worker", input, id: "w1").wait.outcome.to_s)
  end

  class WaiterRelay < Flezha::Relay
    factory_id "waiter_op"
    channels { |c| c.input("in", codec: :text).into(Waiter).output("out") }
  end

  # -- against the core -----------------------------------------------------

  RECORDED = Thread::Queue.new

  # Reads a spawn without an id and one with, reads the second again, then
  # leaves a third unread and hands it out, and emits its input.
  class Spawner < Flezha::Tasklet
    tasklet_name "spawner"

    def run(input, ctx)
      unnamed = ctx.spawn_steps("worker", input)
      named = ctx.spawn_steps("worker", input, id: "w1")
      [-> { unnamed.wait }, -> { named.wait }, -> { named.try_result }].each do |read|
        RECORDED << begin
          read.call
        rescue Flezha::Error => e
          e
        end
      end
      RECORDED << ctx.spawn_steps("worker", input, id: "unread")
      ctx.emit(input)
    end
  end

  class SpawnerRelay < Flezha::Relay
    factory_id "spawner_op"
    channels { |c| c.input("in", codec: :text).into(Spawner).output("out") }
  end

  # One input polled from a local run and dispatched the way a worker does,
  # live or as a replay; yields the core, the three read results and the
  # unread handle, before the core is released.
  def dispatch_one(replay:)
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    core = Flezha::Core.create
    registry = Flezha::Registry.new
    Flezha::Graph.declare(SpawnerRelay.spec, core, registry)
    core.local_start(SpawnerRelay.factory_id, "spawner-local")
    core.local_on_sink("out")
    core.local_feed("in", "p")
    activation = core.poll(5_000)
    activation[:items].each { |item| item[:replay] = replay }
    # Completing the activation releases its handles, and the core queues the
    # unread one's failure as a notice for the SDK logger.
    log, default_log = Flezha::TestSupport::CapturedLog.install
    begin
      Flezha::Dispatch.dispatch(activation, core, Flezha::DispatchDeps.new(registry: registry, bridge: core))
      Flezha.log_notices(core.take_notices)
    ensure
      Flezha.log = default_log
    end
    assert_unread_failure_logged_once(log.lines, replay: replay)
    yield core, *Array.new(4) { RECORDED.pop(timeout: 5) }
  ensure
    core&.local_stop
    core&.free
  end

  # The local run logs its banner; a live invocation also logs the one failure
  # nobody read, and a replay requested nothing, so it has nothing to log.
  def assert_unread_failure_logged_once(lines, replay:)
    banner, *rest = lines
    assert_equal :error, banner[0]
    assert banner[1].start_with?("LOCAL MODE: "), banner[1]
    return assert_empty(rest) if replay

    assert_equal 1, rest.size, lines.inspect
    assert_match(/\Aspawn_steps\(factory `worker`, id `unread`\) failed \(spawn\.unavailable\): .*no control plane/,
                 rest[0][1])
  end

  def test_without_a_control_plane_a_read_raises_unavailable
    dispatch_one(replay: false) do |_core, unnamed, named|
      assert_equal %w[spawn.unavailable spawn.unavailable], [unnamed.code, named.code]
    end
  end

  def test_a_resolved_handle_answers_every_read_inside_its_invocation_the_same
    dispatch_one(replay: false) do |_core, _unnamed, named, again|
      assert_equal [named.code, named.message], [again.code, again.message]
    end
  end

  def test_a_replayed_spawn_sends_nothing_and_answers_the_given_id_or_an_empty_one
    dispatch_one(replay: true) do |_core, unnamed, named|
      assert_equal Flezha::SpawnResult.new(id: "", outcome: :skipped_on_replay), unnamed
      assert_equal Flezha::SpawnResult.new(id: "w1", outcome: :skipped_on_replay), named
    end
  end

  def test_an_unread_failure_is_logged_once_and_the_invocation_still_ships_its_emit
    released = nil
    dispatch_one(replay: false) { |core| released = core.local_next_output(5_000) }

    assert_equal "p", released&.fetch(:payload)
  end

  def test_a_handle_read_after_its_invocation_completed_answers_released
    dispatch_one(replay: false) do |_core, _unnamed, _named, _again, unread|
      assert_equal "runtime.spawn_handle_released", assert_raises(Flezha::Error) { unread.try_result }.code
    end
  end
end
