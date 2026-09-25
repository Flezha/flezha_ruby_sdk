# frozen_string_literal: true

require_relative "test_helper"

# A tasklet's spawn: the call answers a handle at once and never raises what
# the controller decided, and a read answers the result or raises the kind of
# failure. The double scripts the core's answers; the native tests drive the
# core itself over a local run, where no control plane listens.
class SpawnTest < Minitest::Test
  FakeCore = Flezha::TestSupport::FakeCore

  def ctx(core, replay: false)
    Flezha::Ctx.new(spec: nil, node: Flezha::TaskNode.new(0, "spawner"), outgoing: [], operator: "op",
                    cell_key: nil, change_seq: 0, now: 0, replay: replay, state: nil, bridge: core,
                    token: 7)
  end

  def resolved(id, outcome) = JSON.generate({ "id" => id, "outcome" => outcome })

  def failed(id, kind)
    JSON.generate({ "id" => id, "error" => kind, "message" => "the #{kind} sentence" })
  end

  def test_a_spawn_without_an_id_asks_for_one_and_reads_the_generated_id
    core = FakeCore.new(spawns: [resolved("worker-k7m2qzxw4pd3fn2a", "spawned")])
    handle = ctx(core).spawn_bounded("worker", "p1")

    assert_equal [[7, "worker", "", "p1".b, "", false]], core.spawn_requests,
                 "the activation token, an empty id, the payload's own bytes and no policy"
    assert_equal Flezha::SpawnResult.new(id: "worker-k7m2qzxw4pd3fn2a", outcome: :spawned), handle.wait
  end

  def test_a_spawn_with_an_id_spawns_and_then_delivers
    core = FakeCore.new(spawns: [resolved("w1", "spawned"), resolved("w1", "delivered")])
    context = ctx(core)
    first = context.spawn_bounded("worker", "p1", id: "w1")
    second = context.spawn_bounded("worker", "p2", id: "w1")

    assert_equal Flezha::SpawnResult.new(id: "w1", outcome: :spawned), first.wait
    assert_equal Flezha::SpawnResult.new(id: "w1", outcome: :delivered), second.wait
    assert_equal [%w[w1 p1], %w[w1 p2]],
                 core.spawn_requests.map { |request| [request[2], request[3]] }
  end

  # The payload takes the no-codec sink rule `complete` applies: a String is
  # its bytes and anything else its JSON, so the spawned bounded operator's `text` or
  # `json` entry decodes it as it decodes a send's payload.
  def test_a_spawn_payload_is_what_a_text_or_json_entry_reads
    core = FakeCore.new(spawns: [resolved("a", "spawned"), resolved("b", "spawned")])
    context = ctx(core)
    context.spawn_bounded("place", "o-1")
    context.spawn_bounded("place", { "symbol" => "AAPL", "quantity" => 10 })

    text, json = core.spawn_requests.map { |request| request[3] }
    assert_equal "o-1".b, text
    assert_equal Encoding::ASCII_8BIT, text.encoding
    assert_equal({ "symbol" => "AAPL", "quantity" => 10 }, JSON.parse(json))
  end

  def test_the_error_policy_is_sent_and_an_existing_instance_raises_already_running
    core = FakeCore.new(spawns: [failed("w1", "already_running")])
    handle = ctx(core).spawn_bounded("worker", "p", id: "w1", if_running: "error")

    assert_equal "error", core.spawn_requests.first[4]
    error = assert_raises(Flezha::SpawnError::AlreadyRunning) { handle.wait }
    assert_equal "w1", error.id
    assert_equal :already_running, error.kind
  end

  def test_a_spawn_naming_an_unbounded_factory_raises_factory_not_bounded
    handle = ctx(FakeCore.new(spawns: [failed("w1", "factory_not_bounded")]))
             .spawn_bounded("prices", "p", id: "w1")

    error = assert_raises(Flezha::SpawnError::FactoryNotBounded) { handle.wait }
    assert_equal :factory_not_bounded, error.kind
  end

  def test_if_running_without_an_id_raises_at_the_call_and_sends_nothing
    core = FakeCore.new
    error = assert_raises(ArgumentError) { ctx(core).spawn_bounded("worker", "p", if_running: "error") }

    assert_includes error.message, "spawner: "
    assert_includes error.message, "if_running without an id"
    assert_empty core.spawn_requests
  end

  # A refusal and a timeout among them: the call returns a handle, and only a
  # read raises, each kind as its own class under the SDK's hierarchy.
  def test_every_failure_kind_is_raised_by_a_read_as_its_own_class
    Flezha::SpawnError::KINDS.each do |kind, klass|
      handle = ctx(FakeCore.new(spawns: [failed("", kind.to_s)])).spawn_bounded("worker", "p")

      error = assert_raises(klass) { handle.wait }
      assert_kind_of Flezha::Error, error
      assert_equal kind, error.kind
      assert_equal "the #{kind} sentence", error.message
      assert_equal "", error.id
    end
  end

  def test_the_non_waiting_read_answers_nil_until_the_answer_arrives
    core = FakeCore.new(spawns: [nil, resolved("w1", "spawned")])
    handle = ctx(core).spawn_bounded("worker", "p", id: "w1")

    assert_nil handle.try_result
    assert_equal :spawned, handle.try_result.outcome
    assert_equal [[1, 0], [1, 0]], core.spawn_reads, "a non-waiting read never waits"
  end

  def test_a_resolved_handle_answers_every_later_read_the_same
    core = FakeCore.new(spawns: [failed("w1", "refused")])
    handle = ctx(core).spawn_bounded("worker", "p", id: "w1")

    assert_raises(Flezha::SpawnError::Refused) { handle.wait }
    assert_raises(Flezha::SpawnError::Refused) { handle.try_result }
    assert_equal [[1, Flezha::SpawnHandle::WAIT_MS], [1, 0]], core.spawn_reads, "every read asks the core"
  end

  def test_a_wait_waits_in_the_core_and_reads_again_while_pending
    core = FakeCore.new(spawns: [nil, resolved("w1", "spawned")])
    ctx(core).spawn_bounded("worker", "p", id: "w1").wait

    assert_equal [[1, Flezha::SpawnHandle::WAIT_MS]] * 2, core.spawn_reads
  end

  # The fiber executor resumes a fiber that yielded only once another
  # activation arrives, so a wait that yielded to the scheduler would stall.
  def test_a_wait_returns_under_the_fiber_executor
    item = Flezha::TestSupport.input_item("p")
    core = FakeCore.new(activations: [Flezha::TestSupport.activation(factory: "waiter_op", items: [item])],
                        spawns: [nil, resolved("w1", "spawned")])
    deps = Flezha::TestSupport.deps(WaiterOperator, core)
    executor = Flezha::FiberExecutor.new(core, deps, fetchers: 1).start
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
    def run(input, ctx) = ctx.spawn_bounded("worker", input, id: "w1").wait.outcome.to_s
  end

  class WaiterOperator < Flezha::UnboundedOperator
    factory_id "waiter_op"
    channels { |c| c.input("in", codec: :text).into(Waiter).sink("out") }
  end

  # -- against the core -----------------------------------------------------

  RECORDED = Thread::Queue.new

  # Reads a spawn without an id and one with, reads the second again, then
  # leaves a third unread and hands it out, and emits its input.
  class Spawner < Flezha::Tasklet
    tasklet_name "spawner"

    def run(input, ctx)
      unnamed = ctx.spawn_bounded("worker", input)
      named = ctx.spawn_bounded("worker", input, id: "w1")
      [-> { unnamed.wait }, -> { named.wait }, -> { named.try_result }].each do |read|
        RECORDED << begin
          read.call
        rescue Flezha::SpawnError => e
          e
        end
      end
      RECORDED << ctx.spawn_bounded("worker", input, id: "unread")
      input
    end
  end

  class SpawnerOperator < Flezha::UnboundedOperator
    factory_id "spawner_op"
    channels { |c| c.input("in", codec: :text).into(Spawner).sink("out") }
  end

  # One input polled from a local run and dispatched the way a worker does,
  # live or as a replay; yields the core, the three read results and the
  # unread handle, before the core is released.
  def dispatch_one(replay:)
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    core = Flezha::Core.create
    registry = Flezha::Registry.new
    Flezha::Graph.declare(SpawnerOperator.spec, core, registry)
    core.local_start(SpawnerOperator.factory_id, "spawner-local")
    core.local_on_sink("out")
    core.local_feed("in", "p")
    activation = core.poll(5_000)
    activation[:items].each { |item| item[:replay] = replay }
    Flezha::Dispatch.dispatch(activation, core, Flezha::DispatchDeps.new(registry: registry, bridge: core))
    yield core, *Array.new(4) { RECORDED.pop(timeout: 5) }
  ensure
    core&.local_stop
    core&.free
  end

  def test_without_a_control_plane_a_read_raises_unavailable_naming_the_requested_id
    dispatch_one(replay: false) do |_core, unnamed, named|
      assert_instance_of Flezha::SpawnError::Unavailable, unnamed
      assert_equal "", unnamed.id
      assert_instance_of Flezha::SpawnError::Unavailable, named
      assert_equal "w1", named.id
    end
  end

  def test_a_resolved_handle_answers_every_read_inside_its_invocation_the_same
    dispatch_one(replay: false) do |_core, _unnamed, named, again|
      assert_instance_of Flezha::SpawnError::Unavailable, again
      assert_equal [named.id, named.message], [again.id, again.message]
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
    _, stderr = capture_subprocess_io do
      dispatch_one(replay: false) { |core| released = core.local_next_output(5_000) }
    end

    assert_equal "p", released&.fetch(:payload)
    warnings = stderr.lines.grep(/spawn_bounded\(/)
    assert_equal 1, warnings.size, stderr
    assert_includes warnings.first, "`unread`"
    assert_includes warnings.first, "no control plane"
  end

  def test_a_handle_read_after_its_invocation_completed_answers_released
    dispatch_one(replay: false) do |_core, _unnamed, _named, _again, unread|
      error = assert_raises(Flezha::Native::Error) { unread.try_result }
      assert_includes error.message, "released"
    end
  end
end
