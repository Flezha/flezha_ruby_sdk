# frozen_string_literal: true

require_relative "test_helper"

# The author's harness, and the property it exists for: declared state is
# mutated only through declared changes, and this SDK has no runtime guard
# enforcing it — so `replay_equals!` is what makes the rule safe.
class TestingTest < Minitest::Test
  class Counter < Flezha::Tasklet
    tasklet_name "harness_counter"
    def init_state(_cell_key) = { total: 0, seen: [] }

    def apply_change(state, change)
      state[:total] += change[:by]
      state[:seen] << change[:by]
    end

    def run(input, ctx)
      ctx.change({ by: Integer(input) })
      ctx.emit(ctx.state[:total])
    end

    def on_timer(_firing, ctx) = ctx.change({ by: 100 })
    def compensate(outputs, ctx) = ctx.change({ by: -outputs.sum })
  end

  # Mutates the state without recording a change: live in this process, gone
  # after a restart, and reported nowhere until then.
  class Unrecorded < Counter
    tasklet_name "harness_unrecorded"
    def run(input, ctx)
      ctx.state[:total] += Integer(input)
      ctx.emit(ctx.state[:total])
    end
  end

  # Records the change but folds it impurely — the fold reads something
  # neither the state nor the change carries, so a replay, where the handler
  # never ran, produces a different answer.
  class ImpureFold < Counter
    tasklet_name "harness_impure"
    def run(input, ctx)
      @handled = (@handled || 0) + 1
      super
    end

    def apply_change(state, change)
      state[:total] += change[:by] * (@handled || 0)
      state[:seen] << change[:by]
    end
  end

  def test_the_harness_runs_the_tasklet_and_records_what_it_produced
    harness = Flezha::Testing::Harness.new(Counter)

    first = harness.feed("3")
    assert_equal [3], first.outputs
    assert_equal [{ by: 3 }], first.changes

    harness.feed("4")
    assert_equal({ total: 7, seen: [3, 4] }, harness.state)
  end

  def test_replay_equals_agrees_for_a_recorded_pure_fold
    harness = Flezha::Testing::Harness.new(Counter)
    %w[1 2 3].each { |input| harness.feed(input) }

    assert harness.replay_equals!
  end

  def test_replay_equals_catches_a_mutation_that_was_never_recorded
    harness = Flezha::Testing::Harness.new(Unrecorded)
    harness.feed("5")

    error = assert_raises(Flezha::Testing::ReplayMismatch) { harness.replay_equals! }
    assert_includes error.message, "harness_unrecorded"
    assert_includes error.message, "without recording a change"
  end

  def test_replay_equals_catches_an_impure_fold
    harness = Flezha::Testing::Harness.new(ImpureFold)
    harness.feed("1")
    harness.feed("1")

    assert_raises(Flezha::Testing::ReplayMismatch) { harness.replay_equals! }
  end

  def test_a_timer_firing_reaches_on_timer
    harness = Flezha::Testing::Harness.new(Counter)
    harness.fire_timer(:processing_time, 900)

    assert_equal 100, harness.state[:total]
    assert harness.replay_equals!
  end

  def test_a_registered_timer_is_anchored_on_now
    inner = Class.new(Flezha::Tasklet) do
      tasklet_name "timer_registrar"
      def run(_input, ctx) = ctx.emit(ctx.timer_after(50))
    end
    harness = Flezha::Testing::Harness.new(inner)

    outcome = harness.feed("x", now: 1_000)

    assert_equal [1_050], outcome.outputs, "the timer anchors on now, never an ambient read"
    assert_equal [{ kind: "processing_time", scheduled_at: 1_050, register: true }], outcome.timers
  end

  def test_compensation_runs_over_recorded_forward_outputs
    harness = Flezha::Testing::Harness.new(Counter)
    harness.feed("10")
    harness.compensate([4, 6])

    assert_equal 0, harness.state[:total]
  end

  # The runner never emits what a compensate hook returns.
  def test_a_compensate_hook_s_return_value_is_not_emitted
    returning = Class.new(Counter) do
      tasklet_name "harness_compensate_returns"
      def compensate(_outputs, _ctx) = "undo"
    end

    assert_empty Flezha::Testing::Harness.new(returning).compensate([1]).outputs
  end

  # The harness restores through the same function the dispatcher answers a
  # restore fold with, so it proves what a warm restart does.
  def test_restore_rebuilds_the_state_from_a_change_log
    harness = Flezha::Testing::Harness.new(Counter)
    harness.restore(changes: [{ by: 2 }, { by: 5 }].map { |c| Flezha::Codec.encode(c) })

    assert_equal({ total: 7, seen: [2, 5] }, harness.state)
  end

  # A handle already resolved to what a controller would have answered.
  def test_a_spawn_answers_a_resolved_handle_and_a_replay_skips_it
    handles = []
    spawner = Class.new(Flezha::Tasklet) do
      tasklet_name "spawner"

      define_method(:run) do |input, ctx|
        handles.replace([ctx.spawn_steps("stateless_loop_steps", input),
                         ctx.spawn_steps("stateless_loop_steps", input, id: "w1")])
        ctx.emit(handles.map { |handle| handle.try_result.to_h })
      end
    end
    harness = Flezha::Testing::Harness.new(spawner)

    unnamed, named = harness.feed("x").outputs.first
    assert_match(/\Astateless_loop-[a-z2-7]{16}\z/, unnamed[:id], "the controller's generated shape")
    assert_equal :spawned, unnamed[:outcome]
    assert_equal({ id: "w1", outcome: :spawned }, named)
    assert_equal [{ id: "", outcome: :skipped_on_replay }, { id: "w1", outcome: :skipped_on_replay }],
                 harness.feed("x", replay: true).outputs.first,
                 "a spawn is an external effect; a replay must not issue it twice"
    error = assert_raises(Flezha::Error) { handles.first.try_result }
    assert_equal "runtime.spawn_handle_released", error.code,
                 "as live, a handle is read inside the invocation that made it"
  end

  def test_stable_id_is_replay_stable_within_an_invocation
    identifier = Class.new(Flezha::Tasklet) do
      tasklet_name "identifier"
      def run(_input, ctx) = ctx.emit([ctx.stable_id, ctx.stable_id])
    end

    first = Flezha::Testing::Harness.new(identifier).feed("x").outputs.first
    second = Flezha::Testing::Harness.new(identifier).feed("x").outputs.first

    assert_equal first, second, "the n-th call of a replayed invocation equals the n-th original"
    refute_equal first[0], first[1], "repeated calls in one invocation differ"
    assert_equal 32, first[0].length
  end

  class Emitter < Flezha::Tasklet
    tasklet_name "harness_emitter"

    def run(input, ctx)
      ctx.emit({ said: input })
      ctx.emit("echo: #{input}")
    end
  end

  # The harness reads its output back as MessagePack, so a string output is
  # reported as the string rather than as the bytes a plugin would receive.
  def test_the_harness_reports_a_string_and_a_map_exactly_as_emitted
    assert_equal [{ said: "hi" }, "echo: hi"], Flezha::Testing::Harness.new(Emitter).feed("hi").outputs
  end

  # The packaged router, driven with the values its merge maps a document and a
  # trade to.
  def test_the_latest_document_router_hands_each_key_each_document_once
    harness = Flezha::Testing::Harness.new(Flezha::RouteLatest)
    document = { limit: 5 }
    fed = [{ document: document }, { traffic: "A" }, { traffic: "B" }, { document: document }, { traffic: "A" }]

    assert_equal [[], [["A", document]], [["B", document]], [], []],
                 fed.map { |input| harness.feed(input).outputs }
    assert_equal "route_latest", Flezha::RouteLatest.definition.name
    assert_equal({ document: document, version: 1, delivered: { A: 1, B: 1 } }, harness.state)
    assert harness.replay_equals!

    harness.feed({ document: { limit: 6 } })
    assert_equal [["A", { limit: 6 }]], harness.feed({ traffic: "A" }).outputs
    assert harness.replay_equals!
  end

  # A Hash key decodes as a Symbol, so a snapshot restore must not make a key
  # that already received the document owed it again.
  def test_a_router_restored_from_a_snapshot_does_not_deliver_twice
    harness = Flezha::Testing::Harness.new(Flezha::RouteLatest)
    harness.feed({ document: "doc" })
    harness.feed({ traffic: "A" })
    harness.restore(snapshot: Flezha::Codec.encode(harness.state))

    assert_empty harness.feed({ traffic: "A" }).outputs
    assert_empty harness.feed({ document: "doc" }).changes
  end

  class Misuses < Flezha::Tasklet
    tasklet_name "misuses"

    def run(input, ctx)
      case input
      when "channel" then ctx.emit(input, channel: "elsewhere")
      when "metadata" then ctx.emit(input, metadata: { "flz.cell" => "forged" })
      else ctx.change({ n: 1 })
      end
    end
  end

  def test_a_runtime_misuse_raises_the_one_error_with_its_code
    {
      "channel" => ["runtime.channel_not_produced", { "tasklet" => "misuses", "channel" => "elsewhere" }],
      "metadata" => ["runtime.reserved_metadata_key", { "key" => "flz.cell" }],
      "change" => ["runtime.change_without_apply_change", { "tasklet" => "misuses" }]
    }.each do |misuse, (code, details)|
      error = assert_raises(Flezha::Error) { Flezha::Testing::Harness.new(Misuses).feed(misuse) }
      assert_equal [code, details], [error.code, error.details]
    end
  end

  def test_a_restore_the_tasklet_cannot_fold_names_the_hook
    definition = Misuses.definition
    [[{ changes: [{ bytes: "".b }] }, "runtime.change_without_apply_change"],
     [{ snapshot: { from_version: 1, bytes: "".b } }, "runtime.hook_missing"]].each do |payload, code|
      error = assert_raises(Flezha::Error) { Flezha::State.fold_restore(definition, payload, nil, "misuses") }
      assert_equal code, error.code
    end
  end

  def test_stable_id_matches_the_cross_language_vector
    assert_equal "bbf81fd4955afd1724203700a240e822", Flezha::Identity.stable_id("instance/op/key".b, 42, 0)
    assert_equal "bbf81fd4965afd1724203700a240e95d", Flezha::Identity.stable_id("instance/op/key".b, 42, 1)
    assert_equal "15915375576d2f389a938dc3a9940c9d", Flezha::Identity.stable_id("".b, 0, 0)
  end
end
