# frozen_string_literal: true

require_relative "test_helper"

# Every activation kind the boundary defines is answered with its own outcome.
#
# A kind that fell through to the unknown-kind refusal would retire the cell
# it addressed for the life of the process, and the first sign of it would be a
# stalled operator — so the sweep is over the whole vocabulary rather than the
# kinds one fixture happens to exercise.
class DispatchTest < Minitest::Test
  class Counter < Flezha::Tasklet
    tasklet_name "counter"
    def init_state(_key) = { total: 0, seen: [] }
    def apply_change(state, change) = state[:total] += change[:by]

    def run(input, ctx)
      ctx.change({ by: Integer(input) })
      ctx.emit("total=#{ctx.state[:total]}")
    end

    def on_timer(firing, ctx) = ctx.change({ by: firing[:scheduled_at].zero? ? 0 : 1 })
    def compensate(outputs, ctx) = ctx.change({ by: -outputs.size })
  end

  class CounterOperator < Flezha::UnboundedOperator
    factory_id "counter_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(Counter).sink("out", encode: ->(v) { v.to_s.b })
    end
  end

  def setup
    @core = Flezha::TestSupport::FakeCore.new
    @deps = wired_deps(CounterOperator)
    @registry = @deps.registry
  end

  def wired_deps(operator)
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    Flezha::TestSupport.wired_deps(operator, @core)
  end

  def activation(kind, **extra)
    Flezha::TestSupport.activation(factory: "counter_op", kind: kind, **extra)
  end

  def answer(kind, **extra) = Flezha::Dispatch.answer(activation(kind, **extra), @deps)
  def input_item(payload, **extra) = Flezha::TestSupport.input_item(payload, **extra)

  # One activation answered over a declared class of its own, with fresh state.
  def answer_on(operator, **extra)
    deps = wired_deps(operator)
    Flezha::Dispatch.answer(Flezha::TestSupport.activation(factory: operator.factory_id, **extra), deps)
  end

  def test_a_work_batch_answers_one_effect_per_item
    result = answer(:work, items: [input_item("2"), input_item("3")])

    assert_equal "work", result[:outcome]
    assert_equal 2, result[:effects].size
    assert_equal %w[ok ok], result[:effects].map { |e| e[:verdict] }
    assert_equal 1, result[:effects][0][:outputs].size
    # The sink declares a text encoder, so the frame is the bytes it made.
    assert_equal "total=5", result[:effects][1][:outputs][0][:payload]
  end

  def test_a_raised_handler_stops_the_batch_and_reports_a_retryable_verdict
    result = answer(:work, items: [input_item("1"), input_item("not a number"), input_item("1")])

    assert_equal 2, result[:effects].size, "items after the failure are re-handed by the core"
    assert_equal "retryable", result[:effects][1][:verdict]
    assert_includes result[:effects][1][:message], "ArgumentError"
  end

  def test_a_timer_item_reaches_on_timer_with_its_scheduled_time_as_now
    result = answer(:work, items: [{ kind: :timer, timer_kind: :processing_time,
                                     scheduled_at: 4_242, now: 4_242 }])

    assert_equal "ok", result[:effects][0][:verdict]
    assert_equal 4_242, result[:effects][0][:event_time]
  end

  def test_a_restore_fold_rebuilds_the_state_from_its_change_log
    changes = [{ by: 5 }, { by: 6 }].map { |c| { bytes: Flezha::Codec.encode(c) } }

    assert_equal "restored", answer(:restore_fold, changes: changes)[:outcome]

    result = answer(:work, items: [input_item("1")])
    assert_equal "total=12", result[:effects][0][:outputs][0][:payload]
  end

  def test_a_snapshot_answers_the_encoded_state
    answer(:work, items: [input_item("7")])
    result = answer(:snapshot_request)

    assert_equal "snapshot", result[:outcome]
    assert_equal 7, Flezha::Codec.decode(result[:state])[:total]
  end

  def test_a_live_state_read_and_a_migration_serialize_answer_json
    answer(:work, items: [input_item("3")])

    %i[live_state_read migration_serialize].each do |kind|
      result = answer(kind)
      assert_equal({ live_state_read: "state_json", migration_serialize: "migration_state" }[kind],
                   result[:outcome])
      assert_equal 3, Flezha::Codec.from_state_json(result[:json])[:total]
    end
  end

  def test_apply_state_materializes_a_migrated_state
    json = Flezha::Codec.to_state_json({ total: 99, seen: [] })
    assert_equal "state_applied", answer(:apply_state, state_json: json.b)[:outcome]

    result = answer(:work, items: [input_item("1")])
    assert_equal "total=100", result[:effects][0][:outputs][0][:payload]
  end

  def test_a_key_request_answers_one_prepare_answer_per_payload
    result = answer(:key_request,
                    key_request: { channel: 0, payloads: ["1".b, "2".b],
                                   want_keys: true, want_event_time: false })

    assert_equal "prepared", result[:outcome]
    assert_equal 2, result[:prepared].size
    assert_equal [[], []], result[:prepared].map { |a| a[:keys] },
                 "a single-cell tasklet names no key"
  end

  def test_a_compensate_run_folds_the_recorded_outputs
    result = answer(:compensate_run, outputs: [Flezha::Codec.encode("a"), Flezha::Codec.encode("b")])

    assert_equal "work", result[:outcome]
    assert_equal "ok", result[:effects][0][:verdict]
  end

  def test_an_unknown_factory_is_refused_non_retryably
    result = Flezha::Dispatch.answer(activation(:work, factory: "nobody", items: []), @deps)

    assert_equal "failed", result[:outcome]
    assert result[:non_retryable]
  end

  def test_every_activation_kind_is_answered
    # The sweep: no kind may reach the unknown-kind arm.
    kinds = {
      work: { items: [] },
      restore_fold: { changes: [] },
      snapshot_request: {},
      live_state_read: {},
      migration_serialize: {},
      apply_state: { state_json: Flezha::Codec.to_state_json({ total: 0 }).b },
      key_request: { key_request: { channel: 0, payloads: [], want_keys: false,
                                    want_event_time: false } },
      compensate_run: { outputs: [] }
    }

    kinds.each do |kind, extra|
      result = answer(kind, **extra)
      refute_equal "failed", result[:outcome], "#{kind} fell through to a refusal"
    end
  end

  # -- a tasklet ending its bounded operator ----------------------------------------
  #
  # The host only carries the request in the item's effect. Ending the instance
  # once the iteration commits, and failing an unbounded operator's item, are the core's.

  COMPLETIONS = {
    "text" => ["drei ü", "drei \xC3\xBC".b],
    "bytes" => ["\x00\xFFraw".b, "\x00\xFFraw".b],
    "json" => [{ turns: 3 }, '{"turns":3}'.b],
    "none" => [nil, "".b]
  }.freeze

  # Completes twice per input, so the later call is the one that must ride.
  class Finisher < Flezha::Tasklet
    tasklet_name "finisher"

    def run(input, ctx)
      ctx.complete("replaced")
      ctx.complete(COMPLETIONS.fetch(input).first)
      nil
    end
  end

  class FinishingBounded < Flezha::BoundedOperator
    factory_id "finishing_bounded"
    channels { |c| c.input("in", codec: :text).into(Finisher).sink }
  end

  class FinishingUnbounded < Flezha::UnboundedOperator
    factory_id "finishing_unbounded"
    channels { |c| c.input("in", codec: :text).into(Finisher).sink }
  end

  def completion_of(operator, input)
    effect, = answer_on(operator, items: [input_item(input)])[:effects]
    assert_equal "ok", effect[:verdict]
    effect[:complete]
  end

  def test_complete_carries_the_latest_result_in_the_effect
    COMPLETIONS.each do |input, (_, carried)|
      completion = completion_of(FinishingBounded, input)

      assert_equal carried, completion, "#{input} rides as #{carried.inspect}"
      assert_equal Encoding::BINARY, completion.encoding
    end
  end

  def test_an_item_that_never_completes_carries_no_completion
    assert_nil answer(:work, items: [input_item("2")])[:effects][0][:complete]
  end

  def test_an_unbounded_tasklet_completing_is_carried_the_same_way
    assert_equal "drei \xC3\xBC".b, completion_of(FinishingUnbounded, "text")
  end

  class FailsAfterCompleting < Flezha::Tasklet
    tasklet_name "fails_after_completing"

    def run(input, ctx)
      ctx.complete(input)
      raise ArgumentError, "after completing"
    end
  end

  class FailingBounded < Flezha::BoundedOperator
    factory_id "failing_bounded"
    channels { |c| c.input("in", codec: :text).into(FailsAfterCompleting).sink }
  end

  def test_a_failed_item_carries_no_completion
    effect, = answer_on(FailingBounded, items: [input_item("x")])[:effects]

    assert_equal "retryable", effect[:verdict]
    refute effect.key?(:complete), "a failed item's effect carries no completion"
  end

  # -- timers inside a bounded operator ----------------------------------------------
  #
  # A bounded tasklet arms and cancels timers exactly as an unbounded operator one does;
  # only a compensate hook refuses them.

  class TimerSetter < Flezha::Tasklet
    tasklet_name "timer_setter"

    def run(call, ctx)
      case call
      when "after" then ctx.timer_after(60_000)
      when "event_time" then ctx.timer_at_event_time(60_000)
      else ctx.cancel_timer(:processing_time, 60_000)
      end
      nil
    end
  end

  class TimerBounded < Flezha::BoundedOperator
    factory_id "timer_bounded"
    channels { |c| c.input("in", codec: :text).into(TimerSetter) }
  end

  class TimerUnbounded < Flezha::UnboundedOperator
    factory_id "timer_unbounded"
    channels { |c| c.input("in", codec: :text).into(TimerSetter) }
  end

  def test_a_timer_call_inside_a_bounded_reaches_the_item_effect
    {
      "after" => { kind: "processing_time", scheduled_at: 60_100, register: true },
      "event_time" => { kind: "event_time", scheduled_at: 60_000, register: true },
      "cancel" => { kind: "processing_time", scheduled_at: 60_000, register: false }
    }.each do |call, operation|
      effect, = answer_on(TimerBounded, items: [input_item(call)])[:effects]

      assert_equal "ok", effect[:verdict], call
      assert_equal [operation], effect[:timers], call
    end
  end

  class ArmsOnUndo < Flezha::Tasklet
    tasklet_name "arms_on_undo"
    def run(_input, _ctx) = nil
    def compensate(_outputs, ctx) = ctx.timer_after(1_000)
  end

  class ArmsOnUndoBounded < Flezha::BoundedOperator
    factory_id "arms_on_undo_bounded"
    channels { |c| c.input("in", codec: :text).into(ArmsOnUndo) }
  end

  def test_a_timer_call_inside_a_compensate_hook_is_refused
    result = answer_on(ArmsOnUndoBounded, kind: :compensate_run, outputs: [])

    assert_equal "failed", result[:outcome]
    assert result[:error].end_with?("(runtime.timers_unavailable)"), result[:error]
  end

  def test_an_unbounded_tasklet_still_registers_its_timer
    effect, = answer_on(TimerUnbounded, items: [input_item("after")])[:effects]

    assert_equal "ok", effect[:verdict]
    assert_equal [true], effect[:timers].map { |timer| timer[:register] }
  end

  def test_a_result_json_cannot_carry_is_refused_naming_the_tasklet
    klass = Class.new(Flezha::Tasklet) do
      tasklet_name "completes_with_nan"
      def run(_input, ctx) = ctx.complete(Float::NAN)
    end

    error = assert_raises(Flezha::Error) { Flezha::Testing::Harness.new(klass).feed("x") }

    assert_equal ["runtime.value_not_json", "tasklet 'completes_with_nan'", "Float"],
                 [error.code, error.details["owner"], error.details["type"]]
  end

  # The handout contract, against the collaborator that decides it. A real core
  # created without `start` never handed these tokens out, so it is the core
  # itself that says so — a fake that accepts whatever it is handed cannot.
  #
  # An unanswered activation retires its cell for the life of the process, so
  # the guard is the contract rather than politeness.
  def test_an_activation_the_core_never_handed_out_is_abandoned_not_dropped
    skip "needs the compiled veneer" unless Flezha::TestSupport.native?

    core = Flezha::Core.create
    said = reported { Flezha::Dispatch.dispatch(activation(:work, items: []), core, @deps) }
    core.free

    assert_includes said, "(runtime.unknown_activation_token)"
    assert_match(/abandoning activation 1 failed/, said)
  end

  def test_a_dispatch_that_completes_is_not_abandoned
    skip "needs the compiled veneer" unless Flezha::TestSupport.native?

    # Only `complete` is scripted, and only to reach the path a real handout
    # would reach: the abandon is the core's own, so a guard that abandoned an
    # answered activation would earn its refusal and report it.
    core = Flezha::Core.create
    peer = Flezha::TestSupport::FakeCore.new
    peer.define_singleton_method(:abandon) { |token| core.abandon(token) }
    said = reported { Flezha::Dispatch.dispatch(activation(:work, items: []), peer, @deps) }
    core.free

    assert_equal 1, peer.completed.size
    assert_equal "", said
  end

  # What the dispatcher said while it ran. Its guard never raises, so the log
  # line is the only evidence there is.
  class RecordingLog
    def initialize = @lines = []
    def to_s = @lines.join("\n")

    %i[debug info warn error].each { |level| define_method(level) { |message| @lines << message } }
  end

  def reported
    previous = Flezha.log
    Flezha.log = RecordingLog.new
    yield
    Flezha.log.to_s
  ensure
    Flezha.log = previous
  end
  # --- output identity stamps -----------------------------------------------
  #
  # One invocation, several emits: the per-emit ordinal is the half a
  # single-emit assertion cannot see, and the e2e golden checks stamp NAMES only.

  class StampEmitter < Flezha::Tasklet
    tasklet_name "stamp_emitter"

    def run(input, ctx)
      if input == "twice"
        ctx.emit_to("keyed_out", "a")
        ctx.emit_to("keyed_out", "b")
      elsif input == "spoof"
        ctx.emit_to("keyed_out", "v", metadata: { "flz.seq" => "999.0" })
      else
        ctx.emit_to("keyed_out", "k", metadata: { "user" => "meta" })
      end
      nil
    end
  end

  # One keyed plugin channel: the topology every SDK can express, and where the
  # per-emit ordinal has to be right.
  class StampOperator < Flezha::UnboundedOperator
    factory_id "stamp_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .key_by { |_v| "ek" }
       .into(StampEmitter)
       .keyed
       .sink("keyed_out", encode: ->(v) { v.to_s.b })
    end
  end

  class PlainEmitter < Flezha::Tasklet
    tasklet_name "plain_emitter"

    def run(_input, ctx)
      ctx.emit_to("plain_out", "p")
      nil
    end
  end

  # A tasklet owns ONE output channel, so a keyed and a non-keyed plugin
  # channel are two tasklets. The unkeyed one carries no stamps at all.
  class MixedStampOperator < Flezha::UnboundedOperator
    factory_id "mixed_stamp_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .key_by { |_v| "ek" }
       .branch(
         ->(edge) { edge.into(PlainEmitter).sink("plain_out", encode: ->(v) { v.to_s.b }) },
         lambda { |edge|
           edge.into(StampEmitter).keyed
               .sink("keyed_out", encode: ->(v) { v.to_s.b })
         }
       )
    end
  end

  def stamp_answer(payload, operator: StampOperator, items: nil)
    answer_on(operator, cell_key: "ek", change_seq: 5, items: items || [input_item(payload)])
  end

  def meta_of(output) = output[:metadata].to_h

  def test_a_keyed_plugin_emit_carries_the_stamps_and_the_ordinal_advances_per_emit
    outputs = stamp_answer("twice")[:effects][0][:outputs]

    assert_equal 2, outputs.size
    assert_equal({ "flz.cell" => "op-1/stamp_emitter/ek", "flz.seq" => "5.0",
                   "flz.key" => "ek" }, meta_of(outputs[0]))
    assert_equal({ "flz.cell" => "op-1/stamp_emitter/ek", "flz.seq" => "5.1",
                   "flz.key" => "ek" }, meta_of(outputs[1]))
  end

  def test_a_non_keyed_plugin_emit_carries_no_stamps
    plain, = stamp_answer("mixed", operator: MixedStampOperator)[:effects][0][:outputs]

    assert_equal "plain_out", plain[:plugin_channel]
    assert_empty plain[:metadata]
  end

  class Answer < Flezha::Tasklet
    tasklet_name "answer"
    def run(input, ctx) = ctx.emit(input)
  end

  class AnsweringOperator < Flezha::UnboundedOperator
    factory_id "answering_op"
    channels { |c| c.input("in", codec: :text).into(Answer).response }
  end

  def test_an_emit_on_a_response_channel_leaves_as_a_sinks_plugin_output
    output, = answer_on(AnsweringOperator, items: [input_item("once")])[:effects][0][:outputs]

    assert_equal %w[plugin answer once], [output[:target], output[:plugin_channel], output[:payload]]
  end

  def test_user_metadata_rides_a_keyed_plugin_frame_with_the_stamps_after_it
    keyed, = stamp_answer("mixed")[:effects][0][:outputs]

    assert_equal "keyed_out", keyed[:plugin_channel]
    assert_equal({ "user" => "meta", "flz.cell" => "op-1/stamp_emitter/ek",
                   "flz.seq" => "5.0", "flz.key" => "ek" }, meta_of(keyed))
  end

  def test_a_fresh_work_item_restarts_the_emit_ordinal
    result = stamp_answer(nil, items: [input_item("twice"), input_item("twice")])

    assert_equal [%w[5.0 5.1], %w[5.0 5.1]],
                 result[:effects].map { |e| e[:outputs].map { |o| meta_of(o)["flz.seq"] } }
  end

  def test_a_spoofed_stamp_is_refused_before_any_output
    result = stamp_answer("spoof")

    assert_equal "retryable", result[:effects][0][:verdict]
    refute result[:effects][0].key?(:outputs), "a refused emit must leave no outputs behind"
  end
  # -- the structured-bounded emit arm ---------------------------------------
  #
  # One released value is ONE edge output on a bounded class, untransformed and
  # unkeyed, naming the producing channel and its first consumer: the driver
  # fans it out from the journaled handoff. On an unbounded class the same topology
  # releases one output per consumer, transformed and keyed. The plugin output
  # rides alongside either way.

  class BoundedProducer < Flezha::Tasklet
    tasklet_name "bounded_producer"
    def run(input, ctx) = ctx.emit(input)
  end

  # What a consumer of the `handoff` channel was handed, in delivery order.
  DELIVERED = []

  class BoundedB < Flezha::Tasklet
    tasklet_name "bounded_b"

    def run(input, _ctx)
      DELIVERED << input
      nil
    end
  end

  class BoundedC < Flezha::Tasklet
    tasklet_name "bounded_c"

    def run(input, _ctx)
      DELIVERED << input
      nil
    end
  end

  BOUNDED_GRAPH = lambda do |c|
    edge = c.input("in").into(BoundedProducer).named("handoff")
    edge.map { |value| value + 1 }.into(BoundedB)
    edge.into(BoundedC)
    edge.sink
  end

  class BoundedEmitOperator < Flezha::BoundedOperator
    factory_id "bounded_emit"
    channels(&BOUNDED_GRAPH)
  end

  class BoundedEmitUnboundedOperator < Flezha::UnboundedOperator
    factory_id "bounded_emit_stream"
    channels(&BOUNDED_GRAPH)
  end

  # An edge payload is the class codec's MessagePack; the `handoff` sink names
  # no codec, so its frame for a number is JSON.
  def payload_of(output)
    output[:target] == "plugin" ? JSON.parse(output[:payload]) : Flezha::Codec.decode(output[:payload])
  end

  def emit_arm_outputs(operator)
    answer_on(operator, cell_key: "ek", items: [input_item("10")])[:effects][0][:outputs]
  end

  def test_a_structured_bounded_releases_one_untransformed_edge_output
    outputs = emit_arm_outputs(BoundedEmitOperator)

    assert_equal [["edge", 1, nil, 10], ["plugin", nil, "handoff", 10]],
                 outputs.map { |o|
                   [o[:target], o[:consumer], o[:plugin_channel], payload_of(o)]
                 }
    assert_nil outputs[0][:key]
    assert_nil outputs[0][:event_time]
  end

  def test_an_edge_payload_is_the_encoders_own_string
    produced = []
    encode = Flezha::Codec.method(:encode)
    outputs = Flezha::Codec.stub(:encode, ->(value) { encode.call(value).tap { produced << _1 } }) do
      emit_arm_outputs(BoundedEmitUnboundedOperator)
    end

    outputs.select { |output| output[:target] == "edge" }.each do |output|
      assert_equal Encoding::ASCII_8BIT, output[:payload].encoding
      assert produced.any? { _1.equal?(output[:payload]) },
             "the frame carries a copy of what MessagePack returned, not the string itself"
    end
  end

  def test_the_same_graph_on_an_unbounded_class_releases_one_output_per_consumer
    outputs = emit_arm_outputs(BoundedEmitUnboundedOperator)

    assert_equal [["edge", 1, 11], ["edge", 2, 10], ["plugin", nil, 10]],
                 outputs.map { |o| [o[:target], o[:consumer], payload_of(o)] }
  end

  # The other end of the same two arms: what the CONSUMER is handed. A stream
  # producer already applied the edge, so delivery applies nothing; a bounded operator
  # stage released the value raw, so delivery is the one application.

  def delivered(operator, tasklet, payload)
    DELIVERED.clear
    result = answer_on(operator, tasklet: tasklet, cell_key: "ek", items: [input_item(payload, channel: 1)])
    assert_equal "ok", result[:effects][0][:verdict]
    DELIVERED.dup
  end

  def test_an_unbounded_inter_task_edge_applies_its_map_exactly_once
    mapped = emit_arm_outputs(BoundedEmitUnboundedOperator)[0][:payload]

    assert_equal [11], delivered(BoundedEmitUnboundedOperator, 1, mapped)
  end

  def test_a_bounded_consumer_applies_its_own_edge_exactly_once_at_delivery
    released = emit_arm_outputs(BoundedEmitOperator)[0][:payload]

    # One released value reaches BOTH children — the driver's fan-out — and
    # each evaluates its own edge here.
    assert_equal [11], delivered(BoundedEmitOperator, 1, released)
    assert_equal [10], delivered(BoundedEmitOperator, 2, released)
  end

  # An entry naming no codec reads JSON, so plain text fails its decode: the
  # verdict says the message is unreadable (the core parks it under the entry
  # channel, and a bounded operator keeps waiting), and the handler is never handed it.
  class JsonEntryOperator < Flezha::UnboundedOperator
    factory_id "json_entry_op"
    channels { |c| c.input("in").into(BoundedB) }
  end

  def test_plain_text_into_an_entry_naming_no_codec_never_reaches_the_handler
    DELIVERED.clear
    effects = answer_on(JsonEntryOperator, items: [input_item('{"a":1}'), input_item("hello")])[:effects]

    assert_equal %w[ok unreadable], effects.map { |e| e[:verdict] }
    assert_includes effects[1][:message], "JSON::ParserError"
    assert_equal [{ a: 1 }], DELIVERED
  end

  # Only the entrance answers `unreadable`: a readable message the handler
  # raises on is the handler's failure, retryable as before.
  class Refusing < Flezha::Tasklet
    tasklet_name "refusing"
    def run(_input, _ctx) = raise(ArgumentError, "cannot place this")
  end

  class RefusingEntryOperator < Flezha::UnboundedOperator
    factory_id "refusing_entry_op"
    channels { |c| c.input("in").into(Refusing) }
  end

  def test_an_unreadable_message_is_told_apart_from_a_handler_failure
    unreadable = answer_on(RefusingEntryOperator, items: [input_item("not json")])[:effects][0]
    failed = answer_on(RefusingEntryOperator, items: [input_item('{"a":1}')])[:effects][0]

    assert_equal "unreadable", unreadable[:verdict]
    assert_equal "retryable", failed[:verdict]
    assert_includes failed[:message], "ArgumentError: cannot place this"
  end

  # The entry edge's transforms run at the entrance too, so a mapping that
  # rejects a message parks it as an unreadable one rather than failing the
  # handler that never saw it.
  class StrictEntryOperator < Flezha::UnboundedOperator
    factory_id "strict_entry_op"
    channels { |c| c.input("in").map { |value| value.fetch(:k) }.into(BoundedB) }
  end

  def test_an_entry_edge_transform_that_raises_makes_the_message_unreadable
    DELIVERED.clear
    effects = answer_on(StrictEntryOperator, items: [input_item('{"k":"fine"}'), input_item('{"other":1}')])[:effects]

    assert_equal %w[ok unreadable], effects.map { |e| e[:verdict] }
    assert_includes effects[1][:message], "KeyError"
    assert_equal ["fine"], DELIVERED
  end

  # -- merge ----------------------------------------------------------------
  #
  # Two sources into one node, an entry and a tasklet's output: each source
  # runs its own mapping on its own edge, the entry's at delivery and the
  # producer's as it emits, and the key is read off the mapped value.

  class PassThrough < Flezha::Tasklet
    tasklet_name "pass_through"
    def run(input, ctx) = ctx.emit(input)
  end

  class MergeLabel < Flezha::Tasklet
    tasklet_name "merge_label"
    def run(input, ctx) = ctx.emit("#{input[:source]}:#{input[:payload]}")
  end

  MERGE_TAG = lambda do |source|
    ->(payload) { { source: source, symbol: payload.split(":").first, payload: payload } }
  end

  class MergeOperator < Flezha::UnboundedOperator
    factory_id "merge_op"
    channels do |c|
      ticks = c.input("ticks", codec: :text)
      quotes = c.input("quotes", codec: :text).into(PassThrough, "merge_upstream")
      c.merge.from(ticks, &MERGE_TAG.call("ticks")).from(quotes, &MERGE_TAG.call("quotes"))
       .key_by { |value| value[:symbol] }.into(MergeLabel).sink("merged")
    end
  end

  def test_two_merge_sources_reach_one_tasklet_each_under_its_own_mapping_in_order
    upstream = answer_on(MergeOperator, tasklet: 0, cell_key: "AAPL", items: [input_item("AAPL:3", channel: 1)])
    released = upstream[:effects][0][:outputs]
    assert_equal [[1, "AAPL", { source: "quotes", symbol: "AAPL", payload: "AAPL:3" }]],
                 released.map { |o| [o[:consumer], o[:key], Flezha::Codec.decode(o[:payload])] }

    items = [
      input_item("AAPL:1", channel: 0, consumed_offset: 1),
      input_item("AAPL:2", channel: 0, consumed_offset: 2),
      input_item(released.first[:payload], channel: 2, consumed_offset: 1)
    ]
    effects = answer_on(MergeOperator, tasklet: 1, cell_key: "AAPL", items: items)[:effects]

    assert_equal [["ticks:AAPL:1"], ["ticks:AAPL:2"], ["quotes:AAPL:3"]],
                 effects.map { |effect| effect[:outputs].map { |output| output[:payload] } }
  end

  def test_a_merge_key_is_read_off_the_mapped_value_at_the_crossing
    request = { channel: 0, payloads: ["MSFT:1".b], want_keys: true, want_event_time: false }
    result = answer_on(MergeOperator, kind: :key_request, tasklet: 1, cell_key: "AAPL", key_request: request)

    assert_equal [["MSFT"]], result[:prepared].map { |answer| answer[:keys] }
  end

  # -- flat_map ---------------------------------------------------------------
  #
  # A fan is the list shape of the one transform fold, so it reaches the three
  # places a chain runs: the handler, the key crossing and the emit.

  class FanOperator < Flezha::UnboundedOperator
    factory_id "fan_op"
    channels do |c|
      c.input("lines", codec: :text).flat_map { |line| line.split(",") }.into(PassThrough).sink("fanned")
    end
  end

  class KeyedFanOperator < Flezha::UnboundedOperator
    factory_id "keyed_fan_op"
    channels do |c|
      c.input("lines", codec: :text).flat_map { |line| line.split(",") }
       .key_by { |value| value.split(":").first }.into(PassThrough).sink("fanned")
    end
  end

  class EmitFanOperator < Flezha::UnboundedOperator
    factory_id "emit_fan_op"
    channels do |c|
      c.input("lines", codec: :text).into(PassThrough, "fan_source")
       .flat_map { |line| line.split(",") }.key_by { |value| value.split(":").first }.into(PassThrough)
    end
  end

  def test_a_fan_of_three_runs_the_handler_three_times_under_one_effect
    effects = answer_on(FanOperator, items: [input_item("a,b,c", consumed_offset: 7)])[:effects]

    assert_equal 1, effects.size
    assert_equal ["ok", 7, %w[a b c]],
                 [effects[0][:verdict], effects[0][:consumed_offset], effects[0][:outputs].map { |o| o[:payload] }]
  end

  def test_a_fan_of_none_is_a_drop_that_still_consumes_its_offset
    effects = answer_on(FanOperator, items: [input_item("", consumed_offset: 3)])[:effects]

    assert_equal [["ok", 3, []]], effects.map { |e| [e[:verdict], e[:consumed_offset], e[:outputs]] }
  end

  def test_a_fan_followed_by_key_by_answers_every_key_at_the_crossing
    request = { channel: 0, payloads: ["a:1,b:2,c:3".b, "".b], want_keys: true, want_event_time: false }
    prepared = answer_on(KeyedFanOperator, kind: :key_request, key_request: request)[:prepared]

    assert_equal [%w[a b c], []], prepared.map { |answer| answer[:keys] }
  end

  # The crossing answered every key, so the payload reaches each Cell whole:
  # each keeps only the values keyed to it.
  def test_each_cell_a_fanned_payload_reaches_runs_only_its_own_values
    outputs = %w[a b].map do |key|
      answer_on(KeyedFanOperator, cell_key: key, items: [input_item("a:1,b:2,a:3")])[:effects][0][:outputs]
    end

    assert_equal [%w[a:1 a:3], %w[b:2]], outputs.map { |frames| frames.map { |o| o[:payload] } }
  end

  class StaticFanLabel < PassThrough
    tasklet_name "static_fan_label"
    cell_keys "a", "b"
    ROUTES = { "both" => %w[a b], "none" => [] }.freeze
    def cells_for(value) = ROUTES.fetch(value.split(":").first) { [value.split(":").first] }
  end

  class StaticFanOperator < Flezha::UnboundedOperator
    factory_id "static_fan_op"
    channels do |c|
      c.input("lines", codec: :text).flat_map { |line| line.split(",") }.into(StaticFanLabel).sink("fanned")
    end
  end

  # The same on a static consumer, where `cells_for` names the Cells: a
  # value naming both reaches both, and one naming none reaches neither.
  def test_each_static_cell_a_fanned_payload_reaches_runs_only_its_own_values
    outputs = %w[a b].map do |key|
      answer_on(StaticFanOperator, cell_key: key,
                                   items: [input_item("a:1,b:2,both:9,none:0,a:3")])[:effects][0][:outputs]
    end

    assert_equal [%w[a:1 both:9 a:3], %w[b:2 both:9]], outputs.map { |frames| frames.map { |o| o[:payload] } }
  end

  def test_an_emit_through_a_fan_releases_one_keyed_frame_per_value
    outputs = answer_on(EmitFanOperator, items: [input_item("a:1,b:2")])[:effects][0][:outputs]

    assert_equal [%w[a a:1], %w[b b:2]], outputs.map { |o| [o[:key], Flezha::Codec.decode(o[:payload])] }
  end

  # -- the latest-document router, through the dispatcher ---------------------

  class RouterOperator < Flezha::UnboundedOperator
    factory_id "router_op"
    channels do |c|
      c.route_latest(c.input("config"), c.input("trades")) { |trade| trade[:symbol] }
       .sink("routed", codec: :msgpack)
    end
  end

  def test_a_declared_router_hands_each_key_the_document_once_on_its_own_traffic
    document = '{"limit":5}'
    items = [[0, document], [1, '{"symbol":"A"}'], [1, '{"symbol":"B"}'], [0, document],
             [1, '{"symbol":"A"}']].map { |channel, payload| input_item(payload, channel: channel) }
    effects = answer_on(RouterOperator, items: items)[:effects]

    assert_equal [[], [["A", { limit: 5 }]], [["B", { limit: 5 }]], [], []],
                 effects.map { |e| e[:outputs].map { |o| Flezha::Codec.decode(o[:payload]) } }
  end
end
