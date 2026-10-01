# frozen_string_literal: true

require_relative "test_helper"

# An operator inside an operator, run locally: a flow class whose keyed edge
# ends in a steps class runs one stepped instance of it per key, each with its
# own state, and the instance's exit output continues on the outer graph; a
# flow class is inlined under prefixed names.
class InnerOperatorTest < Minitest::Test
  # One turn of a session: `key:text` answered as `key#turns:text`.
  class Turn < Flezha::Tasklet
    tasklet_name "turn"
    def init_state(_key) = { turns: 0 }
    def apply_change(state, change) = state[:turns] += change[:by]

    def run(line, ctx)
      key, text = line.split(":", 2)
      ctx.change({ by: 1 })
      ctx.emit("#{key}##{ctx.state[:turns]}:#{text}")
    end
  end

  class Reply < Flezha::Tasklet
    tasklet_name "reply"
    def run(line, ctx) = ctx.emit(line)
  end

  class Trim < Flezha::Tasklet
    tasklet_name "trim"
    def run(line, ctx) = ctx.emit(line.strip)
  end

  class Upper < Flezha::Tasklet
    tasklet_name "upper"
    def run(line, ctx) = ctx.emit(line.upcase)
  end

  class Session < Flezha::Operator
    factory_id "inner_session_ruby"
    run steps: { each: :per_input }
    durable false
    channels { |c| c.input("turns", codec: :text).into(Turn).named("answer") }
  end

  class Gateway < Flezha::Operator
    factory_id "inner_gateway_ruby"
    channels do |c|
      c.input("requests", codec: :text)
       .key_by { |line| line.split(":").first }
       .into(Session, max_cells: 2)
       .into(Reply)
       .sink("out", codec: :text)
    end
  end

  class Cleanup < Flezha::Operator
    factory_id "inner_cleanup_ruby"
    channels { |c| c.input("raw", codec: :text).into(Trim).into(Upper) }
  end

  class Inlining < Flezha::Operator
    factory_id "inner_inlining_ruby"
    channels { |c| c.input("requests", codec: :text).into(Cleanup).into(Reply).sink("out", codec: :text) }
  end

  class TwoEntries < Flezha::Operator
    factory_id "inner_two_entries_ruby"
    channels do |c|
      c.input("a").into(Trim)
      c.input("b").into(Upper)
    end
  end

  class AmbiguousEntry < Flezha::Operator
    factory_id "inner_ambiguous_entry_ruby"
    channels { |c| c.input("in").into(TwoEntries).into(Reply) }
  end

  class StepsOuter < Flezha::Operator
    factory_id "inner_steps_outer_ruby"
    run steps: { each: :per_input }
    durable false
    channels { |c| c.input("in", codec: :text).into(Session).into(Reply) }
  end

  def setup
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?
  end

  def released(operator, lines, count)
    outputs = []
    run = Flezha::LocalOperator.start(operator)
    begin
      run.on_sink("out") { |output| outputs << output.payload.dup.force_encoding("UTF-8") }
      lines.each { |line| run.feed("requests", line) }
      run.run_until_idle(5_000)
    ensure
      run.close
    end
    assert_equal count, outputs.size, outputs.inspect
    outputs.sort
  end

  def test_every_key_runs_its_own_scope_and_the_exit_continues_the_outer_graph
    assert_equal %w[alice#1:hi alice#2:again bob#1:yo],
                 released(Gateway, %w[alice:hi bob:yo alice:again], 3)
  end

  def test_a_third_key_past_max_cells_is_dropped
    assert_equal %w[alice#1:hi bob#1:yo], released(Gateway, %w[alice:hi bob:yo carol:hey], 2)
  end

  def test_a_flow_class_is_inlined_under_prefixed_names
    assert_equal ["HI"], released(Inlining, ["  hi  "], 1)
    assert_equal %w[inner_cleanup_ruby.trim inner_cleanup_ruby.upper reply],
                 Inlining.spec.tasklets.map(&:name).sort
    assert(Inlining.spec.tasklets.all? { |node| node.embeds.nil? })
  end

  def test_an_inlined_class_with_two_entries_and_no_input_is_refused_naming_both
    error = assert_raises(Flezha::Error) { AmbiguousEntry.spec }
    assert_equal "topology.embedded_entry_ambiguous", error.code
    assert_equal({ "inner" => "inner_two_entries_ruby", "channels" => "a, b" },
                 error.details.transform_keys(&:to_s))
  end

  def test_a_steps_class_embedding_a_steps_class_is_refused_naming_both
    error = assert_raises(Flezha::Error) { Flezha::LocalOperator.start(StepsOuter) }
    assert_equal "topology.nested_steps", error.code, error.message
    assert_includes error.message, "inner_steps_outer_ruby"
    assert_includes error.message, "inner_session_ruby"
  end
end

class InnerOperatorEdgeTest < Minitest::Test
  class AfterATasklet < Flezha::Operator
    factory_id "inner_after_a_tasklet_ruby"
    channels do |c|
      c.input("requests", codec: :text)
       .into(InnerOperatorTest::Trim)
       .key_by { |line| line.split(":").first }
       .into(InnerOperatorTest::Session)
       .into(InnerOperatorTest::Reply)
       .sink("out", codec: :text)
    end
  end

  class SinkAfter < Flezha::Operator
    factory_id "inner_sink_after_ruby"
    channels { |c| c.input("requests", codec: :text).into(InnerOperatorTest::Session).sink("out", codec: :text) }
  end

  def setup
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?
  end

  def test_a_value_a_tasklet_emits_enters_the_scope_in_its_entry_codec
    outputs = []
    run = Flezha::LocalOperator.start(AfterATasklet)
    begin
      run.on_sink("out") { |output| outputs << output.payload.dup.force_encoding("UTF-8") }
      run.feed("requests", "  alice:hi  ")
      run.run_until_idle(5_000)
    ensure
      run.close
    end
    assert_equal ["alice#1:hi"], outputs
  end

  def test_a_sink_right_after_an_embedded_class_is_refused
    error = assert_raises(Flezha::Error) { Flezha::LocalOperator.start(SinkAfter) }
    assert_equal "topology.invalid_setting", error.code
    assert_equal "out", error.details.transform_keys(&:to_s)["value"]
  end
end
