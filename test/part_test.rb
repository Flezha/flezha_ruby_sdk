# frozen_string_literal: true

require_relative "test_helper"

Flezha::TestSupport.require_test_app("fixtures_parts")

# A tasklet's part: the runtime hands the core the activation's token, the
# index of the item the handler runs and the value encoded as the sink would
# encode it, and a refusal raises inside the handler like any raise. The core
# numbers, stamps and drops parts; its own tests and the fake controller cover
# that side.
class PartTest < Minitest::Test
  class Previewer < Flezha::Tasklet
    tasklet_name "previewer"

    def run(input, ctx)
      ctx.part({ "draft" => input })
      ctx.part("#{input}!")
      ctx.emit(input)
    end
  end

  class PreviewOperator < Flezha::UnboundedOperator
    factory_id "preview_op"
    channels do |c|
      c.input("in", codec: :text).into(Previewer).sink("answer", encode: ->(v) { "<#{v.to_json}>".b })
    end
  end

  def answer(core, items)
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    deps = Flezha::TestSupport.wired_deps(PreviewOperator, core)
    activation = Flezha::TestSupport.activation(factory: "preview_op", token: 9, items: items)
    Flezha::Dispatch.answer(activation, deps)
  end

  def input_item(payload) = Flezha::TestSupport.input_item(payload)

  def test_each_part_names_its_token_and_item_and_is_encoded_as_the_sink_encodes
    core = Flezha::TestSupport::FakeCore.new
    result = answer(core, [input_item("a"), input_item("b")])

    assert_equal %w[ok ok], result[:effects].map { |e| e[:verdict] }
    assert_equal [
      [9, 0, '<{"draft":"a"}>'.b], [9, 0, '<"a!">'.b],
      [9, 1, '<{"draft":"b"}>'.b], [9, 1, '<"b!">'.b]
    ], core.parts
  end

  def test_a_refused_part_fails_the_item_like_any_raise
    refusal = Flezha::Error.new("part.no_sink", tasklet: "previewer", channel: "answer")
    result = answer(Flezha::TestSupport::FakeCore.new(part_refusal: refusal), [input_item("a")])

    assert_equal "retryable", result[:effects][0][:verdict]
    assert_includes result[:effects][0][:message], "part.no_sink"
  end

  def test_the_testing_harness_sends_a_part_nowhere
    outcome = Flezha::Testing::Harness.new(Previewer).feed("a")

    assert_equal ["a"], outcome.outputs
  end

  # Through the real core over a local run: every part is dropped, since a
  # local run has no controller and so no listener, and the output is released.
  def test_a_local_run_drops_the_parts_and_releases_the_output
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    assert_equal %w[done:3 done:1], released(Fixtures::PartEcho, %w[3 1])
  end

  class Feeder < Flezha::Tasklet
    tasklet_name "part_feeder"
    REFUSALS = Queue.new

    def run(input, ctx)
      begin
        ctx.part(input)
      rescue Flezha::Error => e
        REFUSALS << e.code
      end
      ctx.emit(input)
    end
  end

  class Relay < Flezha::Tasklet
    tasklet_name "part_relay"
    def run(input, ctx) = ctx.emit(input)
  end

  class FeederOperator < Flezha::UnboundedOperator
    factory_id "part_feeder_op"
    channels { |c| c.input("in", codec: :text).into(Feeder).into(Relay).sink("answer") }
  end

  class Answerer < Feeder
    tasklet_name "part_answerer"
  end

  class AnswererOperator < Flezha::UnboundedOperator
    factory_id "part_answerer_op"
    channels { |c| c.input("in", codec: :text).into(Answerer).response }
  end

  def test_a_channel_without_a_sink_refuses_the_part
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    assert_equal %w[x], released(FeederOperator, %w[x])
    assert_equal %w[y], released(AnswererOperator, %w[y], sink: "part_answerer")
    refusals = Array.new(Feeder::REFUSALS.size) { Feeder::REFUSALS.pop }

    assert_equal %w[part.no_sink part.no_sink], refusals,
                 "a channel feeding another tasklet and a response end preview nothing"
  end

  private

  def released(operator, inputs, sink: "answer")
    outputs = []
    run = Flezha::LocalOperator.start(operator)
    begin
      run.on_sink(sink) { |output| outputs << output.payload.dup.force_encoding("UTF-8") }
      inputs.each { |input| run.feed("in", input) }
      run.run_until_idle(20_000)
    ensure
      run.close
    end
    outputs
  end
end
