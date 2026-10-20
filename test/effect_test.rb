# frozen_string_literal: true

require_relative "test_helper"

# `ctx.effect` (`recorded-effects`): a live input keeps an outside call's
# result, a replay is served it without running the block, a replay that
# reaches outside some other way fails naming the effect, and the refusals.
class EffectTest < Minitest::Test
  # How often each outside call ran, by name.
  CALLS = Hash.new(0)

  def self.model(name)
    CALLS[name] += 1
    { "answer" => name, "call" => CALLS[name] }
  end

  class Asks < Flezha::Tasklet
    tasklet_name "effect_asks"
    def run(question, ctx) = ctx.emit(ctx.effect("model") { EffectTest.model(question) })
  end

  # A codec: the result is kept as upper-case text and read back lower.
  module Upper
    def self.encode(value) = value.upcase
    def self.decode(body) = body.downcase
  end

  class WithCodec < Flezha::Tasklet
    tasklet_name "effect_codec"
    def run(word, ctx) = ctx.emit(ctx.effect("tool", codec: Upper) { word })
  end

  class OneMore < Flezha::Tasklet
    tasklet_name "effect_one_more"
    def run(_input, ctx)
      @calls = (@calls || 0) + 1
      @calls.times { ctx.effect("model") { "same" } }
    end
  end

  class Around < Flezha::Tasklet
    tasklet_name "effect_around"
    def run(_input, ctx)
      if @replaying
        ctx.emit(EffectTest.model("around"))
      else
        @replaying = true
        ctx.emit(ctx.effect("model") { EffectTest.model("around") })
      end
    end
  end

  class Nested < Flezha::Tasklet
    tasklet_name "effect_nested"
    def run(_input, ctx) = ctx.effect("outer") { ctx.effect("inner") { 1 } }
  end

  class Fails < Flezha::Tasklet
    tasklet_name "effect_fails"
    def run(_input, ctx)
      @attempts = (@attempts || 0) + 1
      ctx.effect("model") { raise IOError, "down" } if @attempts == 1
      ctx.emit(ctx.effect("model") { "second" })
    end
  end

  class EffectLocal < Flezha::Relay
    factory_id "effect_local_ruby"
    channels { |c| c.input("in", codec: :text).into(Asks).output("out", codec: :json) }
  end

  def harness(tasklet) = Flezha::Testing::TaskletTestHarness.new(tasklet)

  def test_a_live_input_runs_the_block_and_a_replay_is_served_its_result
    harness = harness(Asks)
    live = harness.feed("live-q")
    replayed = harness.feed("live-q", replay: true)

    assert_equal 1, CALLS["live-q"], "the replay did not run the block"
    assert_equal [{ answer: "live-q", call: 1 }], live.outputs
    assert_equal live.outputs, replayed.outputs
  end

  def test_a_codec_encodes_and_decodes_the_result
    assert_equal ["hammer"], harness(WithCodec).feed("Hammer").outputs
  end

  def test_a_replay_that_runs_a_block_fails_naming_the_effect
    harness = harness(OneMore)
    harness.feed("q")
    error = assert_raises(Flezha::Testing::EffectReplayMismatch) { harness.feed("q", replay: true) }
    assert_match(/effect 'model' ran its block/, error.message)
  end

  def test_a_call_made_around_the_effect_on_replay_fails_naming_it
    harness = harness(Around)
    harness.feed("q")
    error = assert_raises(Flezha::Testing::EffectReplayMismatch) { harness.feed("q", replay: true) }
    assert_match(/effect 'model' was never asked for/, error.message)
  end

  def test_a_bad_name_is_refused_without_a_retry
    ["", "m" * 256].each do |name|
      tasklet = Class.new(Flezha::Tasklet) do
        tasklet_name "effect_bad_name"
        define_method(:run) { |_input, ctx| ctx.effect(name) { flunk "a refused call runs nothing" } }
      end
      error = assert_raises(Flezha::Error) { harness(tasklet).feed("q") }
      assert_equal "effect.bad_name", error.code
      assert_predicate error, :non_retryable?
    end
  end

  def test_an_effect_inside_an_effect_is_refused
    error = assert_raises(Flezha::Error) { harness(Nested).feed("q") }
    assert_equal "effect.nested", error.code
  end

  def test_a_failing_block_keeps_nothing_and_its_error_propagates
    harness = harness(Fails)
    assert_raises(IOError) { harness.feed("q") }
    assert_equal ["second"], harness.feed("q").outputs, "the failed call kept nothing"
  end

  def test_a_result_the_controller_kept_first_is_what_the_call_answers
    harness = harness(Asks)
    harness.define_singleton_method(:effect_end) { |*| '{"answer":"first"}' }
    assert_equal [{ answer: "first" }], harness.feed("first-q").outputs
  end

  def test_local_mode_runs_the_block_each_time_through_the_core
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    released = []
    log, default_log = Flezha::TestSupport::CapturedLog.install
    run = Flezha::LocalRelay.start(EffectLocal)
    begin
      run.on_output("out") { |output| released << JSON.parse(output.payload) }
      run.feed("in", "local-q")
      run.feed("in", "local-q")
      run.run_until_idle(20_000)
    ensure
      run.close
      Flezha.log = default_log
    end
    # The core's local-mode banner is the one line the run logs.
    assert_equal [[:error, "LOCAL MODE: "]], log.lines.map { |level, message| [level, message[0, 12]] }

    assert_equal 2, CALLS["local-q"], "nothing re-runs a local input, so nothing is kept"
    assert_equal [{ "answer" => "local-q", "call" => 1 }, { "answer" => "local-q", "call" => 2 }], released
  end
end
