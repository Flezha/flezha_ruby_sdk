# frozen_string_literal: true

require_relative "test_helper"

# The Ruby SDK's local mode: a user's own relay class, its declared
# two-tasklet graph, and no controller anywhere.
#
# The graph and its expected output are read from
# `test/fixtures/local_mode_word_count.txt`, which the Rust, Java, Node,
# Python and Elixir local-mode suites read too — so every core is compared
# against one recorded expectation instead of by inspection.
class LocalModeTest < Minitest::Test
  # One line into its words: the hop to `Tally` is the runtime's job.
  class Split < Flezha::Tasklet
    tasklet_name "split"
    def run(line, ctx)
      line.split.each { |word| ctx.emit(word.downcase) }
      nil
    end
  end

  # One Cell per word, minted on first sight of it.
  class Tally < Flezha::Tasklet
    tasklet_name "tally"
    idle_ttl_seconds 300
    def init_state(_key) = { seen: 0 }
    def apply_change(state, change) = state[:seen] = change[:seen]

    def run(word, ctx)
      seen = ctx.state[:seen] + 1
      ctx.change({ seen: seen })
      ctx.emit("#{word} #{seen}")
    end
  end

  class WordCount < Flezha::Relay
    factory_id "local_word_count_ruby"
    channels do |c|
      c.input("lines", codec: :text)
       .into(Split)
       .key_by { |word| word }
       .into(Tally).keyed
       .output("totals")
    end
  end

  def setup
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?
  end

  # The scenario every core runs: the lines to feed, and the output values.
  def scenario
    path = Flezha::TestSupport.contract_file("test/fixtures/local_mode_word_count.txt")
    lines = []
    totals = Hash.new(0)
    File.read(path).each_line(chomp: true) do |row|
      lines << row.delete_prefix("lines: ") if row.start_with?("lines: ")
      totals[row.delete_prefix("totals: ")] += 1 if row.start_with?("totals: ")
    end
    [lines, totals]
  end

  def test_a_declared_graph_runs_locally_and_releases_the_recorded_output
    lines, expected = scenario
    released = []
    run = Flezha::LocalRelay.start(WordCount)
    begin
      run.on_output("totals") { |output| released << output }
      lines.each { |line| run.feed("lines", line) }
      run.run_until_idle(20_000)
    ensure
      run.close
    end

    counted = released.each_with_object(Hash.new(0)) { |o, acc| acc[o.payload.dup.force_encoding("UTF-8")] += 1 }

    assert_equal expected, counted,
                 "both tasklets ran, the runtime performed the hop, and one Cell per word counted"
    assert_equal "totals", released.first.channel
    %w[flz.cell flz.seq flz.key].each do |stamp|
      assert_includes released.first.metadata, stamp,
                      "a keyed sink carries the identity stamps a deployed run stamps"
    end
  end

  def test_a_channel_the_class_does_not_declare_is_refused_by_name
    run = Flezha::LocalRelay.start(WordCount)
    begin
      error = assert_raises(Flezha::Error) { run.feed("nope", "1") }

      assert_equal ["runtime.channel_undeclared", "nope"], [error.code, error.details["channel"]]
      assert_includes error.details["channels"], "lines"
    ensure
      run.close
    end
  end

  # Reads the map an undeclared entry decodes, and emits a string and a map.
  class Defaults < Flezha::Tasklet
    tasklet_name "defaults"
    def run(value, ctx)
      ctx.emit("a is #{value[:a]}")
      ctx.emit({ a: value[:a] })
      nil
    end
  end

  class DefaultCodecs < Flezha::Relay
    factory_id "local_default_codecs_ruby"
    channels { |c| c.input("in").into(Defaults).output("out") }
  end

  ECHOLESS_CALLS = Queue.new

  class Echoless < Flezha::Relay
    factory_id "local_echoless_ruby"
    input "ticks"
    codec :text

    def run(tick, _ctx)
      ECHOLESS_CALLS << tick
      "got #{tick}"
    end
  end

  def test_a_short_form_with_no_output_whose_run_returns_a_value_runs_it_once
    run = Flezha::LocalRelay.start(Echoless)
    begin
      run.feed("ticks", "a")
      run.run_until_idle(5_000)
    ensure
      run.close
    end

    assert_equal 1, ECHOLESS_CALLS.size, "a returned value is not output, and nothing is retried"
  end

  def test_an_undeclared_entry_reads_json_and_an_undeclared_output_follows_the_value
    released = []
    run = Flezha::LocalRelay.start(DefaultCodecs)
    begin
      run.on_output("out") { |output| released << output.payload }
      run.feed("in", '{"a":1}')
      run.run_until_idle(5_000)
    ensure
      run.close
    end

    assert_equal ["a is 1", '{"a":1}'], released
  end
end
