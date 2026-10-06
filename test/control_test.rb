# frozen_string_literal: true

require_relative "test_helper"

# Starting from application code: the factory first, the id only when the
# application names one, and an answer of exactly the id and the outcome.
class ControlTest < Minitest::Test
  # The control call the controller receives, and the answer it gives.
  def start(answer, *args, **options)
    calls = []
    core = Flezha::TestSupport::FakeCore.new
    result = core.stub(:admin_call, ->(verb, json) { calls << [verb, JSON.parse(json)] && answer }) do
      Flezha::Control.new(core).start_relay(*args, **options)
    end
    [calls, result]
  end

  def test_a_start_without_an_id_sends_none_and_answers_the_generated_one
    calls, result = start('{"id":"echo-k7m2qzxw4pd3fn2a","outcome":"started"}', "echo")

    assert_equal [["Relay.start", [{ "factory_id" => "echo" }]]], calls
    assert_equal Flezha::StartResult.new(id: "echo-k7m2qzxw4pd3fn2a", outcome: :started), result
    assert_equal %i[id outcome], result.members
  end

  def test_a_start_with_an_id_names_it_and_a_running_one_answers_already_running
    answer = '{"id":"my_echo","outcome":"already_running"}'
    calls, result = start(answer, "echo", id: "my_echo")

    assert_equal [["Relay.start", [{ "factory_id" => "echo", "id" => "my_echo" }]]],
                 calls
    assert_equal Flezha::StartResult.new(id: "my_echo", outcome: :already_running), result
  end

  def test_a_plugin_is_enabled_by_name_and_bound_and_unbound_by_relay
    calls = []
    core = Flezha::TestSupport::FakeCore.new
    core.stub(:admin_call, ->(verb, json) { calls << [verb, JSON.parse(json)] && "null" }) do
      control = Flezha::Control.new(core)
      control.enable_plugin("writer", "relay_file_output", opts: { "path" => "out.txt" })
      control.enable_plugin("feed", "relay_websocket", single: true, relay_id: "my_echo",
                                                       hooks: [["on_output", {}]])
      control.bind_plugin("writer", "my_echo")
      control.bind_plugin("writer", "my_other", hooks: ["on_output"], channel: "in")
      control.unbind_plugin("writer", "my_echo")
      control.disable_plugin("writer")
      control.list_plugins
    end

    assert_equal [
      ["PluginManager.enable", ["writer", { "provider" => "relay_file_output", "opts" => { "path" => "out.txt" } }]],
      ["PluginManager.enable", ["feed", { "provider" => "relay_websocket", "opts" => {}, "single" => true,
                                          "relay_id" => "my_echo", "hooks" => [["on_output", {}]] }]],
      ["PluginManager.bind", ["writer", { "relay_id" => "my_echo" }]],
      ["PluginManager.bind", ["writer", { "relay_id" => "my_other", "hooks" => ["on_output"], "channel" => "in" }]],
      ["PluginManager.unbind", ["writer", { "relay_id" => "my_echo" }]],
      ["PluginManager.disable", ["writer", {}]],
      ["PluginManager.list", []]
    ], calls
  end

  def test_a_refused_start_raises_as_every_refused_control_call_does
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    core = Flezha::TestSupport::FakeCore.new
    refusal = ->(*) { raise Flezha::Error.new("quota.exceeded", kind: "max_relays", current: 3, limit: 3) }

    error = assert_raises(Flezha::Error) do
      core.stub(:admin_call, refusal) { Flezha::Control.new(core).start_relay("echo") }
    end
    assert_equal ["quota.exceeded", "max_relays"], [error.code, error.details["kind"]]
  end

  # A core that records what crosses to it.
  class Recording
    attr_reader :calls

    def initialize(answer)
      @answer = answer
      @calls = []
    end

    def admin_call(verb, json) = (@calls << [verb, JSON.parse(json)]) && @answer
    def send_message(*args) = (@calls << args) && (args.last ? "null" : @answer)
  end

  def runner_over(core) = Flezha::Runner.new("test-ns").tap { |r| r.instance_variable_set(:@core, core) }

  def test_a_send_hands_its_options_to_the_core_and_reads_the_answer
    core = Recording.new('{"to":"prices#p1","verdict":"absorbed"}')
    runner = runner_over(core)
    runner.send_to("my_echo", "hello", "in")
    verdict = runner.send_to("prices", "x", "in", msg_id: "fx/0/7", key: "EURUSD")

    assert_nil runner.send_to("orders", "hé", "in", local: true)
    assert_equal [["my_echo", "in", "hello".b, nil, nil, false],
                  ["prices", "in", "x".b, "fx/0/7", "EURUSD", false],
                  ["orders", "in", "hé".b, nil, nil, true]],
                 core.calls
    assert_equal({ "to" => "prices#p1", "verdict" => "absorbed" }, verdict)
  end

  def test_a_call_sends_the_value_as_an_output_would_and_decodes_each_answer
    replies = ['{"answer":{"status":"placed"},"answer_codec":"json"}', '{"answer":"ok","answer_codec":"text"}',
               '{"answer":"AQI=","answer_codec":"bytes"}', '{"accepted":true}']
    calls = []
    core = Flezha::TestSupport::FakeCore.new
    answers = core.stub(:admin_call, ->(verb, json) { calls << [verb, JSON.parse(json)] && replies.shift }) do
      runner = runner_over(core)
      [runner.call("orders", "place", { "pair" => "EUR/USD" }), runner.call("orders", "note", "hé"),
       runner.call("orders", "raw", "\x07".b), runner.call("orders", "audit", "x")]
    end

    assert_equal [{ "status" => "placed" }, "ok", "\x01\x02".b, { "accepted" => true }], answers
    base64 = ->(text) { [text].pack("m0") }
    assert_equal [["Relay.call_channel", ["orders", "place", base64.('{"pair":"EUR/USD"}'), { "payload" => "base64" }]],
                  ["Relay.call_channel", ["orders", "note", base64.("hé"), { "payload" => "base64" }]],
                  ["Relay.call_channel", ["orders", "raw", "Bw==", { "payload" => "base64" }]],
                  ["Relay.call_channel", ["orders", "audit", base64.("x"), { "payload" => "base64" }]]], calls
  end

  def test_a_refused_call_raises_the_controller_code
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    core = Flezha::TestSupport::FakeCore.new
    refusal = ->(*) { raise Flezha::Error.new("api.channel_unknown", channel: "plce", channels: "place") }
    error = assert_raises(Flezha::Error) do
      core.stub(:admin_call, refusal) { runner_over(core).call("orders", "plce", {}) }
    end
    assert_equal "api.channel_unknown", error.code
  end

  # Against the real core: no runner here holds `elsewhere`.
  def test_the_core_refuses_a_local_send_held_elsewhere_or_with_an_id
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    core = Flezha::Core.create
    begin
      runner = runner_over(core)
      error = assert_raises(Flezha::Error) { runner.send_to("elsewhere", "x", "in", local: true) }
      assert_equal "runtime.relay_not_local", error.code
      error = assert_raises(Flezha::Error) { runner.send_to("orders", "x", "in", local: true, msg_id: "m-1") }
      assert_equal "usage.options_conflict", error.code
      assert_kind_of Array, runner.relays
    ensure
      core.free
    end
    refused = assert_raises(Flezha::Error) { Flezha::Runner.new("test-ns").relays }
    assert_equal "runtime.runner_not_started", refused.code
  end
end
