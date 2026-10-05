# frozen_string_literal: true

require_relative "test_helper"

# `call`: a chain's end on another relay. The builder records the `call`
# link and declares the Response channel `<channel>.response`; an emit on the
# calling channel leaves as one plugin output per request, with the call
# chain's transforms and key applied here; each outcome arrives as a
# `Flezha::Response` decoded from the controller's envelope. The refusals are
# the core's, reached through this builder.
class CallTest < Minitest::Test
  ENVELOPES = JSON.parse(
    File.read(Flezha::TestSupport.contract_file("test/fixtures/call_response_envelope.json"))
  ).fetch("cases")

  RAW = ->(bytes) { bytes }

  class Ask < Flezha::Tasklet
    tasklet_name "ask"
    def run(input, ctx) = ctx.emit(input)
  end

  def relay(factory, run: nil, &channels)
    klass = Class.new(Flezha::Relay)
    klass.factory_id factory
    klass.run(run) if run
    klass.channels(&channels)
    klass
  end

  def decl(klass) = Flezha::Graph.to_host_class_decl(klass.spec)

  def validate(spec)
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    Flezha::TestSupport.validate(spec)
  end

  def codes(klass)
    Flezha::TestSupport.codes(assert_raises(Flezha::Error) { validate(klass.spec) })
  end

  def back_into_caller
    relay("call_back") { |c| c.input("in").into(Ask).named("quote").call("pricing", deadline_ms: 500).into(Ask) }
  end

  def test_the_call_link_closes_the_chain_and_the_response_channel_is_an_entry
    channels = decl(back_into_caller)["channels"]

    assert_equal [[{ "link" => "named", "name" => "quote" }],
                  [{ "link" => "call", "target" => "pricing", "deadline_ms" => 500, "create" => true,
                     "response" => "quote.response" }]],
                 channels[1]["chains"]
    assert_equal %w[quote.response entry], channels[2].values_at("name", "kind")
    assert_equal [[{ "link" => "into", "tasklet" => 0 }]], channels[2]["chains"]
  end

  def test_a_factory_call_records_create_channel_and_its_key
    klass = relay("call_factory") do |c|
      c.input("in").into(Ask).key_by(&:to_s)
       .call(factory: "agent", deadline_ms: 60_000, create: false, channel: "turn",
             answer_channel: "reply").into(Ask)
    end

    assert_equal [{ "link" => "key" },
                  { "link" => "call", "factory" => "agent", "deadline_ms" => 60_000, "create" => false,
                    "channel" => "turn", "answer_channel" => "reply", "response" => "ask.response" }],
                 decl(klass)["channels"][1]["chains"][0]
  end

  def test_the_core_derives_the_call_end_and_its_response_channel
    derived = validate(back_into_caller.spec)["channels"].map do |channel|
      [channel["name"], channel["kind"], channel["producer"], channel["consumers"].map { |e| e["tasklet"] }]
    end

    assert_equal [["in", "entry", nil, [0]], ["quote", "plugin", 0, []], ["quote.response", "entry", nil, [0]]],
                 derived
  end

  def test_the_refusals_are_the_cores
    refused = {
      "topology.channel_ends_twice" =>
        relay("twice") { |c| c.input("in").into(Ask).sink("out").call("p", deadline_ms: 5).into(Ask) },
      "topology.call_deadline_zero" =>
        relay("zero") { |c| c.input("in").into(Ask).call("p", deadline_ms: 0).into(Ask) },
      "topology.call_without_key" =>
        relay("unkeyed") { |c| c.input("in").into(Ask).call(factory: "a", deadline_ms: 5, create: false).into(Ask) },
      "topology.call_response_unread" =>
        relay("unread") { |c| c.input("in").into(Ask).call("p", deadline_ms: 5) },
      "topology.call_outside_per_input_loop" =>
        relay("once", run: :steps) { |c| c.input("in").into(Ask).call("p", deadline_ms: 5).into(Ask) }
    }

    refused.each { |code, klass| assert_includes codes(klass), code }
    per_input = relay("per_input", run: { steps: { each: :per_input } }) do |c|
      c.input("in").into(Ask).call("p", deadline_ms: 5).into(Ask)
    end
    assert validate(per_input.spec)
  end

  def test_a_call_on_an_entry_channel_is_refused_as_a_sink_there_is
    klass = relay("on_entry") { |c| c.input("in").call("p", deadline_ms: 5) }

    error = assert_raises(Flezha::Error) { klass.spec }
    assert_equal ["topology.sink_on_entry", "in"], [error.code, error.details["channel"]]
  end

  class Gateway < Flezha::Relay
    factory_id "call_gateway_emit"
    channels do |c|
      asked = c.input("in", codec: :text).into(Ask)
      asked.filter { |line| line != "drop" }.map { |line| "req:#{line}" }.key_by { |line| line.split(":")[1] }
           .call(factory: "agent", deadline_ms: 5, codec: :text).into(Ask)
    end
  end

  def test_an_emit_on_a_calling_channel_leaves_one_plugin_request_per_value
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    deps = Flezha::TestSupport.wired_deps(Gateway)
    items = [Flezha::TestSupport.input_item("drop"), Flezha::TestSupport.input_item("s1")]
    effects = Flezha::Dispatch.answer(Flezha::TestSupport.activation(factory: "call_gateway_emit", items: items),
                                      deps)[:effects]

    assert_empty effects[0][:outputs]
    request, = effects[1][:outputs]
    assert_equal ["plugin", "ask", "s1", "req:s1"], request.values_at(:target, :plugin_channel, :key, :payload)
  end

  def test_the_response_channel_decodes_the_envelope_with_the_calls_codec
    response_channel = Gateway.spec.channels.find { |channel| channel.name == "ask.response" }
    envelope = { call: "c", target: "agent-1", created: true, request: ["s1"].pack("m0"),
                 value: ["fine"].pack("m0") }.to_json

    assert_equal Flezha::Response.new(request: "s1", value: "fine", error: nil, target: "agent-1", created: true),
                 response_channel.decode.call(envelope.b)
  end

  ENVELOPES.each do |envelope|
    define_method("test_the_envelope_#{envelope['name']}_decodes") do
      expect = envelope["expect"]
      response = Flezha::Response.decode(envelope["payload"], RAW)

      assert_equal [expect["target"], expect["created"], expect["request"].unpack1("m0")],
                   [response.target, response.created, response.request]
      assert_equal expect["ok"], response.ok?
      if expect["ok"]
        assert_equal expect["value"].unpack1("m0"), response.value
      else
        assert_nil response.value
        assert_equal expect.values_at("error_code", "error_class", "error_message"),
                     [response.error.code, response.error.error_class, response.error.message]
        assert_kind_of Flezha::Error, response.error
      end
    end
  end
end
