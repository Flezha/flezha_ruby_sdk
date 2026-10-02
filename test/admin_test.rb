# frozen_string_literal: true

require_relative "test_helper"

# Starting from application code: the factory first, the id only when the
# application names one, and an answer of exactly the id and the outcome.
class AdminTest < Minitest::Test
  # The admin call the controller receives, and the answer it gives.
  def start(answer, *args, **options)
    calls = []
    core = Flezha::TestSupport::FakeCore.new
    result = core.stub(:admin_call, ->(verb, json) { calls << [verb, JSON.parse(json)] && answer }) do
      Flezha::Admin.new(core).start_operator(*args, **options)
    end
    [calls, result]
  end

  def test_a_start_without_an_id_sends_none_and_answers_the_generated_one
    calls, result = start('{"id":"echo-k7m2qzxw4pd3fn2a","outcome":"started"}', "echo")

    assert_equal [["Operator.start", [{ "factory_id" => "echo" }]]], calls
    assert_equal Flezha::StartResult.new(id: "echo-k7m2qzxw4pd3fn2a", outcome: :started), result
    assert_equal %i[id outcome], result.members
  end

  def test_a_start_with_an_id_names_it_and_a_running_one_answers_already_running
    answer = '{"id":"my_echo","outcome":"already_running"}'
    calls, result = start(answer, "echo", id: "my_echo")

    assert_equal [["Operator.start", [{ "factory_id" => "echo", "id" => "my_echo" }]]],
                 calls
    assert_equal Flezha::StartResult.new(id: "my_echo", outcome: :already_running), result
  end

  def test_a_plugin_is_enabled_by_name_and_bound_and_unbound_by_operator
    calls = []
    core = Flezha::TestSupport::FakeCore.new
    core.stub(:admin_call, ->(verb, json) { calls << [verb, JSON.parse(json)] && "null" }) do
      admin = Flezha::Admin.new(core)
      admin.enable_plugin("writer", "operator_file_sink", opts: { "path" => "out.txt" })
      admin.enable_plugin("feed", "operator_websocket", single: true, operator_id: "my_echo",
                                                        hooks: [["on_output", {}]])
      admin.bind_plugin("writer", "my_echo")
      admin.bind_plugin("writer", "my_other", hooks: ["on_output"], channel: "in")
      admin.unbind_plugin("writer", "my_echo")
      admin.disable_plugin("writer")
      admin.list_plugins
    end

    assert_equal [
      ["PluginManager.enable", ["writer", { "provider" => "operator_file_sink", "opts" => { "path" => "out.txt" } }]],
      ["PluginManager.enable", ["feed", { "provider" => "operator_websocket", "opts" => {}, "single" => true,
                                          "operator_id" => "my_echo", "hooks" => [["on_output", {}]] }]],
      ["PluginManager.bind", ["writer", { "operator_id" => "my_echo" }]],
      ["PluginManager.bind", ["writer", { "operator_id" => "my_other", "hooks" => ["on_output"], "channel" => "in" }]],
      ["PluginManager.unbind", ["writer", { "operator_id" => "my_echo" }]],
      ["PluginManager.disable", ["writer", {}]],
      ["PluginManager.list", []]
    ], calls
  end

  def test_a_refused_start_raises_as_every_refused_admin_call_does
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    core = Flezha::TestSupport::FakeCore.new
    refusal = ->(*) { raise Flezha::Error.new("quota.exceeded", kind: "max_operators", current: 3, limit: 3) }

    error = assert_raises(Flezha::Error) do
      core.stub(:admin_call, refusal) { Flezha::Admin.new(core).start_operator("echo") }
    end
    assert_equal ["quota.exceeded", "max_operators"], [error.code, error.details["kind"]]
  end
end
