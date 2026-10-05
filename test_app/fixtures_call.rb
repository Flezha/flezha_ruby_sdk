# frozen_string_literal: true

# Twins of the reference's relay call fixtures
# (sdk/rust/test_app/src/relay_call.rs), under the same factory ids,
# tasklet names, channel names and payloads: relays that call another
# relay from their graph and read the outcome as a `Flezha::Response`.
#
# - `call_pricing`: a flow callee answering `price:<input>` on its sink
#   `quotes`; an input starting `silent` is never answered.
# - `call_orders`: a flow caller calling the instance `pricing`, answered from
#   `quotes`.
# - `call_agent`: a durable per-input keep-progress session answering
#   `answer:<turn>`; `boom` fails the turn, `slow` answers after 2 s.
# - `call_gateway`: an effectively-once caller calling `call_agent` by the
#   session key of each `<session>:<text>` turn, and without creating it for a
#   `resume:<session>:<text>` turn, both Response channels wired back into its
#   calling tasklet.
# - `call_tool_session`: a durable per-input session calling `pricing` from a
#   turn and reading the outcome as its next turn.
#
# Every outcome is written to a sink as one line,
# `<request>=ok:<answer>@<target>/<created>` or
# `<request>=error:<code>@<target>/<created>`.

require "flezha"

require_relative "fixtures_state"

module Fixtures
  module RelayCall
    DEADLINE_MS = 30_000

    # The prefix a caller's tasklet puts on an outcome line, which no request
    # carries, so one output channel can feed both the call and the sink.
    OUTCOME = "response "

    # The prefix of a turn resuming a session that must already run: its call
    # may not create the callee.
    RESUME = "resume:"

    module_function

    def render(response)
      outcome = response.ok? ? "ok:#{response.value}" : "error:#{response.error.code}"
      "#{response.request}=#{outcome}@#{response.target}/#{response.created}"
    end

    # What a caller's tasklet emits: a turn as it came, an outcome as its line.
    def pass(input) = input.is_a?(Flezha::Response) ? "#{OUTCOME}#{render(input)}" : input

    def outcome?(line) = line.start_with?(OUTCOME)
    def resume?(line) = line.start_with?(RESUME)
    def turn?(line) = !outcome?(line) && !resume?(line)

    # The session a turn or an outcome line belongs to: what precedes its `:`.
    def session(line) = line.delete_prefix(OUTCOME).split(":", 2).first.to_s
  end

  class PricingQuote < Flezha::Tasklet
    tasklet_name "pricing_quote"
    def run(input, ctx) = input.start_with?("silent") ? nil : ctx.emit("price:#{input}")
  end

  class CallPricing < Flezha::Relay
    factory_id "call_pricing"
    channels { |c| c.input("in", codec: :text).into(PricingQuote).sink("quotes") }
  end

  class OrderAsk < PassThrough
    tasklet_name "order_ask"
  end

  class OrderPriced < Flezha::Tasklet
    tasklet_name "order_priced"
    def run(input, ctx) = ctx.emit(RelayCall.render(input))
  end

  class CallOrders < Flezha::Relay
    factory_id "call_orders"
    channels do |c|
      c.input("in", codec: :text).into(OrderAsk).named("order")
       .call("pricing", deadline_ms: RelayCall::DEADLINE_MS, answer_channel: "quotes", codec: :text)
       .into(OrderPriced).sink("priced")
    end
  end

  class AgentTurn < Flezha::Tasklet
    tasklet_name "agent_turn"

    def run(input, ctx)
      text = input.include?(":") ? input.split(":", 2).last : input
      raise Flezha::NonRetryableError, "the turn failed" if text == "boom"

      sleep 2 if text == "slow"
      ctx.emit("answer:#{input}")
    end
  end

  class CallAgent < Flezha::Relay
    factory_id "call_agent"
    run steps: { each: :per_input, on_failure: :keep_progress }
    durable FIXTURE_DURABLE
    max_duration_ms 600_000
    channels { |c| c.input("turn", codec: :text).into(AgentTurn).sink }
  end

  class GatewayAccount < Flezha::Tasklet
    tasklet_name "gateway_account"
    def run(input, ctx) = ctx.emit(RelayCall.pass(input))
  end

  class GatewayResume < Flezha::Tasklet
    tasklet_name "gateway_resume"
    def run(input, ctx) = ctx.emit(input.delete_prefix(RelayCall::RESUME))
  end

  class GatewayLog < Flezha::Tasklet
    tasklet_name "gateway_log"
    def run(input, ctx) = ctx.emit(input.delete_prefix(RelayCall::OUTCOME))
  end

  class CallGateway < Flezha::Relay
    factory_id "call_gateway"
    run flow: { exactly: :effectively_once }
    channels do |c|
      account = c.input("in", codec: :text).into(GatewayAccount).named("gateway")
      account.filter { |line| RelayCall.turn?(line) }.key_by { |line| RelayCall.session(line) }
             .call(factory: "call_agent", deadline_ms: RelayCall::DEADLINE_MS, codec: :text)
             .into(GatewayAccount)
      account.filter { |line| RelayCall.resume?(line) }.into(GatewayResume).named("resume")
             .key_by { |line| RelayCall.session(line) }
             .call(factory: "call_agent", deadline_ms: RelayCall::DEADLINE_MS, create: false, codec: :text)
             .into(GatewayAccount)
      account.filter { |line| RelayCall.outcome?(line) }.key_by { |line| RelayCall.session(line) }
             .into(GatewayLog).keyed.sink("gateway_out")
    end
  end

  class ToolTurn < Flezha::Tasklet
    tasklet_name "tool_turn"
    def run(input, ctx) = ctx.emit(RelayCall.pass(input))
  end

  class ToolLog < Flezha::Tasklet
    tasklet_name "tool_log"
    def run(input, ctx) = ctx.emit(input.delete_prefix(RelayCall::OUTCOME))
  end

  class CallToolSession < Flezha::Relay
    factory_id "call_tool_session"
    run steps: { each: :per_input }
    durable FIXTURE_DURABLE
    max_duration_ms 600_000
    channels do |c|
      turn = c.input("turn", codec: :text).into(ToolTurn).named("tool")
      turn.filter { |line| !RelayCall.outcome?(line) }
          .call("pricing", deadline_ms: RelayCall::DEADLINE_MS, codec: :text).into(ToolTurn)
      turn.filter { |line| RelayCall.outcome?(line) }.into(ToolLog).sink("tool_out")
    end
  end

  CALLS = [CallPricing, CallOrders, CallAgent, CallGateway, CallToolSession].freeze
end
