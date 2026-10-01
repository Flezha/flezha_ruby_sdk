# frozen_string_literal: true

# One stepped session per key inside a flow gateway, the mirror of
# `sdk/rust/test_app/src/inner.rs`: `key:text` inputs; each key's session
# counts its turns in its own state and answers `key#turns:text` on the
# gateway's `replies` sink. The session sleeps after
# `FLEZHA_FIXTURE_SLEEP_AFTER_MS` and wakes on its key's next message with its
# count intact, while the gateway and every other key go on. Its second stage
# fails on the text `fail` (the turn is undone) and holds `hold` until the
# release env (a crash lands between the two stages); the gateway's reply holds
# `stall` the same way (a crash lands after the session answered).
#
# `loop_gateway` embeds the chained `countdown`: `label:n` counts down one per
# pass, and only the converged `label:0` reaches the gateway's `replies`.

require "flezha"

require_relative "fixtures"
require_relative "fixtures_steps"

module Fixtures
  class SessionTurn < Flezha::Tasklet
    tasklet_name "session_turn"
    def init_state(_key) = { value: 0 }
    def apply_change(state, change) = state[:value] += change[:delta]

    def run(input, ctx)
      key, text = input.to_s.split(":", 2)
      ctx.change({ delta: 1 })
      ctx.emit("#{key}##{ctx.state[:value]}:#{text}")
    end

    def compensate(_outputs, ctx) = ctx.change({ delta: -1 })
  end

  class SessionCheck < Flezha::Tasklet
    tasklet_name "session_check"

    def run(input, ctx)
      case input.to_s.split(":", 2).last
      when "fail" then raise("session_check refuses fail")
      when "hold" then Fixtures.block_until_released
      end
      ctx.emit(input)
    end
  end

  class SessionSteps < Flezha::Operator
    factory_id "session"
    run steps: { each: :per_input }
    durable FIXTURE_DURABLE
    # A host tasklet emits only on a channel it declares: this one is the exit.
    channels { |c| c.input("turns", codec: :text).into(SessionTurn).into(SessionCheck).named("answer") }
  end

  class GatewayReply < Flezha::Tasklet
    tasklet_name "gateway_reply"
    def run(input, ctx)
      Fixtures.block_until_released if input.to_s.split(":", 2).last == "stall"
      ctx.emit(input)
    end
  end

  class Gateway < Flezha::Operator
    factory_id "session_gateway"
    # It journals its input (a kept instance that may sleep), so an answer a
    # session passed it is replayed after a crash.
    durable({ local: { sleep_after: 3_600_000 } })
    channels do |c|
      c.input("requests", codec: :text)
       .key_by { |value| PREFIX.call(value) }
       .into(SessionSteps)
       .into(GatewayReply)
       .sink("replies", codec: :text)
    end
  end

  class CountdownStep < Flezha::Tasklet
    tasklet_name "countdown_step"

    def run(input, ctx)
      label, n = input.to_s.split(":", 2)
      ctx.emit("#{label}:#{Integer(n) - 1}") if Integer(n).positive?
    end
  end

  class CountdownSteps < Flezha::Operator
    factory_id "countdown"
    run steps: { each: :chained }
    channels { |c| c.input("in", codec: :text).into(CountdownStep).named("carry") }
  end

  class LoopGateway < Flezha::Operator
    factory_id "loop_gateway"
    channels do |c|
      c.input("requests", codec: :text)
       .into(CountdownSteps)
       .into(GatewayReply)
       .sink("replies", codec: :text)
    end
  end

  # The embedded classes are not registered on their own.
  INNER = [Gateway, LoopGateway].freeze
end
