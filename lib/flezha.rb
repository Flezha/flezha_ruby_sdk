# frozen_string_literal: true

# The Flezha Ruby SDK: a poll host over `flz_core`.
#
# The SDK contains no kernel code. Transport, framing, ordering, per-cell
# exclusivity, durability, retries, the retry-to-park ladder, timers, credit,
# routing, the steps driver and reconnect all live in the core; this gem
# brings the authoring surface, the codec for user types, the threading host
# and idiomatic errors.
#
#   class Echo < Flezha::Tasklet
#     tasklet_name "echo"
#     def run(input, ctx) = ctx.emit(input)
#   end
#
#   class EchoRelay < Flezha::Relay
#     factory_id "echo"
#     channels { |c| c.input("input", codec: :text).into(Echo).sink("out") }
#   end
#
#   Flezha.create_runner("default").register_factory(EchoRelay).run

require_relative "flezha/version"
require_relative "flezha/errors"
require_relative "flezha/env"
require_relative "flezha/logger"
require_relative "flezha/identity"
require_relative "flezha/codec"
require_relative "flezha/response"
require_relative "flezha/state"
require_relative "flezha/builder"
require_relative "flezha/tasklet"
require_relative "flezha/route_latest"
require_relative "flezha/relay"
require_relative "flezha/graph"
require_relative "flezha/ctx"
require_relative "flezha/dispatch"
require_relative "flezha/native"
require_relative "flezha/control"
require_relative "flezha/executor"
require_relative "flezha/runner"
require_relative "flezha/local"
require_relative "flezha/testing"
