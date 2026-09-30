# frozen_string_literal: true

# Starts the desk's runner: the process that connects to the controller and
# serves agentdesk's operators, in the `agents` namespace.

require "flezha"
require_relative "agent_operator"
require_relative "gateway_operator"

# The sessions print their undos; written at once, they reach the log in the
# order they happened.
$stdout.sync = true

runner = Flezha.create_runner("agents")
               .register_factory(AgentOperator)
               .register_factory(GatewayOperator)
               .boot

runner.await_termination
