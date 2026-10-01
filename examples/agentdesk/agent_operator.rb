# frozen_string_literal: true

require "flezha"
require_relative "reply"
require_relative "deliver"

# One conversation: an operator that runs in steps, one turn per message it is
# sent, for as long as the session lasts. Every session is an instance of its
# own, with its own state.
class AgentOperator < Flezha::Operator
  factory_id "agent"
  run steps: { each: :per_input, on_failure: :keep_progress }   # one turn per message; a failed turn fails alone
  durable local: { sleep_after: 10_000 }                       # an idle session sleeps after 10 s

  channels do |c|
    c.input("turn", codec: :text)
     .into(Reply)
     .sink("answer")                                         # to whoever listens
     .into(Deliver)
     .response                                               # to whoever called
    c.input("message")                                       # from the gateway
     .map { |message| message[:text] }
     .into(Reply)
  end
end
