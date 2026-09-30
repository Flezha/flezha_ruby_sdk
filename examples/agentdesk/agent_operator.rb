# frozen_string_literal: true

require "flezha"
require_relative "reply"
require_relative "deliver"

# One conversation: a bounded operator that runs one turn per message it is
# sent, for as long as the session lasts. Every session is an instance of its
# own, with its own state.
class AgentOperator < Flezha::BoundedOperator
  factory_id "agent"
  loop_mode :per_input                                       # one turn per message
  rollback_mode :keep_progress                               # a failed turn fails alone

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
