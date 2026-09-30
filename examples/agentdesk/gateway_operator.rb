# frozen_string_literal: true

require "flezha"
require_relative "request"
require_relative "watch"
require_relative "budget"
require_relative "admit"

# The HTTP side of the desk: an unbounded operator that carries the HTTP plugin,
# answers every request, and calls the session a message is for.
class GatewayOperator < Flezha::UnboundedOperator
  factory_id "gateway"

  DEADLINE_MS = 60_000

  channels do |c|
    c.effectively_once                                       # each request and answer on disk before it runs
    user = ->(value) { value[:user] }
    http = c.input("http", decode: Request.method(:decode))
    http.filter { |request| !request.sends? }
        .into(Watch)
        .response                                            # a stream, or 404
    verdicts = http.filter(&:sends?)
                   .map(&:message)
                   .key_by(&user)                            # one cell per user
                   .into(Budget)
    verdicts.into(Admit)
            .response                                        # 202, or 429 over the budget
    verdicts.filter { |verdict| Budget.admitted?(verdict) }
            .map { |verdict| verdict[:message] }
            .key_by(&user)                                   # the user's session
            .call(factory: "agent", channel: "message", deadline_ms: DEADLINE_MS)
            .map { |response| Budget.used(response) }
            .key_by(&user)
            .into(Budget)                                    # the tokens the answer used
  end
end
