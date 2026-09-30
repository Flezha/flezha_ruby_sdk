# frozen_string_literal: true

require "flezha"

# Answers a request for a session's events with its stream, and any other
# request with 404.
class Watch < Flezha::Tasklet
  tasklet_name "watch"

  def run(request, ctx)
    ctx.emit(request.listens? ? { stream: request.user } : { status: 404, body: "no such route" })
  end
end
