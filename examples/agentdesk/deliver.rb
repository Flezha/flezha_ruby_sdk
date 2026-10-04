# frozen_string_literal: true

require "flezha"

# Hands the finished answer to the caller: a channel has one end, and `answer`
# is a sink.
class Deliver < Flezha::Tasklet
  tasklet_name "deliver"

  def run(answer, ctx) = ctx.emit(answer) # the whole answer, to whoever called

  # The undo, when a turn is taken back: an answer already given has nothing to take back.
  def compensate(_answers, _ctx); end
end
