# frozen_string_literal: true

require "flezha"

# A processing stage of the operator's graph, run once per message. This one
# prefixes the input, and what `run` returns is emitted.
#
# With no `initial_state` and no `apply_change`, nothing is kept between
# messages and nothing is journaled. Where the result goes is not decided here:
# the operator decides that, so the graph can be rewired without touching this
# class.
class EchoTasklet < Flezha::Tasklet
  # The tasklet's stable identity. State is filed under it, so renaming it
  # later declares a different tasklet rather than renaming this one.
  tasklet_name "echo"

  def run(input, _ctx) = "echo: #{input}"
end
