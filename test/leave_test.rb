# frozen_string_literal: true

require_relative "test_helper"

# A leave: the core tells the host a cell, or a whole relay, left this runner,
# and a cell started again under the same name begins at its initial state.
class LeaveTest < Minitest::Test
  class Total < Flezha::Tasklet
    tasklet_name "total"
    def init_state(_key) = { total: 0 }
    def apply_change(state, change) = state[:total] += change[:by]
    def run(input, ctx) = ctx.change({ by: Integer(input) })
  end

  class TotalRelay < Flezha::Relay
    factory_id "total_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") }).into(Total).output("out")
    end
  end

  def setup
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    @deps = Flezha::TestSupport.wired_deps(TotalRelay, Flezha::TestSupport::FakeCore.new)
  end

  def answer(kind, **extra)
    Flezha::Dispatch.answer(Flezha::TestSupport.activation(factory: "total_op", kind: kind, **extra), @deps)
  end

  def total(**at) = Flezha::Codec.from_state_json(answer(:live_state_read, **at)[:json])[:total]

  def test_a_cell_that_left_starts_again_at_its_initial_state_as_does_every_cell_of_a_relay_that_left
    answer(:work, items: [Flezha::TestSupport.input_item("3")])
    answer(:work, relay: "op-2", items: [Flezha::TestSupport.input_item("4")])

    assert_equal "state_applied", answer(:leave, whole_relay: false)[:outcome]
    assert_equal 0, total
    assert_equal 4, total(relay: "op-2")

    answer(:leave, relay: "op-2", whole_relay: true)
    assert_equal 0, total(relay: "op-2")
  end
end
