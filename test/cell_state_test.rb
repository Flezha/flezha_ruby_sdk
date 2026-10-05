# frozen_string_literal: true

require_relative "test_helper"

# A cell belongs to one relay.
#
# One registered class serves every relay of its factory on a runner, such
# as each instance of a steps relay or each partition of a stream, and
# each has cells of its own under the same tasklet and cell key. Every path
# that reads or writes a cell's state is driven here with two relays side
# by side.
class CellStateTest < Minitest::Test
  # Keeps what it was sent, and undoes by emitting what its own cell holds.
  class Kept < Flezha::Tasklet
    tasklet_name "kept"
    def init_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change
    def run(value, ctx) = ctx.change(value)
    def compensate(_outputs, ctx) = ctx.emit(ctx.state[:seen].dup)
  end

  class KeptRelay < Flezha::Relay
    factory_id "kept_relay"
    channels { |c| c.input("input", codec: :text).into(Kept).sink("out", codec: :msgpack) }
  end

  def setup
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    @deps = Flezha::TestSupport.wired_deps(KeptRelay)
  end

  def answer(kind, relay, **extra)
    Flezha::Dispatch.answer(
      Flezha::TestSupport.activation(factory: "kept_relay", kind: kind, relay: relay,
                                     **extra),
      @deps
    )
  end

  def work(relay, value)
    effect, = answer(:work, relay, items: [Flezha::TestSupport.input_item(value)])[:effects]
    assert_equal "ok", effect[:verdict], effect[:message]
  end

  def read(relay) = Flezha::Codec.from_state_json(answer(:live_state_read, relay)[:json])

  def test_two_relays_of_one_factory_keep_their_own_cell
    work("order_a", "a")
    work("order_b", "b")
    work("order_a", "c")

    assert_equal({ seen: %w[a c] }, read("order_a"))
    assert_equal({ seen: %w[b] }, read("order_b"))
  end

  def test_a_restore_and_an_applied_state_reach_only_their_own_relay
    restored = answer(:restore_fold, "order_a", changes: [{ bytes: Flezha::Codec.encode("r") }])
    applied = answer(:apply_state, "order_b", state_json: '{"seen":["m"]}'.b)

    assert_equal "restored", restored[:outcome]
    assert_equal "state_applied", applied[:outcome]
    assert_equal({ seen: %w[r] }, read("order_a"))
    assert_equal({ seen: %w[m] }, read("order_b"))
    assert_equal({ seen: [] }, read("order_c"))
  end

  def test_a_snapshot_and_a_migration_serialize_read_their_own_relays_cell
    work("order_a", "a")
    work("order_b", "b")

    snapshot = answer(:snapshot_request, "order_b")[:state]
    migrated = answer(:migration_serialize, "order_b")[:json]

    assert_equal({ seen: %w[b] }, Flezha::Codec.decode(snapshot))
    assert_equal({ seen: %w[b] }, Flezha::Codec.from_state_json(migrated))
  end

  def test_a_compensation_reads_its_own_relays_cell
    work("order_a", "a")
    work("order_b", "b")

    effect, = answer(:compensate_run, "order_b", outputs: [])[:effects]

    assert_equal %w[b], Flezha::Codec.decode(effect[:outputs][0][:payload])
  end

  # Each thread drives its own relay, so what each cell holds does not
  # depend on how the threads interleave; a shared cell would hold both.
  def test_relays_served_on_several_threads_keep_their_own_cell
    relays = %w[order_a order_b order_c order_d]
    relays.map { |relay| Thread.new { 50.times { |i| work(relay, "#{relay}#{i}") } } }
          .each(&:join)

    relays.each do |relay|
      assert_equal((0...50).map { |i| "#{relay}#{i}" }, read(relay)[:seen])
    end
  end

  def test_a_harness_restore_fills_the_cell_it_reads
    harness = Flezha::Testing::Harness.new(Kept, relay: "order_a")
    harness.restore(changes: [Flezha::Codec.encode("r")])

    assert_equal({ seen: %w[r] }, harness.state)
  end
end
