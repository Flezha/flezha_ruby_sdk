# frozen_string_literal: true

require_relative "test_helper"

# Every `HostClassDecl` key the core learns, spelled by this builder.
#
# The fixture is the list of record, shared with the `flz_core` test that
# asserts the core's own struct and with the Java and Node builders' tests. A
# key the core learns that Ruby cannot spell fails here, which is the point:
# the cross-SDK rule is that a declaration key lands in every SDK in one
# change, and this is what makes forgetting one a red build rather than a
# silently narrower SDK.
class DeclKeysTest < Minitest::Test
  KEYS_FILE = Flezha::TestSupport.contract_file("test/fixtures/host_class_decl_keys.txt")

  # A declaration populated far enough that every optional key is present.
  class Entry < Flezha::Tasklet
    tasklet_name "entry"
    mailbox_capacity 8
    max_timers_per_cell 16
    schema_version 3
    min_readable_schema_version 1
    def migrate_state(_from, bytes) = Flezha::Codec.decode(bytes)
    def init_state(_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]
    def event_time(input) = Integer(input)
    def run(input, ctx) = ctx.change({ push: input })
  end

  class Downstream < Flezha::Tasklet
    tasklet_name "downstream"
    idle_ttl_seconds 1
    max_cells 16
    def run(input, ctx) = ctx.emit(input)
    def compensate(_outputs, _ctx) = nil
  end

  class Fanned < Flezha::Tasklet
    tasklet_name "fanned"
    cell_keys "a", "b"
    def cells_for(_input) = ["a"]
    def run(input, ctx) = ctx.emit(input)
  end

  class FullFlow < Flezha::Relay
    factory_id "decl_keys_flow"
    description "every flow key"
    run flow: { exactly: { effectively_once: {
      ack: { checkpoint: { every: 4, outputs: :at_checkpoint, max_delay_ms: 250 } },
      correction_ms: 5_000, beyond_window: :drop
    } } }
    durable local: { sleep_after: 2_000 }
    max_duration_ms 60_000
    channels do |c|
      c.compact_after_bytes(4_096)
      edge = c.input("input").into(Entry)
      edge.named("hop").key_by { |value| value.to_s }.retry_max(5).retry_backoff_ms(50)
          .into(Downstream)
      edge.named("fan").into(Fanned)
      c.input("second", description: "every key", schema: { type: "object" })
       .into(Entry).keyed.output("sink")
    end
  end

  def spelled
    stream = Flezha::Graph.to_host_class_decl(FullFlow.spec)
    keys = stream.keys.map { |key| "class.#{key}" }
    keys |= stream["channels"].flat_map { |c| c.keys.map { |key| "channel.#{key}" } }
    keys |= stream["tasklets"].flat_map { |p| p.keys.map { |key| "tasklet.#{key}" } }
    keys.sort
  end

  def declared
    File.readlines(KEYS_FILE, chomp: true)
        .reject { |line| line.empty? || line.start_with?("#") }
        .sort
  end

  def test_the_builder_spells_every_declared_key
    missing = declared - spelled
    assert_empty missing,
                 "the Ruby builder cannot spell #{missing.join(', ')}; teach it, or the core " \
                 "learned a key this SDK will never send"
  end

  def test_the_builder_invents_no_key_the_core_never_learned
    # `deny_unknown_fields` would refuse one at declare time, but at the first
    # controller connection rather than here.
    extra = spelled - declared
    assert_empty extra, "the Ruby builder emits #{extra.join(', ')}, which the core never learned"
  end

  # A merge closes every source's chain in the core's link vocabulary: the
  # source's own links, its mapping, the merge's key, then the merge naming
  # the node and a group of its own per merge.
  def test_a_merge_records_each_source_chain_in_the_cores_link_shape
    klass = Class.new(Flezha::Relay)
    klass.factory_id "decl_keys_merge"
    klass.channels do |c|
      c.merge.from(c.input("a")) { |v| v }.from(c.input("b").flat_map { |v| [v] }) { |v| v }
       .key_by(&:to_s).into(Downstream)
      c.merge.from(c.input("c")) { |v| v }.from(c.input("d")) { |v| v }.into(Downstream, "other")
    end
    transform = { "link" => "transform" }
    key = { "link" => "key" }
    first = { "link" => "merge", "tasklet" => 0, "group" => 0 }
    second = { "link" => "merge", "tasklet" => 1, "group" => 1 }

    assert_equal [[[transform, key, first]], [[transform, transform, key, first]],
                  [[transform, second]], [[transform, second]]],
                 Flezha::Graph.to_host_class_decl(klass.spec)["channels"].map { |c| c["chains"] }
  end
end

# The cross-language topology golden.
#
# `test/fixtures/reference_topologies.txt` is the Rust reference's channel
# topology for two fixtures, transcribed by hand. Several SDKs implement one
# design with no compiler between them, so the file is what keeps them from
# drifting: this builder declares both shapes in Ruby's spelling and the lines
# the CORE DERIVES from its record must be the golden's, in order.
class ReferenceTopologiesTest < Minitest::Test
  TOPOLOGIES_FILE =
    Flezha::TestSupport.contract_file("test/fixtures/reference_topologies.txt")

  BYTES = ->(value) { value.to_s }

  class HybridProducer < Flezha::Tasklet
    tasklet_name "hybrid_producer_tasklet"
    def run(input, ctx) = ctx.emit(input)
  end

  class HybridConsumer < Flezha::Tasklet
    tasklet_name "hybrid_consumer_tasklet"
    def run(_input, _ctx) = nil
  end

  class HybridChannel < Flezha::Relay
    factory_id "hybrid_channel"
    channels do |c|
      c.input("input").into(HybridProducer)
       .output("hybrid_indicator", encode: BYTES).into(HybridConsumer)
    end
  end

  class TierCommitA < Flezha::Tasklet
    tasklet_name "tier_commit_a"
    def run(input, ctx) = ctx.emit(input, key: "job")
  end

  class BlockB < Flezha::Tasklet
    tasklet_name "block_b"
    def run(_input, _ctx) = nil
  end

  class CommitBlockChain < Flezha::Relay
    factory_id "commit_block_chain"
    run :steps
    channels do |c|
      c.input("in").into(TierCommitA).keyed
       .output("mid", encode: BYTES).into(BlockB)
    end
  end

  def golden(topology)
    File.readlines(TOPOLOGIES_FILE, chomp: true)
        .map(&:strip)
        .select { |line| line.start_with?("#{topology} |") }
  end

  # The DERIVED declaration rendered in the golden's columns, one line per
  # channel. Every column but the index is the core's answer now — the record
  # carries chains, and the consumers, the plugin kind and the output name are
  # read off them.
  def emitted(topology, spec)
    Flezha::TestSupport.validate(spec)["channels"].each_with_index.map do |channel, index|
      consumers = channel["consumers"]
      [
        topology, index, channel["name"], channel["kind"],
        channel["producer"] || "-",
        consumers.empty? ? "-" : consumers.map { |edge| edge["tasklet"] }.join(","),
        channel["sink"] || "-"
      ].join(" | ")
    end
  end

  def setup
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?
  end

  def test_the_hybrid_stream_topology_is_the_reference
    assert_equal golden("hybrid_channel"), emitted("hybrid_channel", HybridChannel.spec)
  end

  def test_the_structured_steps_topology_is_the_reference
    assert_equal golden("commit_block_chain"), emitted("commit_block_chain", CommitBlockChain.spec)
  end
end
