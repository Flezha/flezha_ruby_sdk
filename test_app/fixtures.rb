# frozen_string_literal: true

# Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
# under the same factory ids, tasklet names, channel names and output bytes.
#
# Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
# reference: every entry names the text codec, and a sink naming none sends a
# String as its own bytes, as the reference's does. An inter-tasklet hop
# between two Ruby tasklets carries MessagePack, which no one else reads.

require "flezha"

# The persisted-state family is registered from its own file.
require_relative "fixtures_state"

module Fixtures
  # The key convention every keyed fixture shares: text before the first ':'.
  PREFIX = ->(input) { input.to_s.split(":").first.to_s }

  # -- do_nothing / bench_echo ---------------------------------------------

  class DoNothing < Flezha::Tasklet
    tasklet_name "do_nothing_tasklet"
    def run(input, _ctx) = input
  end

  class DoNothingOperator < Flezha::UnboundedOperator
    factory_id "do_nothing"
    channels do |c|
      c.input("input", codec: :text).into(DoNothing)
       .sink("echo")
    end
  end

  # Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
  # merge-blocking bench factory and must not change; this keeps the round-trip
  # bench's back-pressure posture independent of it.
  class BenchEchoOperator < Flezha::UnboundedOperator
    factory_id "bench_echo"
    channels do |c|
      c.input("input", codec: :text).on_full(:drop_nothing)
       .into(DoNothing)
       .sink("echo")
    end
  end

  # -- compare_filter -------------------------------------------------------

  # State seeded by the controller's init values and then only read: there is
  # nothing to journal, so the tasklet declares no change fold.
  class CompareFilter < Flezha::Tasklet
    tasklet_name "compare_filter_tasklet"
    def initial_state(_cell_key) = { comparable: "default", inverted: false }
    def run(input, ctx) = ctx.state[:comparable] == input ? input : nil
  end

  class CompareFilterOperator < Flezha::UnboundedOperator
    factory_id "compare_filter"
    channels do |c|
      c.input("input", codec: :text).into(CompareFilter)
       .sink("sink")
    end
  end

  # -- static_cell / sugar_static_cell / mixed_operator ---------------

  STATIC_KEYS = %w[a b c d].freeze

  # The static fan-out every static fixture shares.
  module StaticFanOut
    def cells_for(input)
      first = input.to_s[0]
      [STATIC_KEYS.include?(first) ? first : "d"]
    end
  end

  class StaticEntry < Flezha::Tasklet
    include StaticFanOut
    tasklet_name "static_entry_tasklet"
    cell_keys(*STATIC_KEYS)
    def initial_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      input
    end
  end

  class MacroStaticEntry < StaticEntry
    tasklet_name "macro_static_entry_tasklet"
  end

  class PassThrough < Flezha::Tasklet
    def run(input, _ctx) = input
  end

  class StaticDownstream < PassThrough
    tasklet_name "static_downstream_tasklet"
  end

  class MacroStaticDownstream < PassThrough
    tasklet_name "macro_static_downstream_tasklet"
  end

  class StaticCellOperator < Flezha::UnboundedOperator
    factory_id "static_cell"
    channels do |c|
      c.input("input", codec: :text).into(StaticEntry).named("hop")
       .into(StaticDownstream)
       .sink("kafka")
    end
  end

  # The reference pipeline sugar leaves the hop unnamed and lets the sink name
  # the producing channel.
  class SugarStaticCellOperator < Flezha::UnboundedOperator
    factory_id "sugar_static_cell"
    channels do |c|
      c.input("input", codec: :text).into(MacroStaticEntry)
       .into(MacroStaticDownstream)
       .sink("test_kafka")
    end
  end

  class MixedMacroEntry < PassThrough
    tasklet_name "mixed_macro_entry_tasklet"
  end

  class MixedHandDownstream < PassThrough
    tasklet_name "mixed_hand_downstream"
  end

  class MixedOperator < Flezha::UnboundedOperator
    factory_id "mixed_operator"
    channels do |c|
      c.input("input", codec: :text).into(MixedMacroEntry).named("hop")
       .into(MixedHandDownstream)
       .sink("kafka")
    end
  end

  # -- the §5 short forms --------------------------------------------------
  #
  # One operator class each, carrying its one tasklet's `run` body instead
  # of `channels`; `runner.rb` registers the class itself. No tasklet stands
  # beside them.

  class SugarDoNothing < Flezha::UnboundedOperator
    factory_id "sugar_do_nothing"
    sink "sugar_do_nothing_echo"
    codec :text
    def run(input, _ctx) = input
  end

  class SugarStoreInput < Flezha::UnboundedOperator
    factory_id "sugar_store_input"
    sink "sugar_store_sink"
    codec :text
    def initial_state(_cell_key) = { text_inputs: [] }
    def apply_change(state, change) = state[:text_inputs] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      input
    end
  end

  class SugarNamedInput < Flezha::UnboundedOperator
    factory_id "sugar_named_input"
    sink "sugar_named_input_sink"
    input "prices"
    codec :text
    def run(input, _ctx) = input
  end

  class SugarBounded < Flezha::BoundedOperator
    factory_id "sugar_bounded"
    sink "sugar_bounded_placed"
    input "order"
    codec :text
    def run(input, _ctx) = input
  end

  class SugarBoundedPerInput < Flezha::BoundedOperator
    factory_id "sugar_bounded_per_input"
    sink "sugar_bounded_per_input_out"
    codec :text
    loop_mode :per_input
    def run(input, _ctx) = input
  end

  # No sink: what it was sent is proven by its state.
  class SugarStateOnly < Flezha::UnboundedOperator
    factory_id "sugar_state_only"
    input "ticks"
    codec :text
    def initial_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      nil
    end
  end

  # -- hybrid_channel ------------------------------------------------------
  #
  # One emit reaching two destinations: an inter-tasklet consumer and a
  # plugin wire with its own serializer. The serializer refuses a sentinel
  # value, so a user can observe end to end that a refusal is isolated to
  # the emit rather than failing the item.

  HYBRID_SENTINEL = 0xdeadbeef

  class HybridProducer < Flezha::Tasklet
    tasklet_name "hybrid_producer_tasklet"
    def run(input, ctx)
      parsed = Integer(input.to_s, exception: false) || 0
      value = parsed.between?(0, 0xffffffff) ? parsed : 0
      begin
        ctx.emit(value)
      rescue StandardError => e
        warn("hybrid_producer_tasklet: serializer refused an emit: #{e.message}")
      end
      nil
    end
  end

  class HybridConsumer < Flezha::Tasklet
    tasklet_name "hybrid_consumer_tasklet"
    def run(input, _ctx)
      puts("hybrid_consumer_tasklet: received #{input}")
      nil
    end
  end

  HYBRID_ENCODE = lambda do |value|
    if value == HYBRID_SENTINEL
      raise "hybrid_indicators: serializer rejected sentinel 0xDEADBEEF"
    end

    [value].pack("N")
  end

  class HybridChannelOperator < Flezha::UnboundedOperator
    factory_id "hybrid_channel"
    channels do |c|
      c.input("input", codec: :text).into(HybridProducer).branch(
        # The consumer edge first, so a refused plugin serialization leaves the
        # in-process fan-out already recorded — the reference's isolation.
        ->(edge) { edge.named("hybrid_indicator").into(HybridConsumer) },
        ->(edge) { edge.sink(encode: HYBRID_ENCODE) }
      )
    end
  end

  # -- merged_input ----------------------------------------------------------
  #
  # Two entry channels merged into one tasklet. Each source's mapping tags
  # the value with the channel it came from, the merge keys on the symbol the
  # mapping read, and the emitted line is `<source>:<payload>`.

  MERGE_TAG = lambda do |source|
    ->(payload) { { source: source, symbol: PREFIX.call(payload), payload: payload } }
  end

  class MergeLabel < Flezha::Tasklet
    tasklet_name "merge_label_tasklet"
    dynamic_cells
    def run(input, _ctx) = "#{input[:source]}:#{input[:payload]}"
  end

  class MergedInputOperator < Flezha::UnboundedOperator
    factory_id "merged_input"
    channels do |c|
      c.merge
       .from(c.input("ticks", codec: :text), &MERGE_TAG.call("ticks"))
       .from(c.input("quotes", codec: :text), &MERGE_TAG.call("quotes"))
       .key_by { |value| value[:symbol] }
       .into(MergeLabel).keyed
       .sink("merged")
    end
  end

  # -- transform_chain -------------------------------------------------------
  #
  # Every value transform on one edge: `drop` is filtered out, a comma list
  # fans out into one value per element, each is upper-cased and keyed on its
  # prefix.

  class TransformLabel < PassThrough
    tasklet_name "transform_label_tasklet"
    dynamic_cells
  end

  class TransformChainOperator < Flezha::UnboundedOperator
    factory_id "transform_chain"
    channels do |c|
      c.input("trades", codec: :text)
       .filter { |trade| trade != "drop" }
       .flat_map { |trade| trade.split(",") }
       .map(&:upcase)
       .key_by { |trade| PREFIX.call(trade) }
       .into(TransformLabel).keyed
       .sink("transformed")
    end
  end

  # -- cell_stress -------------------------------------------------------

  STRESS_KEYS = %w[p0 p1 p2 p3 p4 p5 p6 p7].freeze

  class StressA < Flezha::Tasklet
    tasklet_name "stress_a"
    cell_keys(*STRESS_KEYS)
    def cells_for(_input) = STRESS_KEYS
    def run(input, _ctx) = input
  end

  class StressToP0 < Flezha::Tasklet
    cell_keys(*STRESS_KEYS)
    def cells_for(_input) = ["p0"]
    def run(input, _ctx) = input
  end

  class StressB < StressToP0
    tasklet_name "stress_b"
  end

  class StressC < StressToP0
    tasklet_name "stress_c"
  end

  class StressD < StressToP0
    tasklet_name "stress_d"
  end

  class CellStressOperator < Flezha::UnboundedOperator
    factory_id "cell_stress"
    channels do |c|
      c.input("input", codec: :text).into(StressA).named("a_to_b")
       .into(StressB).named("b_to_c")
       .into(StressC).named("c_to_d")
       .into(StressD)
       .sink("echo")
    end
  end

  # -- poison_probe / wedge_probe ------------------------------------------

  class PoisonProbe < Flezha::Tasklet
    tasklet_name "poison_probe_tasklet"
    ANTIDOTE = { taken: false }

    def run(input, _ctx)
      ANTIDOTE[:taken] = true if input == "antidote"
      raise "poison payload" if input == "poison" && !ANTIDOTE[:taken]

      "ok:#{input}"
    end
  end

  class PoisonProbeOperator < Flezha::UnboundedOperator
    factory_id "poison_probe"
    channels do |c|
      c.input("input", codec: :text).retry_backoff_ms(25)
       .into(PoisonProbe)
       .sink("out")
    end
  end

  # The `pool_wedge` fixture: the two ways a runner stops doing work, on one
  # operator.
  #
  #   "wedge" — blocks the dispatch inside user code forever, so the SDK's
  #     dispatch counter stops advancing while the heartbeat keeps reporting
  #     the runner alive: connected, heartbeating, processing nothing.
  #   "abort" — ends its activation as violently as this host allows, WITHOUT
  #     emitting, once per process. Ruby has no way for user code to kill a
  #     worker outright — the dispatcher rescues per item and the raise rides
  #     the retry ladder — so what the scenario asserts is that the Cell is
  #     served again and the aborting item does not produce twice.
  class WedgeProbe < Flezha::Tasklet
    tasklet_name "wedge_probe_tasklet"
    ABORTED = { done: false }

    def run(input, _ctx)
      loop { sleep(3600) } if input == "wedge"

      if input == "abort" && !ABORTED[:done]
        ABORTED[:done] = true
        raise "wedge_probe: killing the worker holding this activation"
      end

      "ok:#{input}"
    end
  end

  class WedgeProbeOperator < Flezha::UnboundedOperator
    factory_id "wedge_probe"
    channels do |c|
      c.input("input", codec: :text).into(WedgeProbe)
       .sink("out")
    end
  end

  # -- candle ---------------------------------------------------------------
  #
  # Late-input correction is about fold ORDER, and the only way to prove that
  # is to look at the order. Send 100, 300, then the late 200. Without
  # correction the fold order is [100, 300, 200] and the candle is permanently
  # wrong. With it, the engine rewinds behind 200, replays the retained input
  # log sorted by event time, and the fold order becomes [100, 200, 300].

  class Candle < Flezha::Tasklet
    tasklet_name "candle_tasklet"
    clock :event_time
    # Event times in fold order: the assertion surface.
    def initial_state(_cell_key) = { folded: [] }
    def apply_change(state, change) = state[:folded] << change[:fold]
    def event_time(input) = AS_INT.call(input.to_s.split(":", 2).first)

    def run(input, ctx)
      # `ctx.now` IS the extracted event time under an event-time clock.
      event_time = ctx.now
      ctx.change({ fold: event_time })
      # One row per candle bucket, so a corrected replay overwrites by key
      # rather than appending a second row.
      bucket = event_time / 1_000
      ctx.emit("#{bucket}=#{input}", key: "candle/#{bucket}")
      nil
    end
  end

  class CandleOperator < Flezha::UnboundedOperator
    factory_id "candle"
    channels do |c|
      c.effectively_once
      c.correction_window(600_000)
      c.input("input", codec: :text).into(Candle).keyed
       .sink("sink")
    end
  end

  # -- session_timeout ------------------------------------------------------
  #
  # The worked session-timeout shape: each input pushes the idle deadline out,
  # and the firing closes the session. Single-cell so the e2e's crash-resume
  # half exercises a durable Cell. The timeout is read from
  # `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).

  class SessionTimeout < Flezha::Tasklet
    tasklet_name "session_timeout_tasklet"
    def initial_state(_cell_key) = { touched: [], closed_at: [] }

    def apply_change(state, change)
      if change.key?(:touched)
        state[:touched] << change[:touched]
      else
        state[:closed_at] << change[:closed]
      end
    end

    def run(input, ctx)
      ctx.change({ touched: input })
      # A processing-time timer, because the answer to "nothing has arrived"
      # cannot be an event-time one: nothing would advance the clock.
      timeout = ENV.fetch("TIMER_TEST_TIMEOUT_MS", "").to_i
      ctx.timer_after(timeout.positive? ? timeout : 1000)
      nil
    end

    def on_timer(firing, ctx)
      fired_at = firing[:scheduled_at]
      ctx.change({ closed: fired_at })
      ctx.emit("closed@#{fired_at}")
    end
  end

  class SessionTimeoutOperator < Flezha::UnboundedOperator
    factory_id "session_timeout"
    channels do |c|
      c.input("input", codec: :text).into(SessionTimeout)
       .sink("kafka")
    end
  end

  # -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} --------------

  class BoundedMailbox < Flezha::Tasklet
    mailbox_capacity 4
    def initial_state(_cell_key) = { processed: 0 }
    def apply_change(state, _change) = state[:processed] += 1

    def run(input, ctx)
      ctx.change({ processed: true })
      input
    end
  end

  # One operator per overflow policy, over the same bounded tasklet. The
  # capacity and the fold come from the base class: a subclass inherits every
  # declaration its parent made.
  BOUNDED_MAILBOX = %w[drop_nothing drop_newest drop_oldest].map do |policy|
    tasklet = Class.new(BoundedMailbox) { tasklet_name "#{policy}_test_tasklet" }

    Class.new(Flezha::UnboundedOperator) do
      factory_id "bounded_mailbox_#{policy}"
      channels do |c|
        c.input("#{policy}_input", codec: :text).on_full(policy.to_sym)
         .into(tasklet)
         .sink("#{policy}_echo")
      end
    end
  end.freeze

  # -- partition_relay -----------------------------------------------------
  #
  # The partitioned-operators upstream: keyed on the wiring so it can be
  # partitioned at all, and emitting keyed so a partitioned forward target can
  # resolve the owning member. The plugin channel is named `input` because a
  # forwarded frame keeps its producing channel's name, and that is what
  # selects the receiving operator's entry channel — so the entry here is `in`.

  class PartitionRelay < Flezha::Tasklet
    tasklet_name "partition_relay_tasklet"
    dynamic_cells
    def initial_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      ctx.emit(input, key: PREFIX.call(input))
      nil
    end
  end

  class PartitionRelayOperator < Flezha::UnboundedOperator
    factory_id "partition_relay"
    channels do |c|
      c.input("in", codec: :text).key_by { |value| PREFIX.call(value) }
       .into(PartitionRelay).keyed
       .sink("input")
    end
  end

  # -- dynamic_cell ------------------------------------------------------

  class DynamicCell < Flezha::Tasklet
    tasklet_name "dynamic_cell_tasklet"
    dynamic_cells idle_ttl_ms: (ENV["DYN_TEST_TTL_MS"].to_i if ENV["DYN_TEST_TTL_MS"].to_i.positive?)
    def initial_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      input
    end
  end

  class DynamicCellOperator < Flezha::UnboundedOperator
    factory_id "dynamic_cell"
    channels do |c|
      c.input("input", codec: :text).on_full(:drop_nothing)
       # Filtered before keying: a dropped value answers no key, so the core
       # never mints a cell for it.
       .filter { |input| input != "drop" }
       .key_by { |value| PREFIX.call(value) }
       .into(DynamicCell)
       .sink("kafka")
    end
  end

  # -- spawn_requester -----------------------------------------------------

  # On each input, spawns `stateless_loop_bounded` once under a generated id
  # and once under the input, and prints both results: a handle's answer is
  # observable nowhere but in the tasklet that read it.
  class SpawnRequester < Flezha::Tasklet
    tasklet_name "spawn_requester"

    def run(input, ctx)
      handles = {
        "unnamed" => ctx.spawn_bounded("stateless_loop_bounded", input),
        "named" => ctx.spawn_bounded("stateless_loop_bounded", input, id: input)
      }
      handles.each { |label, handle| SpawnRequester.report(label, handle) }
      nil
    end

    # A spawn handle's answer on stdout, as `spawn_result <label> <id> <outcome>`.
    def self.report(label, handle)
      result = handle.wait
      puts("spawn_result #{label} #{result.id} #{result.outcome}")
    rescue Flezha::SpawnError => e
      puts("spawn_result #{label} error #{e.kind}")
    ensure
      $stdout.flush
    end
  end

  class SpawnRequesterOperator < Flezha::UnboundedOperator
    factory_id "spawn_requester"
    channels do |c|
      c.input("in", codec: :text).into(SpawnRequester)
       .sink("out")
    end
  end

  # -- spawn_hold_requester ------------------------------------------------

  # An effectively-once stream whose tasklet spawns `stateless_loop_bounded`
  # with no id, prints the answer as `spawn_result hold <id> <outcome>`, then
  # blocks until the release env is set, so a runner killed there dies with
  # the spawn answered and its input not durably consumed.
  class SpawnHoldRequester < Flezha::Tasklet
    tasklet_name "spawn_hold_requester"

    def run(input, ctx)
      SpawnRequester.report("hold", ctx.spawn_bounded("stateless_loop_bounded", input))
      Fixtures.block_until_released
      nil
    end
  end

  class SpawnHoldRequesterOperator < Flezha::UnboundedOperator
    factory_id "spawn_hold_requester"
    channels do |c|
      c.effectively_once
      c.input("in", codec: :text).on_full(:drop_nothing).into(SpawnHoldRequester).keyed
       .sink("out")
    end
  end

  # -- spawn_once_requester ------------------------------------------------

  # On each input, spawns `typed_once_bounded` once with no id, carrying the
  # input inside a JSON object its `order` entry decodes, and prints the
  # result as `spawn_result once <id> <outcome>`: a spawn's payload is the
  # spawned instance's first message, encoded by the no-codec sink rule.
  class SpawnOnceRequester < Flezha::Tasklet
    tasklet_name "spawn_once_requester"

    def run(input, ctx)
      SpawnRequester.report("once", ctx.spawn_bounded("typed_once_bounded", { "text" => input }))
      nil
    end
  end

  class SpawnOnceRequesterOperator < Flezha::UnboundedOperator
    factory_id "spawn_once_requester"
    channels do |c|
      c.input("in", codec: :text).into(SpawnOnceRequester)
       .sink("out")
    end
  end

  # Every operator this app registers, in declaration order.
  ALL = [
    DoNothingOperator, BenchEchoOperator, CompareFilterOperator,
    StaticCellOperator, SugarStaticCellOperator, MixedOperator,
    HybridChannelOperator,
    MergedInputOperator, TransformChainOperator, CellStressOperator, *STATE,
    CandleOperator, PoisonProbeOperator, WedgeProbeOperator, SessionTimeoutOperator, *BOUNDED_MAILBOX,
    PartitionRelayOperator, DynamicCellOperator, SpawnRequesterOperator,
    SpawnHoldRequesterOperator, SpawnOnceRequesterOperator
  ].freeze

  # The §5 short forms this app registers.
  SHORTHAND = [
    SugarDoNothing, SugarStoreInput, SugarNamedInput, SugarBounded, SugarBoundedPerInput,
    SugarStateOnly
  ].freeze
end
