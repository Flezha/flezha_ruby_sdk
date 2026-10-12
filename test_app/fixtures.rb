# frozen_string_literal: true

# Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
# under the same factory ids, tasklet names, channel names and output bytes.
#
# Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
# reference: every entry names the text codec, and an output naming none sends a
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
    def run(input, ctx) = ctx.emit(input)
  end

  class DoNothingRelay < Flezha::Relay
    factory_id "do_nothing"
    channels do |c|
      c.input("input", codec: :text).into(DoNothing)
       .output("echo")
    end
  end

  # Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
  # merge-blocking bench factory and must not change; this keeps the round-trip
  # bench's back-pressure posture independent of it.
  class BenchEchoRelay < Flezha::Relay
    factory_id "bench_echo"
    channels do |c|
      c.input("input", codec: :text).on_full(:drop_nothing)
       .into(DoNothing)
       .output("echo")
    end
  end

  # -- complete_flow / chained_steps ----------------------------------------

  # A flow class that echoes each input on `complete_flow_out` and ends its
  # instance on `bye`, the echo of `bye` included.
  class CompleteFlow < Flezha::Tasklet
    tasklet_name "complete_flow_tasklet"
    def run(input, ctx)
      ctx.complete(input) if input == "bye"
      ctx.emit(input)
    end
  end

  class CompleteFlowRelay < Flezha::Relay
    factory_id "complete_flow"
    channels do |c|
      c.input("input", codec: :text).into(CompleteFlow).output("complete_flow_out")
    end
  end

  # A durable chained loop: each pass prints `chained_pass <n>` and hands the
  # next pass `n - 1` on its own channel; the pass at zero hands nothing, and
  # the empty batch ends the instance.
  class ChainedCountdown < Flezha::Tasklet
    tasklet_name "chained_countdown"
    def run(input, ctx)
      puts("chained_pass #{input}")
      $stdout.flush
      n = Integer(input)
      ctx.emit((n - 1).to_s) if n.positive?
    end
  end

  class ChainedStepsRelay < Flezha::Relay
    factory_id "chained_steps"
    run steps: { each: :chained }
    channels do |c|
      c.input("in", codec: :text).into(ChainedCountdown).named("carry")
    end
  end

  # -- compare_filter -------------------------------------------------------

  # State seeded by the controller's init values and then only read: there is
  # nothing to journal, so the tasklet declares no change fold.
  class CompareFilter < Flezha::Tasklet
    tasklet_name "compare_filter_tasklet"
    def init_state(_cell_key) = { comparable: "default", inverted: false }

    def run(input, ctx)
      ctx.emit(input) if ctx.state[:comparable] == input
    end
  end

  class CompareFilterRelay < Flezha::Relay
    factory_id "compare_filter"
    channels do |c|
      c.input("input", codec: :text).into(CompareFilter)
       .output("sink")
    end
  end

  # -- static_cell / sugar_static_cell / mixed_relay ---------------

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
    def init_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      ctx.emit(input)
    end
  end

  class MacroStaticEntry < StaticEntry
    tasklet_name "macro_static_entry_tasklet"
  end

  class PassThrough < Flezha::Tasklet
    def run(input, ctx) = ctx.emit(input)
  end

  class StaticDownstream < PassThrough
    tasklet_name "static_downstream_tasklet"
  end

  class MacroStaticDownstream < PassThrough
    tasklet_name "macro_static_downstream_tasklet"
  end

  class StaticCellRelay < Flezha::Relay
    factory_id "static_cell"
    channels do |c|
      c.input("input", codec: :text).into(StaticEntry).named("hop")
       .into(StaticDownstream)
       .output("kafka")
    end
  end

  # The reference pipeline sugar leaves the hop unnamed and lets the output name
  # the producing channel.
  class SugarStaticCellRelay < Flezha::Relay
    factory_id "sugar_static_cell"
    channels do |c|
      c.input("input", codec: :text).into(MacroStaticEntry)
       .into(MacroStaticDownstream)
       .output("test_kafka")
    end
  end

  class MixedMacroEntry < PassThrough
    tasklet_name "mixed_macro_entry_tasklet"
  end

  class MixedHandDownstream < PassThrough
    tasklet_name "mixed_hand_downstream"
  end

  class MixedRelay < Flezha::Relay
    factory_id "mixed_relay"
    channels do |c|
      c.input("input", codec: :text).into(MixedMacroEntry).named("hop")
       .into(MixedHandDownstream)
       .output("kafka")
    end
  end

  # -- the §5 short forms --------------------------------------------------
  #
  # One relay class each, carrying its one tasklet's `run` body instead
  # of `channels`; `runner.rb` registers the class itself. No tasklet stands
  # beside them.

  class SugarDoNothing < Flezha::Relay
    factory_id "sugar_do_nothing"
    output "sugar_do_nothing_echo"
    codec :text
    def run(input, ctx) = ctx.emit(input)
  end

  class SugarStoreInput < Flezha::Relay
    factory_id "sugar_store_input"
    output "sugar_store_sink"
    codec :text
    def init_state(_cell_key) = { text_inputs: [] }
    def apply_change(state, change) = state[:text_inputs] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      ctx.emit(input)
    end
  end

  class SugarNamedInput < Flezha::Relay
    factory_id "sugar_named_input"
    output "sugar_named_input_sink"
    input "prices"
    codec :text
    def run(input, ctx) = ctx.emit(input)
  end

  class SugarSteps < Flezha::Relay
    factory_id "sugar_steps"
    run :steps
    durable FIXTURE_DURABLE
    output "sugar_steps_placed"
    input "order"
    codec :text
    def run(input, ctx) = ctx.emit(input)
  end

  class SugarStepsPerInput < Flezha::Relay
    factory_id "sugar_steps_per_input"
    output "sugar_steps_per_input_out"
    codec :text
    run steps: { each: :per_input }
    durable FIXTURE_DURABLE
    def run(input, ctx) = ctx.emit(input)
  end

  # No output: what it was sent is proven by its state.
  class SugarStateOnly < Flezha::Relay
    factory_id "sugar_state_only"
    input "ticks"
    codec :text
    def init_state(_cell_key) = { seen: [] }
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

  class HybridChannelRelay < Flezha::Relay
    factory_id "hybrid_channel"
    channels do |c|
      c.input("input", codec: :text).into(HybridProducer).branch(
        # The consumer edge first, so a refused plugin serialization leaves the
        # in-process fan-out already recorded — the reference's isolation.
        ->(edge) { edge.named("hybrid_indicator").into(HybridConsumer) },
        ->(edge) { edge.output(encode: HYBRID_ENCODE) }
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
    def run(input, ctx) = ctx.emit("#{input[:source]}:#{input[:payload]}")
  end

  class MergedInputRelay < Flezha::Relay
    factory_id "merged_input"
    channels do |c|
      c.merge
       .from(c.input("ticks", codec: :text), &MERGE_TAG.call("ticks"))
       .from(c.input("quotes", codec: :text), &MERGE_TAG.call("quotes"))
       .key_by { |value| value[:symbol] }
       .into(MergeLabel).keyed
       .output("merged")
    end
  end

  # -- transform_chain -------------------------------------------------------
  #
  # Every value transform on one edge: `drop` is filtered out, a comma list
  # fans out into one value per element, each is upper-cased and keyed on its
  # prefix.

  class TransformLabel < PassThrough
    tasklet_name "transform_label_tasklet"
  end

  class TransformChainRelay < Flezha::Relay
    factory_id "transform_chain"
    channels do |c|
      c.input("trades", codec: :text)
       .filter { |trade| trade != "drop" }
       .flat_map { |trade| trade.split(",") }
       .map(&:upcase)
       .key_by { |trade| PREFIX.call(trade) }
       .into(TransformLabel).keyed
       .output("transformed")
    end
  end

  # -- cell_stress -------------------------------------------------------

  STRESS_KEYS = %w[p0 p1 p2 p3 p4 p5 p6 p7].freeze

  class StressA < Flezha::Tasklet
    tasklet_name "stress_a"
    cell_keys(*STRESS_KEYS)
    def cells_for(_input) = STRESS_KEYS
    def run(input, ctx) = ctx.emit(input)
  end

  class StressToP0 < Flezha::Tasklet
    cell_keys(*STRESS_KEYS)
    def cells_for(_input) = ["p0"]
    def run(input, ctx) = ctx.emit(input)
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

  class CellStressRelay < Flezha::Relay
    factory_id "cell_stress"
    channels do |c|
      c.input("input", codec: :text).into(StressA).named("a_to_b")
       .into(StressB).named("b_to_c")
       .into(StressC).named("c_to_d")
       .into(StressD)
       .output("echo")
    end
  end

  # -- poison_probe / wedge_probe ------------------------------------------

  class PoisonProbe < Flezha::Tasklet
    tasklet_name "poison_probe_tasklet"
    ANTIDOTE = { taken: false }

    def run(input, ctx)
      ANTIDOTE[:taken] = true if input == "antidote"
      raise "poison payload" if input == "poison" && !ANTIDOTE[:taken]

      ctx.emit("ok:#{input}")
    end
  end

  class PoisonProbeRelay < Flezha::Relay
    factory_id "poison_probe"
    channels do |c|
      c.input("input", codec: :text).retry_backoff_ms(25)
       .into(PoisonProbe)
       .output("out")
    end
  end

  # The `pool_wedge` fixture: the two ways a runner stops doing work, on one
  # relay.
  #
  #   "wedge" — blocks the dispatch inside user code forever, so the SDK's
  #     dispatch counter stops advancing while the heartbeat keeps reporting
  #     the runner alive: connected, heartbeating, processing nothing.
  #   "abort" — ends its activation as violently as this host allows, WITHOUT
  #     emitting, once per process. Ruby has no way for user code to kill a
  #     worker outright — the dispatcher rescues per item and the raise rides
  #     the retry ladder — so what the scenario asserts is that the Cell is
  #     served again and the aborting item does not produce twice.
  #   "slow" — emits two seconds late, so the relay can be stopped or moved
  #     while the activation still runs.
  class WedgeProbe < Flezha::Tasklet
    tasklet_name "wedge_probe_tasklet"
    ABORTED = { done: false }

    def run(input, ctx)
      loop { sleep(3600) } if input == "wedge"

      if input == "abort" && !ABORTED[:done]
        ABORTED[:done] = true
        raise "wedge_probe: killing the worker holding this activation"
      end
      sleep(2) if input == "slow"

      ctx.emit("ok:#{input}")
    end
  end

  class WedgeProbeRelay < Flezha::Relay
    factory_id "wedge_probe"
    channels do |c|
      c.input("input", codec: :text).into(WedgeProbe)
       .output("out")
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
    # Event times in fold order: the assertion surface.
    def init_state(_cell_key) = { folded: [] }
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

  class CandleRelay < Flezha::Relay
    factory_id "candle"
    run flow: { exactly: { effectively_once: { correction_ms: 600_000 } } }
    channels do |c|
      c.input("input", codec: :text).into(Candle).keyed
       .output("sink")
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
    def init_state(_cell_key) = { touched: [], closed_at: [] }

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

  class SessionTimeoutRelay < Flezha::Relay
    factory_id "session_timeout"
    channels do |c|
      c.input("input", codec: :text).into(SessionTimeout)
       .output("kafka")
    end
  end

  # -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} --------------

  class BoundedMailbox < Flezha::Tasklet
    mailbox_capacity 4
    def init_state(_cell_key) = { processed: 0 }
    def apply_change(state, _change) = state[:processed] += 1

    def run(input, ctx)
      ctx.change({ processed: true })
      ctx.emit(input)
    end
  end

  # One relay per overflow policy, over the same bounded-mailbox tasklet. The
  # capacity and the fold come from the base class: a subclass inherits every
  # declaration its parent made.
  BOUNDED_MAILBOX = %w[drop_nothing drop_newest drop_oldest].map do |policy|
    tasklet = Class.new(BoundedMailbox) { tasklet_name "#{policy}_test_tasklet" }

    Class.new(Flezha::Relay) do
      factory_id "bounded_mailbox_#{policy}"
      channels do |c|
        c.input("#{policy}_input", codec: :text).on_full(policy.to_sym)
         .into(tasklet)
         .output("#{policy}_echo")
      end
    end
  end.freeze

  # -- partition_pass -----------------------------------------------------
  #
  # The partitioned-relays upstream: keyed on the wiring so it can be
  # partitioned at all, and emitting keyed so a partitioned forward target can
  # resolve the owning member. The plugin channel is named `input` because a
  # forwarded frame keeps its producing channel's name, and that is what
  # selects the receiving relay's entry channel — so the entry here is `in`.

  class PartitionPass < Flezha::Tasklet
    tasklet_name "partition_pass_tasklet"
    def init_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      ctx.emit(input, key: PREFIX.call(input))
      nil
    end
  end

  class PartitionPassRelay < Flezha::Relay
    factory_id "partition_pass"
    channels do |c|
      c.input("in", codec: :text).key_by { |value| PREFIX.call(value) }
       .into(PartitionPass).keyed
       .output("input")
    end
  end

  # -- dynamic_cell ------------------------------------------------------

  class DynamicCell < Flezha::Tasklet
    tasklet_name "dynamic_cell_tasklet"
    # The e2e sets the TTL in milliseconds for every SDK's runner; a Ruby
    # tasklet declares whole seconds, so it rounds up.
    ttl_ms = ENV["DYN_TEST_TTL_MS"].to_i
    idle_ttl_seconds((ttl_ms + 999) / 1000) if ttl_ms.positive?
    def init_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def run(input, ctx)
      ctx.change({ push: input })
      ctx.emit(input)
    end
  end

  class DynamicCellRelay < Flezha::Relay
    factory_id "dynamic_cell"
    channels do |c|
      c.input("input", codec: :text).on_full(:drop_nothing)
       # Filtered before keying: a dropped value answers no key, so the core
       # never mints a cell for it.
       .filter { |input| input != "drop" }
       .key_by { |value| PREFIX.call(value) }
       .into(DynamicCell)
       .output("kafka")
    end
  end

  # -- spawn_requester -----------------------------------------------------

  # On each input, spawns `stateless_loop_steps` once under a generated id
  # and once under the input, and prints both results: a handle's answer is
  # observable nowhere but in the tasklet that read it.
  class SpawnRequester < Flezha::Tasklet
    tasklet_name "spawn_requester"

    def run(input, ctx)
      handles = {
        "unnamed" => ctx.spawn_steps("stateless_loop_steps", input),
        "named" => ctx.spawn_steps("stateless_loop_steps", input, id: input)
      }
      handles.each { |label, handle| SpawnRequester.report(label, handle) }
      nil
    end

    # A spawn handle's answer on stdout, as `spawn_result <label> <id> <outcome>`.
    def self.report(label, handle)
      result = handle.wait
      puts("spawn_result #{label} #{result.id} #{result.outcome}")
    rescue Flezha::Error => e
      puts("spawn_result #{label} error #{e.code}")
    ensure
      $stdout.flush
    end
  end

  class SpawnRequesterRelay < Flezha::Relay
    factory_id "spawn_requester"
    channels do |c|
      c.input("in", codec: :text).into(SpawnRequester)
       .output("out")
    end
  end

  # -- spawn_hold_requester ------------------------------------------------

  # An effectively-once stream whose tasklet spawns `stateless_loop_steps`
  # with no id, prints the answer as `spawn_result hold <id> <outcome>`, then
  # blocks until the release env is set, so a runner killed there dies with
  # the spawn answered and its input not durably consumed.
  class SpawnHoldRequester < Flezha::Tasklet
    tasklet_name "spawn_hold_requester"

    def run(input, ctx)
      SpawnRequester.report("hold", ctx.spawn_steps("stateless_loop_steps", input))
      Fixtures.block_until_released
      nil
    end
  end

  class SpawnHoldRequesterRelay < Flezha::Relay
    factory_id "spawn_hold_requester"
    run flow: { exactly: :effectively_once }
    channels do |c|
      c.input("in", codec: :text).on_full(:drop_nothing).into(SpawnHoldRequester).keyed
       .output("out")
    end
  end

  # -- spawn_once_requester ------------------------------------------------

  # On each input, spawns `typed_once_steps` once with no id, carrying the
  # input inside a JSON object its `order` entry decodes, and prints the
  # result as `spawn_result once <id> <outcome>`: a spawn's payload is the
  # spawned instance's first message, encoded by the no-codec output rule.
  class SpawnOnceRequester < Flezha::Tasklet
    tasklet_name "spawn_once_requester"

    def run(input, ctx)
      SpawnRequester.report("once", ctx.spawn_steps("typed_once_steps", { "text" => input }))
      nil
    end
  end

  class SpawnOnceRequesterRelay < Flezha::Relay
    factory_id "spawn_once_requester"
    channels do |c|
      c.input("in", codec: :text).into(SpawnOnceRequester)
       .output("out")
    end
  end

  # -- call_response -------------------------------------------------------

  # Relays an outside caller calls and whose output answers it: from the
  # first tasklet, from a downstream one, and from whichever of two filtered
  # paths the input took. `call_turn` is the steps
  # one, in fixtures_steps.rb.
  class CallEchoTasklet < PassThrough
    tasklet_name "call_echo"
  end

  class CallEcho < Flezha::Relay
    factory_id "call_echo"
    channels { |c| c.input("in", codec: :text).into(CallEchoTasklet).output }
  end

  class CallParse < PassThrough
    tasklet_name "call_parse"
  end

  class CallAnswer < Flezha::Tasklet
    tasklet_name "call_answer"
    def run(input, ctx) = ctx.emit("re:#{input}")
  end

  class CallDownstream < Flezha::Relay
    factory_id "call_downstream"
    channels { |c| c.input("in", codec: :text).into(CallParse).into(CallAnswer).output }
  end

  # An input starting `no` takes the refused path, every other the accepted one.
  class CallAdmit < PassThrough
    tasklet_name "call_admit"
  end

  class CallAccept < Flezha::Tasklet
    tasklet_name "call_accept"
    def run(input, ctx) = ctx.emit("accepted:#{input}")
  end

  class CallRefuse < Flezha::Tasklet
    tasklet_name "call_refuse"
    def run(input, ctx) = ctx.emit("refused:#{input}")
  end

  class CallSplit < Flezha::Relay
    factory_id "call_split"
    channels do |c|
      admitted = c.input("in", codec: :text).into(CallAdmit)
      admitted.filter { |input| !input.start_with?("no") }.into(CallAccept).output
      admitted.filter { |input| input.start_with?("no") }.into(CallRefuse).output
    end
  end

  # Every relay this app registers, in declaration order.
  ALL = [
    DoNothingRelay, BenchEchoRelay, CompareFilterRelay,
    CompleteFlowRelay, ChainedStepsRelay,
    StaticCellRelay, SugarStaticCellRelay, MixedRelay,
    HybridChannelRelay,
    MergedInputRelay, TransformChainRelay, CellStressRelay, *STATE,
    CandleRelay, PoisonProbeRelay, WedgeProbeRelay, SessionTimeoutRelay, *BOUNDED_MAILBOX,
    PartitionPassRelay, DynamicCellRelay, SpawnRequesterRelay,
    SpawnHoldRequesterRelay, SpawnOnceRequesterRelay,
    CallEcho, CallDownstream, CallSplit
  ].freeze

  # The §5 short forms this app registers.
  SHORTHAND = [
    SugarDoNothing, SugarStoreInput, SugarNamedInput, SugarSteps, SugarStepsPerInput,
    SugarStateOnly
  ].freeze
end
