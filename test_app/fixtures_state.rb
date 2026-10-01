# frozen_string_literal: true

# Fixtures whose subject is persisted Cell state: the keyed upsert the bench
# shares, and the counter and resume twins — the same partition as
# test/apps/python_runner/fixtures_state.py and the reference's state.rs.

require "flezha"

module Fixtures
  # The reference parses `i64` and reads an unparsable input as 0.
  AS_INT = ->(value) { Integer(value.to_s, exception: false) || 0 }

  # A kept instance that sleeps after `FLEZHA_FIXTURE_SLEEP_AFTER_MS`, so a
  # test can watch one sleep; the class default when the variable is unset.
  FIXTURE_DURABLE = if (sleep_after = AS_INT.call(ENV.fetch("FLEZHA_FIXTURE_SLEEP_AFTER_MS", ""))).positive?
                      { local: { sleep_after: sleep_after } }
                    else
                      :local
                    end

  # -- bench_keyed_nopersist / bench_keyed / the tier stores -----------------
  #
  # The reference's `TierStoreState` and its upsert fold: `tier_store`,
  # `checkpoint_store` and `bench_checkpoint` are the same keyed upsert under
  # three acknowledgement postures; only `run` differs.
  class KeyedUpsert < Flezha::Tasklet
    def init_state(_cell_key) = { values: {} }
    def apply_change(state, change) = state[:values][change[:key]] = change[:value]

    # Record and emit one `key:value` input under its natural key.
    def upsert(input, ctx)
      key, value = input.to_s.split(":", 2)
      ctx.change({ key: key, value: value })
      ctx.emit("#{key}=#{value}", key: key)
      nil
    end
  end

  class TierStore < KeyedUpsert
    tasklet_name "tier_store_tasklet"

    def run(input, ctx)
      # Dead-letter e2e: a "poison" value fails every attempt, exhausting the
      # retry budget so the tier-on atomic park advances the cell past it.
      raise "poison value" if input.to_s.split(":", 2)[1] == "poison"

      upsert(input, ctx)
    end
  end

  # `tier_store`'s twin, whose interval closes on a `commit:` input.
  class CheckpointStore < KeyedUpsert
    tasklet_name "checkpoint_store_tasklet"

    def run(input, ctx)
      if input.to_s.split(":", 2)[0] == "commit"
        ctx.checkpoint
        return nil
      end

      upsert(input, ctx)
    end
  end

  # `checkpoint_store` driven by a DECLARED cadence rather than an input.
  class BenchCheckpoint < KeyedUpsert
    tasklet_name "bench_checkpoint_tasklet"
    def run(input, ctx) = upsert(input, ctx)
  end

  # The same keyed upsert under both durability postures, so the round-trip
  # bench can isolate the cost of per-Cell persistence.
  class KeyedUpsertOperator < Flezha::Operator
    channels do |c|
      c.input("input", codec: :text).on_full(:drop_nothing)
       .into(TierStore).keyed
       .sink("sink")
    end
  end

  # Per-cell persistence off: this is the heap-only posture the bench
  # isolates against the durable one.
  class BenchKeyedNoPersistOperator < KeyedUpsertOperator
    factory_id "bench_keyed_nopersist"
    durable false
  end

  class BenchKeyedOperator < KeyedUpsertOperator
    factory_id "bench_keyed"
  end

  # The effectively-once reference: a keyed upsert under the tier. A 300 ms
  # backoff base keeps the min-watermark freeze observable in the DLQ e2e.
  # Declared DropNothing: a durable-input-log operator that shed at its
  # mailbox would hold the min-watermark on the shed offset forever while the
  # log behind it kept growing.
  class TierStoreOperator < Flezha::Operator
    factory_id "tier_store"
    run flow: { exactly: :effectively_once }
    channels do |c|
      c.input("input", codec: :text).on_full(:drop_nothing).retry_backoff_ms(300)
       .into(TierStore).keyed
       .sink("sink")
    end
  end

  # `tier_store` kept `durable: remote`: an input is acknowledged only once the
  # durability backend holds the records it produced.
  class RemoteTierStoreOperator < TierStoreOperator
    factory_id "remote_tier_store"
    durable :remote
  end

  # -- tier_chain ------------------------------------------------------------
  #
  # An effectively-once stream of two tasklets. `tier_relay` counts what it
  # relays and passes each `key:value` on; `tier_tally` counts each key's
  # inputs and emits `key=count`. Until the release env is set the tally fails
  # the value `hold` and keeps retrying it, so a runner killed there has made
  # the relay's record durable and not the tally's.
  class Counting < Flezha::Tasklet
    def init_state(_cell_key) = { value: 0 }
    def apply_change(state, change) = state[:value] += change[:delta]
  end

  class TierRelay < Counting
    tasklet_name "tier_relay"

    def run(input, ctx)
      ctx.change({ delta: 1 })
      ctx.emit(input)
      nil
    end
  end

  class TierTally < Counting
    tasklet_name "tier_tally"

    def run(input, ctx)
      key, _, value = input.to_s.partition(":")
      raise "held until released" if value == "hold" && !ENV.key?(RELEASE_ENV)

      ctx.change({ delta: 1 })
      ctx.emit("#{key}=#{ctx.state[:value]}", key: key)
      nil
    end
  end

  class TierChainOperator < Flezha::Operator
    factory_id "tier_chain"
    run flow: { exactly: :effectively_once }
    channels do |c|
      c.input("input", codec: :text).on_full(:drop_nothing)
       .into(TierRelay)
       .key_by { |value| PREFIX.call(value) }.retry_max(1_000)
       .into(TierTally).keyed
       .sink("sink")
    end
  end

  class CheckpointStoreOperator < Flezha::Operator
    factory_id "checkpoint_store"
    run flow: { exactly: { effectively_once: { ack: :checkpoint } } }
    channels do |c|
      c.input("input", codec: :text).into(CheckpointStore).keyed
       .sink("sink")
    end
  end

  # The same tasklet declared `outputs: :at_checkpoint`: the interval's outputs
  # are held with its changes and ship inside its checkpoint record.
  # `every` is deliberately absent so the test drives the close
  # itself — with `commit:` for the batch case, and with the delay alone for
  # the quiet-Cell case.
  class CheckpointBatchedOperator < Flezha::Operator
    factory_id "checkpoint_batched_store"
    run flow: { exactly: { effectively_once: { ack: { checkpoint: { outputs: :at_checkpoint, max_delay_ms: 1_500 } } } } }
    channels do |c|
      c.input("input", codec: :text).into(CheckpointStore).keyed
       .sink("sink")
    end
  end

  # A positive integer from the environment, or the default.
  ENV_INT = ->(key, default) { (value = ENV.fetch(key, "").to_i).positive? ? value : default }

  # `checkpoint_store` driven by a DECLARED cadence instead of a `commit:`
  # input: acknowledging every `FLEZHA_BENCH_CHECKPOINT_EVERY` inputs (default
  # 500) per Cell. `FLEZHA_BENCH_CHECKPOINT_OUTPUTS=at_checkpoint` (with
  # `FLEZHA_BENCH_CHECKPOINT_DELAY_MS`) additionally holds the interval's
  # outputs until it closes.
  class BenchCheckpointOperator < Flezha::Operator
    factory_id "bench_checkpoint"
    checkpoint = { every: ENV_INT.call("FLEZHA_BENCH_CHECKPOINT_EVERY", 500) }
    if ENV["FLEZHA_BENCH_CHECKPOINT_OUTPUTS"] == "at_checkpoint"
      checkpoint.merge!(outputs: :at_checkpoint,
                        max_delay_ms: ENV_INT.call("FLEZHA_BENCH_CHECKPOINT_DELAY_MS", 50))
    end
    run flow: { exactly: { effectively_once: { ack: { checkpoint: checkpoint } } } }
    channels do |c|
      c.input("input", codec: :text).on_full(:drop_nothing)
       .into(BenchCheckpoint).keyed
       .sink("sink")
    end
  end

  # -- store_input -----------------------------------------------------------

  # The durable-state reference: every input is appended through a change.
  class StoreInput < Flezha::Tasklet
    tasklet_name "store_input_tasklet"
    def init_state(_cell_key) = { binary_inputs: [], text_inputs: [] }
    def apply_change(state, change) = state[:text_inputs] << change[:push_text]

    def run(input, ctx)
      ctx.change({ push_text: input })
      ctx.emit(input)
    end
  end

  class StoreInputOperator < Flezha::Operator
    factory_id "store_input"
    channels do |c|
      c.input("input", codec: :text).into(StoreInput)
       .sink("sink")
    end
  end

  # -- stateful_counter / persistence_off_counter ----------------------------

  class CounterEntry < Flezha::Tasklet
    tasklet_name "counter_entry_tasklet"
    def run(input, ctx) = ctx.emit(AS_INT.call(input))
  end

  class Counter < Flezha::Tasklet
    tasklet_name "counter_tasklet"
    def init_state(_cell_key) = { value: 0 }
    def apply_change(state, change) = state[:value] += change[:delta]

    def run(input, ctx)
      ctx.change({ delta: input })
      nil
    end
  end

  class StatefulCounterOperator < Flezha::Operator
    factory_id "stateful_counter"
    channels do |c|
      c.input("input", codec: :text).into(CounterEntry).named("ticks").into(Counter)
    end
  end

  # The same topology with per-Cell persistence off, under the reference's
  # node names: the toggle is a class property, not a different computation.
  class PersistenceOffCounterOperator < Flezha::Operator
    factory_id "persistence_off_counter"
    durable false
    channels do |c|
      c.input("input", codec: :text)
       .into(CounterEntry, "persistence_off_entry_tasklet").named("ticks")
       .into(Counter, "persistence_off_counter_tasklet")
    end
  end

  # -- sleeping_counter ------------------------------------------------------
  #
  # A flow operator that sleeps when idle: `key:delta` inputs fold into one running
  # total per key, and every input emits `key=total` on the `totals` sink. The
  # sleep e2e reads the total after a wake to prove the Cell came back with the
  # state it slept with.

  class SleepingCounter < Flezha::Tasklet
    tasklet_name "sleeping_counter_tasklet"
    def init_state(_cell_key) = { value: 0 }
    def apply_change(state, change) = state[:value] += change[:delta]

    def run(input, ctx)
      key, _, delta = input.to_s.partition(":")
      ctx.change({ delta: AS_INT.call(delta) })
      ctx.emit("#{key}=#{ctx.state[:value]}")
    end
  end

  class SleepingCounterOperator < Flezha::Operator
    factory_id "sleeping_counter"
    durable FIXTURE_DURABLE
    channels do |c|
      c.input("input", codec: :text).key_by { |value| PREFIX.call(value) }
       .into(SleepingCounter).sink("totals")
    end
  end

  # -- coalescing_counter ----------------------------------------------------

  # Wire format: `<interval>:<ts>:<delta>`; a missing or unparsable part reads 0.
  class TickEntry < Flezha::Tasklet
    tasklet_name "tick_entry_tasklet"

    def run(input, ctx)
      interval, ts, delta = input.to_s.split(":", 3)
      ctx.emit({ interval: AS_INT.call(interval), ts: AS_INT.call(ts), delta: AS_INT.call(delta) })
    end
  end

  # The reference coalesces this change on `(interval, ts)` through a `Change`
  # trait method. The SDK carries no coalesce-key seam, so the change ships
  # uncoalesced — a throughput property, not a semantic one: the fold is
  # idempotent per `(interval, ts)` either way, so replay produces the same
  # state.
  class Tick < Flezha::Tasklet
    tasklet_name "tick_tasklet"
    def init_state(_cell_key) = { last: 0 }
    def apply_change(state, change) = state[:last] = change[:delta]

    def run(input, ctx)
      ctx.change(input.dup)
      nil
    end
  end

  class CoalescingCounterOperator < Flezha::Operator
    factory_id "coalescing_counter"
    channels do |c|
      c.input("input", codec: :text).into(TickEntry).named("ticks").into(Tick)
    end
  end

  # -- resume_probe ----------------------------------------------------------

  # Emits the running total, so a resume is observable on the wire. Default +
  # change replay rebuilds the total on restore; no snapshot fast path needed.
  class ResumeProbe < Flezha::Tasklet
    tasklet_name "resume_probe_tasklet"
    def init_state(_cell_key) = { total: 0 }
    def apply_change(state, change) = state[:total] += change[:add]

    def run(input, ctx)
      delta = AS_INT.call(input)
      total = ctx.state[:total] + delta
      ctx.change({ add: delta })
      ctx.emit(total.to_s)
    end
  end

  class ResumeProbeOperator < Flezha::Operator
    factory_id "resume_probe"
    channels do |c|
      c.input("input", codec: :text).into(ResumeProbe)
       .sink("total")
    end
  end

  # -- schema_evolution_v1 / schema_evolution_v2 -----------------------------
  #
  # The same class either side of a deploy that edits its own state type. Both
  # halves declare the SAME tasklet name, so an operator started under v1
  # and restarted under v2 restores from the very WAL v1 wrote — which is what
  # a deploy does, with the two binaries collapsed into one fixture file.
  #
  # v1: `{total}`. v2: `{total, currency}` at schema version 2, reading down to
  # 1 through a declared migration. Output is `total:currency` so the e2e can
  # read the migrated field off the wire.

  class EvolvingCartV1 < Flezha::Tasklet
    tasklet_name "evolving_cart"
    def init_state(_cell_key) = { total: 0 }
    def apply_change(state, change) = state[:total] += change[:add]

    def run(input, ctx)
      delta = AS_INT.call(input)
      total = ctx.state[:total] + delta
      ctx.change({ add: delta })
      ctx.emit("#{total}:")
    end
  end

  class EvolvingCartV2 < Flezha::Tasklet
    tasklet_name "evolving_cart"
    schema_version 2
    min_readable_schema_version 1

    def migrate_state(from_version, bytes)
      raise "no migration from #{from_version}" unless from_version == 1

      old = Flezha::Codec.decode(bytes) || {}
      { total: old.fetch(:total, 0), currency: "EUR" }
    end

    def migrate_change(from_version, bytes)
      raise "no migration from #{from_version}" unless from_version == 1

      old = Flezha::Codec.decode(bytes) || {}
      { add: old.fetch(:add, 0), note: "migrated" }
    end

    def init_state(_cell_key) = { total: 0, currency: "" }
    def apply_change(state, change) = state[:total] += change[:add]

    def run(input, ctx)
      delta = AS_INT.call(input)
      total = ctx.state[:total] + delta
      currency = ctx.state[:currency].to_s.empty? ? "EUR" : ctx.state[:currency]
      ctx.change({ add: delta, note: "live" })
      ctx.emit("#{total}:#{currency}")
    end
  end

  class SchemaEvolutionV1Operator < Flezha::Operator
    factory_id "schema_evolution_v1"
    channels do |c|
      c.input("input", codec: :text).into(EvolvingCartV1)
       .sink("total")
    end
  end

  class SchemaEvolutionV2Operator < Flezha::Operator
    factory_id "schema_evolution_v2"
    channels do |c|
      c.input("input", codec: :text).into(EvolvingCartV2)
       .sink("total")
    end
  end

  # The persisted-state family, in declaration order.
  STATE = [
    BenchKeyedNoPersistOperator, StoreInputOperator, StatefulCounterOperator,
    PersistenceOffCounterOperator, SleepingCounterOperator, CoalescingCounterOperator,
    ResumeProbeOperator, BenchKeyedOperator, SchemaEvolutionV1Operator,
    SchemaEvolutionV2Operator, TierStoreOperator, RemoteTierStoreOperator, TierChainOperator,
    CheckpointStoreOperator,
    CheckpointBatchedOperator, BenchCheckpointOperator
  ].freeze
end
