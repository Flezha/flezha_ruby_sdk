# frozen_string_literal: true

require "json"
require "securerandom"

require_relative "builder"
require_relative "codec"
require_relative "dispatch"
require_relative "state"

module Flezha
  # Driving one tasklet without a controller, for the author's own tests.
  #
  # The harness is not a second implementation of the dispatcher: it drives
  # `Dispatch.invoke` — the same function every activation kind that reaches a
  # handler goes through — over a one-tasklet `RegisteredClass`, and
  # `replay_equals` folds through `State.fold_restore`, the function a real
  # restore calls. So what it proves is what the runner does, rather than
  # something that agrees with it.
  module Testing
    # The recorded change log and the live state disagree: the harness's own
    # assertion about the tasklet under test, not a refusal.
    class ReplayMismatch < StandardError
      def initialize(tasklet, live, replayed)
        super(
          "#{tasklet}: replaying the recorded change log against a fresh initial state " \
          "produced #{replayed.inspect}, but the live state is #{live.inspect}. Either a " \
          "handler mutated the state without recording a change, or apply_change is not a " \
          "pure fold — both are lost on restart and reported nowhere until then."
        )
      end
    end

    # A replayed input made an outside call its live run kept through
    # `ctx.effect` some other way: its block ran again, or the kept result was
    # never asked for.
    class EffectReplayMismatch < StandardError
      attr_reader :name

      def initialize(tasklet, name, what)
        @name = name
        super(
          "#{tasklet}: on the replay, effect '#{name}' #{what}. A re-run reads an outside " \
          "call's result only through ctx.effect, with the same name in the same order."
        )
      end
    end

    # The longest effect name, in bytes, as the core refuses it.
    MAX_EFFECT_NAME = 255

    # One input's effect results, by `[name, ordinal]`, and the invocation's
    # count of each name and the call whose block is running.
    Effects = Struct.new(:kept, :replaying, :ordinals, :running)
    private_constant :Effects

    # What one invocation produced.
    Outcome = Struct.new(:outputs, :changes, :timers, keyword_init: true)

    # The core bridge a `Ctx` asks, answered as the core would for one
    # tasklet with no controller behind it.
    class FakeCore
      BASE32 = [*"a".."z", *"2".."7"].freeze

      # Answers by handle, until the invocation that made them ends, as the
      # core releases them.
      attr_reader :spawns

      def initialize(tasklet)
        @tasklet = tasklet
        @spawns = {}
        @spawn_count = 0
        # Each live input's effect results, in order, and how many replays ran.
        @kept = []
        @replays = 0
        @effects = Effects.new({}, false, {}, nil)
      end

      # Serve the next input: a live one keeps its results, the n-th replay
      # reads what the n-th live input kept.
      def begin_input(replay)
        if replay
          @effects = Effects.new(@kept[@replays] || {}, true, {}, nil)
          @replays += 1
        else
          @effects = Effects.new({}, false, {}, nil)
          @kept << @effects.kept
        end
      end

      # A replay must ask for every result its live input kept.
      def finish_input
        unasked = @effects.kept.each_key.find { |name, ordinal| ordinal >= @effects.ordinals.fetch(name, 0) }
        raise EffectReplayMismatch.new(@tasklet, unasked.first, "was never asked for") if @effects.replaying && unasked
      end

      # A harness has no controller, so a spawn resolves at once to what one
      # would have answered: spawned under the given id or a generated-looking
      # one, or skipped on a replay.
      def spawn_request(_token, factory_id, id, _payload, _if_running, replay, _offset)
        id = generated_id(factory_id) if id.empty? && !replay
        outcome = replay ? "skipped_on_replay" : "spawned"
        handle = @spawn_count += 1
        @spawns[handle] = JSON.generate({ "id" => id, "outcome" => outcome })
        handle
      end

      def spawn_result(handle, _timeout_ms)
        @spawns.fetch(handle) { raise Error.new("runtime.spawn_handle_released", handle: handle) }
      end

      # No controller and no listener, so the part, already encoded, goes
      # nowhere.
      def part(_token, _item, _payload) = nil

      # The core's answer to a begun `ctx.effect`, over this input's results:
      # the kept one, or run the block (on a live input only).
      def effect_begin(_token, _item, name)
        raise Error.new("effect.bad_name", name: name.inspect) if name.empty? || name.bytesize > MAX_EFFECT_NAME

        effects = @effects
        raise Error.new("effect.nested", name: name) if effects.running

        ordinal = effects.ordinals.fetch(name, 0)
        effects.ordinals[name] = ordinal + 1
        key = [name, ordinal]
        return [:kept, effects.kept[key]] if effects.kept.key?(key)
        raise EffectReplayMismatch.new(@tasklet, name, "ran its block") if effects.replaying

        effects.running = key
        [:run, nil]
      end

      # Keep a live input's result, as the controller does.
      def effect_end(_token, _item, payload)
        effects = @effects
        raise "an end follows a begin that runs" unless effects.running

        effects.kept[effects.running] = payload.b
        effects.running = nil
        nil
      end

      def effect_release(_token, _item)
        @effects.running = nil
      end

      private

      # The controller's shape: the factory id trimmed so the whole id fits the
      # 32-byte limit, then 16 base32 characters.
      def generated_id(factory_id)
        prefix = factory_id.byteslice(0, 15).scrub("").sub(/[-_]+\z/, "")
        "#{prefix}-#{SecureRandom.alphanumeric(16, chars: BASE32)}"
      end
    end
    private_constant :FakeCore

    # A `ctx.effect` keeps its result on a live `feed`; the n-th
    # `feed(..., replay: true)` replays the n-th live input and is served what
    # it kept, and raises `EffectReplayMismatch` when a block runs there or a
    # kept result is never asked for.
    class TaskletTestHarness
      attr_reader :log

      def initialize(tasklet_class, cell_key: nil, relay: "harness")
        @definition = tasklet_class.definition
        @cell_key = cell_key
        # What every invocation's activation names: the cell this harness drives.
        @target = { relay: relay, cell_key: cell_key }.freeze
        # The real channel builder, not a hand-written spec: one entry channel
        # into the tasklet, and one plugin output so `ctx.emit` has somewhere
        # to go. A tasklet under test emits to be observed, not to be routed,
        # and the output names MessagePack so `outputs` reports exactly the value
        # a handler emitted.
        builder = ChannelBuilder.new("<harness>")
        builder.input("in").into(tasklet_class).output("out", codec: :msgpack)
        @node = builder.tasklets.first
        @spec = ClassSpec.new(factory_id: "<harness>", tasklets: builder.tasklets, channels: builder.channels)
        # The dispatcher's own cell store, so the state this harness reports
        # is the state a runner would hold.
        @registered = RegisteredClass.new(@spec)
        @address = RegisteredClass.address(@target, @node)
        # No core runs here, so the harness names the cell's identity itself.
        @registered.keep_identity(@address, "#{relay}/#{@node.name}/#{cell_key}".b)
        @core = FakeCore.new(@definition.name)
        @deps = DispatchDeps.new(registry: nil, bridge: @core)
        @log = []
        @change_seq = 0
      end

      # The cell's live state, as the dispatcher left it.
      def state = @registered.state_of(@node, @address)

      # Run the tasklet against one input; `replay` replays the next live
      # input, in order.
      def feed(input, now: 0, replay: false)
        @core.begin_input(replay)
        outcome = run(now, replay) { |ctx| @definition.instance.run(input, ctx) }
        @core.finish_input
        outcome
      end

      # Fire a per-cell timer. `now` IS the scheduled time, as it is live.
      def fire_timer(kind, scheduled_at)
        run(scheduled_at, false) do |ctx|
          @definition.instance.on_timer({ kind: kind, scheduled_at: scheduled_at }, ctx)
        end
      end

      # Run the declared compensation over recorded forward outputs.
      def compensate(outputs)
        run(0, false) { |ctx| @definition.instance.compensate(outputs, ctx) }
      end

      # Rebuild the state the way a restore does, from a snapshot and a change
      # log — the same call the dispatcher answers `restore_fold` with.
      def restore(snapshot: nil, changes: [])
        @registered.set_state(@address, fold(snapshot, changes))
      end

      # Fold every recorded change into a fresh initial state and require the
      # answer to equal the live one.
      #
      # In this SDK that catches both failures at once: a handler that mutated
      # the state without recording a change, and an `apply_change` that is not
      # a pure fold. The SDKs whose state is guarded can only catch the second.
      def replay_equals
        replayed = fold(nil, @log)
        raise ReplayMismatch.new(@definition.name, state, replayed) unless replayed == state

        true
      end

      private

      # What the body answers is dropped, as the dispatcher drops it.
      def run(now, replay)
        activation = @target.merge(change_seq: @change_seq)
        ctx = begin
          Dispatch.invoke(activation, @registered, @node, @deps, now, replay) { |c| yield(c) }
        ensure
          @core.spawns.clear
        end
        record = ctx.send(:effect_record, nil)
        @log.concat(record[:changes])
        @change_seq += record[:changes].size
        Outcome.new(
          outputs: record[:outputs].map { |o| Codec.decode(o[:payload]) },
          changes: record[:changes].map { |bytes| decode_change(bytes) },
          timers: record[:timers]
        )
      end

      def fold(snapshot, changes)
        State.fold_restore(
          @definition,
          { snapshot: snapshot && { bytes: snapshot }, changes: changes.map { |bytes| { bytes: bytes } } },
          @cell_key, @definition.name
        )
      end

      def decode_change(bytes)
        codec = @definition.change_codec
        codec ? codec.decode(bytes) : Codec.decode(bytes)
      end
    end
  end
end
