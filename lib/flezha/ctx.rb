# frozen_string_literal: true

require "json"

require_relative "builder"
require_relative "codec"
require_relative "errors"
require_relative "identity"

module Flezha
  # The invocation context: every deterministic effect a handler is allowed.
  #
  # The context accumulates one item's effects (recorded changes, outputs,
  # timer operations, the checkpoint flag, a completion request) and the
  # dispatcher turns them into the item effect the core folds. Nothing here
  # reaches the wire on its own; an emit is a value in a list until the
  # completion carries it.
  class Ctx
    # Metadata keys the runtime owns. A handler that sets one is refused.
    RESERVED_PREFIX = "flz."

    attr_reader :changes, :outputs, :timers, :cell_key, :replay

    def initialize(spec:, node:, outgoing:, relay:, cell_key:, change_seq:,
                   now:, replay:, state:, bridge:, token:, offset: nil, item: nil, compensating: false)
      @token = token
      # The index of the work item this invocation runs within its activation,
      # which the core numbers and stamps a part by. A compensation has none.
      @item = item
      # The input-log offset of the item this invocation runs: a spawn carries
      # it so a crash re-run of the same input resolves to the worker the first
      # run spawned.
      @offset = offset
      @compensating = compensating
      @spec = spec
      @node = node
      @outgoing = outgoing
      @relay = relay
      @cell_key = cell_key
      @change_seq = change_seq
      @now = now
      @replay = replay
      @state = state
      @bridge = bridge
      @changes = []
      @outputs = []
      @timers = []
      @checkpointed = false
      @completion = nil
      @ordinal = 0
      @emit_ordinal = 0
      @identity = nil
    end

    # The cell's state. Mutated only through `change`: a field assigned
    # directly is live in this process and gone after a restart.
    attr_reader :state

    # The core-evaluated declared clock for this item. Never ambient time.
    def now = @now

    # A replay-stable identifier, distinct per invocation within the cell.
    def stable_id
      # Lazily, because most invocations never ask and the identity is the one
      # part of the derivation that costs an allocation.
      @identity ||= Identity.cell_identity(@relay, @node.name, @cell_key.to_s)
      id = Identity.stable_id(@identity, @change_seq, @ordinal)
      @ordinal += 1
      id
    end

    # Record one declared change: journaled, then folded into the state.
    def change(change)
      definition = @node.definition
      unless definition.apply_change?
        raise Error.new("runtime.change_without_apply_change", tasklet: @node.name)
      end

      codec = definition.change_codec
      @changes << (codec ? codec.encode(change).b : Codec.encode(change))
      # Fold it live through the same method the restore fold uses, so the live
      # path and the replay path cannot drift. `apply_change` mutates the state
      # in place and its answer is ignored — the same contract Rust's
      # `&mut S` and the other SDKs carry, so a scalar state is declared as a
      # Hash rather than reassigned here.
      definition.fold_change(@state, change)
    end

    # Emit one output value on the tasklet's declared output.
    def emit(value, key: nil, channel: nil, metadata: nil)
      targets = channel ? @outgoing.select { |c| c.name == channel } : @outgoing
      if targets.empty?
        raise Error.new("runtime.channel_not_produced", tasklet: @node.name, channel: channel) if channel

        raise Error.new("runtime.no_output_channel", tasklet: @node.name)
      end

      targets.each { |target| release(target, value, key, metadata) }
      nil
    end

    # Send `value` as a part: a preview of the output this invocation is
    # producing, which leaves the runner now rather than when the handler
    # returns. It previews the tasklet's own output channel and is encoded as
    # an emit of the same value there would be. The relay's listeners
    # receive it before the output; an output never does, and it is never durable
    # or replayed. Parts are numbered from 0 within each attempt, so a retry
    # starts its preview again.
    #
    # It never waits: a part that cannot leave at once (no connection, a local
    # run, a stream that would block, a frame over the size limit) is dropped,
    # and a replayed input sends none. Raises a `Flezha::Error` coded
    # `part.no_output` when the output channel does not end in an output (it only
    # feeds another tasklet, or ends in `call`), and `part.outside_invocation`
    # through a context whose invocation has returned.
    def part(value)
      channel = @outgoing.first
      # A channel with no plugin end has no encoding; the core refuses its part
      # before reading a byte.
      payload = channel&.sink ? channel.sink_encode.call(value).b : "".b
      # A compensation holds no work item, which the core answers as outside
      # any invocation, whatever the index.
      @bridge.part(@token, @item || 0, payload)
      nil
    end

    # Emit onto a named channel — the spelling for a tasklet with several.
    def emit_to(channel, value, key: nil, metadata: nil)
      emit(value, key: key, channel: channel, metadata: metadata)
    end

    # Register a processing-time timer `ms` from `now`. Answers its time.
    # Inside a `compensate` hook each timer call raises `runtime.timers_unavailable`.
    def timer_after(milliseconds)
      # Anchored on `now`, never an ambient read: that is what makes a replayed
      # invocation register the timestamp the live one did.
      at = @now + Integer(milliseconds)
      timer(kind: "processing_time", scheduled_at: at, register: true)
      at
    end

    # Register an event-time timer at an absolute time. Answers it.
    def timer_at_event_time(at)
      at = Integer(at)
      timer(kind: "event_time", scheduled_at: at, register: true)
      at
    end

    # Cancel a registered timer by the value its registration answered.
    def cancel_timer(kind, at)
      timer(kind: kind.to_s, scheduled_at: Integer(at), register: false)
      nil
    end

    # Close the open checkpoint interval (checkpoint ack-granularity classes).
    def checkpoint
      @checkpointed = true
      nil
    end

    # End the instance this tasklet runs in, with `result`.
    #
    # The rest of the iteration still runs and commits; the instance then ends
    # instead of waiting for its next input, a flow instance once every Cell is
    # idle. In a `once` steps relay it sets the result the pass ends with.
    # `result` is encoded as an output naming no codec encodes a value: a String as
    # its bytes and anything else as JSON; nil carries an empty result. A later
    # call in the same invocation replaces an earlier one.
    def complete(result = nil)
      @completion = result.nil? ? "".b : Codec.plain_payload(result, "tasklet '#{@node.name}'").b
      nil
    end

    # Spawn a steps instance of `factory_id` with `payload` as its first
    # message, answering a `SpawnHandle` at once. The payload is encoded as an
    # output naming no codec encodes a value, the rule `complete` applies: a
    # String as its bytes and anything else as JSON, so the instance's `text`
    # or `json` entry channel reads it as it reads a send. With an `id` already
    # running, `if_running: "deliver"` (the default) hands it `payload` instead,
    # and `"error"` resolves to a `Flezha::Error` coded `relay.already_running`. A replayed
    # invocation sends nothing and resolves to `:skipped_on_replay`.
    #
    # Without an `id` the controller generates one and always starts a new
    # instance, so a spawn reissued after a lost or timed-out answer starts a
    # second one, and a replayed invocation answers an empty id. Pass an id to
    # start at most one instance, or to get the id back on replay.
    def spawn_steps(factory_id, payload, id: nil, if_running: nil)
      if if_running && id.to_s.empty?
        raise ArgumentError,
              "#{@node.name}: spawn_steps was given if_running without an id; if_running " \
              "decides what a running instance named by id gets, and a spawn without an id " \
              "always starts a new one"
      end

      handle = @bridge.spawn_request(@token, factory_id, id.to_s,
                                     Codec.plain_payload(payload, "tasklet '#{@node.name}'").b,
                                     if_running.to_s, @replay, @offset)
      SpawnHandle.new(@bridge, handle)
    end

    # Run the block, a call to anything outside the instance (a model, a tool,
    # a service), and keep its result: when this invocation runs again (a
    # crash re-run, a replay, a wake, a move), the call answers the kept result
    # and the block does not run.
    #
    # The result is kept, synced on the controller, before this answers. A
    # crash between the block finishing and that point runs it again, so give
    # the outside service `stable_id` as an idempotency key where it takes
    # one. `name` names the call: a non-empty String of at most 255 bytes,
    # counted per invocation, so a call of another name added before this one
    # leaves its kept result where it was. One call runs at a time; a
    # concurrent one is refused.
    #
    # The result is encoded as an output naming no codec encodes a value (a
    # String as its bytes, anything else as JSON) and read back as JSON, else
    # text, else bytes; `codec` (an object answering `encode` and `decode`, as
    # a change codec does) encodes and decodes it instead. The call answers the
    # result as it reads back, on a first run and a re-run alike.
    #
    # An invocation that took no input-log offset (local mode, a relay with no
    # input log) runs the block each time and keeps nothing. The block's own
    # error propagates and nothing is kept. Raises a `Flezha::Error` coded
    # `effect.bad_name`, `effect.nested`, `effect.outside_invocation`,
    # `effect.too_large` or `effect.not_recorded` (the only one retried).
    def effect(name, codec: nil)
      item = @item || 0
      status, kept = effect_refusals { @bridge.effect_begin(@token, item, name) }
      return effect_decode(kept, codec) if status == :kept

      begin
        value = yield
        encoded = (codec ? codec.encode(value) : Codec.plain_payload(value, "effect '#{name}'")).b
        first = effect_refusals { @bridge.effect_end(@token, item, encoded) } if status == :run
        effect_decode(first || encoded, codec)
      ensure
        # A no-op once the end has kept the result.
        @bridge.effect_release(@token, item)
      end
    end

    # The effect record for this item, as the core folds it.
    def effect_record(consumed_offset)
      {
        verdict: "ok",
        changes: @changes,
        outputs: @outputs,
        consumed_offset: consumed_offset,
        event_time: @now,
        timers: @timers,
        checkpoint: @checkpointed,
        complete: @completion
      }
    end

    private

    # An `effect.*` refusal is non-retryable, but for `effect.not_recorded`: a
    # call that could not be kept is the one worth running again.
    def effect_refusals
      yield
    rescue Error => e
      e.define_singleton_method(:non_retryable?) { true } if e.code.start_with?("effect.") && e.code != "effect.not_recorded"
      raise
    end

    def effect_decode(body, codec) = codec ? codec.decode(body) : Codec.plain_value(body)

    def timer(operation)
      # A rollback is no place to arm a deadline.
      raise Error.new("runtime.timers_unavailable") if @compensating

      @timers << operation
    end

    def release(channel, value, key, metadata)
      # In steps a value rides the edge path to its first consumer, and an
      # inter-tasklet terminal's value to the core as the pass's handoff (a
      # chained loop's carry); a plugin-bound terminal releases nothing there.
      if @spec.runs_in_steps?
        consumer = channel.consumers.first&.tasklet || handoff_producer(channel)
        @outputs << steps_output(channel, consumer, value) if consumer
      else
        channel.consumers.each do |edge|
          @outputs.concat(edge_outputs(channel, edge, value, key, metadata))
        end
      end
      @outputs << plugin_output(channel, value, key, metadata) if channel.sink
      @outputs.concat(call_outputs(channel, value, metadata)) if channel.call
    end

    # The requests a call end sends, with the call chain's transforms and key
    # applied here as an edge's are; the core stamps the call onto each.
    def call_outputs(channel, value, metadata)
      call = channel.call
      Flezha.apply_transforms(call.transforms, value).map do |request|
        {
          target: "plugin", channel: nil, consumer: nil, plugin_channel: channel.name,
          key: call.key_by&.call(request), event_time: nil, metadata: meta_list(metadata),
          payload: call.encode.call(request).b
        }
      end
    end

    # The producer of an inter-tasklet channel nothing consumes; the core reads
    # no consumer for a terminal's handoff.
    def handoff_producer(channel)
      channel.producer if channel.kind == :inter_task && channel.sink.nil? && channel.call.nil?
    end

    def steps_output(channel, consumer, value)
      payload = Codec.encode(value)
      {
        target: "edge", channel: channel.index, consumer: consumer,
        plugin_channel: nil, key: nil, event_time: nil, metadata: [], payload: payload
      }
    end

    # One consumer's frames, with its own transforms, key and clock: none when
    # a filter dropped the value, one per value when a flat_map fanned it.
    def edge_outputs(channel, edge, value, key, metadata)
      definition = @spec.tasklets[edge.tasklet].definition
      # Producer-side, in declaration order: the core routes on `(edge, key)`
      # and never evaluates a host closure.
      Flezha.apply_transforms(edge.transforms, value).map do |transformed|
        {
          target: "edge",
          channel: channel.index,
          consumer: edge.tasklet,
          plugin_channel: nil,
          key: key || (edge.key_by && edge.key_by.call(transformed)) || @cell_key,
          event_time: definition.event_time? ? definition.event_time_of(transformed) : nil,
          metadata: meta_list(metadata),
          payload: Codec.encode(transformed)
        }
      end
    end

    # The plugin frame, serialized once from the value as emitted: an edge's
    # transforms belong to that edge, so they never reach the output.
    def plugin_output(channel, value, key, metadata)
      resolved = key || @cell_key
      if channel.keyed && resolved.nil?
        raise Error.new("runtime.key_required_on_keyed_channel", channel: channel.name)
      end

      pairs = meta_list(metadata)
      stamp_identity(pairs, channel, resolved)
      # An encoder's string carries whatever encoding it was built in, so it is
      # copied into bytes; `String#b` copies even when the encoding matches.
      payload = channel.sink_encode.call(value).b
      {
        target: "plugin", channel: nil, consumer: nil, plugin_channel: channel.name,
        key: resolved, event_time: nil, metadata: pairs, payload: payload
      }
    end

    # Output identity for one plugin frame, appended after the user's metadata.
    # Written only when the class records no effects: under the tier, and for a
    # durable steps relay, the controller derives the identity from the durable
    # effect record, and a second answer here would contradict it. The ordinal
    # advances on every plugin output, stamped or not, so hosts of one topology
    # agree on the sequence.
    def stamp_identity(pairs, channel, key)
      if channel.keyed && !@spec.records_effects?
        pairs.push(
          ["flz.cell", "#{@relay}/#{@node.name}/#{@cell_key}"],
          ["flz.seq", "#{@change_seq}.#{@emit_ordinal}"],
          ["flz.key", key.to_s]
        )
      end
      @emit_ordinal += 1
    end

    def meta_list(metadata)
      return [] unless metadata

      metadata.map do |key, value|
        raise Error.new("runtime.reserved_metadata_key", key: key.to_s) if key.to_s.start_with?(RESERVED_PREFIX)

        [key.to_s, value.to_s]
      end
    end
  end

  # A spawn that reached an instance: the id it resolved to, the caller's or
  # the generated one, and `:spawned`, `:delivered` or `:skipped_on_replay`.
  SpawnResult = Struct.new(:id, :outcome, keyword_init: true)

  # The answer to one spawn request, read inside the invocation that made it:
  # a resolved handle answers every read the same until its invocation ends,
  # when the core releases it and logs once a failure no read collected.
  class SpawnHandle
    # Long enough to reach the core's own deadline, which resolves the request
    # as timed out thirty seconds after it was made.
    WAIT_MS = 30_000

    def initialize(bridge, handle)
      @bridge = bridge
      @handle = handle
    end

    # Wait for the answer: a `SpawnResult`, or raises the `Flezha::Error`
    # naming why the spawn failed, its code the controller's (such as
    # `relay.already_running`) or the core's (`spawn.timeout`,
    # `spawn.unavailable`).
    #
    # The wait is in the core with the GVL released, as every waiting core
    # call is, so under the fiber executor it holds the dispatch thread, and
    # every other handler fiber with it, until the answer arrives.
    def wait
      result = read(WAIT_MS) until result
      result
    end

    # The answer if it has arrived, without waiting: `nil` while pending, and
    # otherwise what `wait` answers or raises.
    def try_result = read(0)

    private

    def read(timeout_ms)
      return unless (json = @bridge.spawn_result(@handle, timeout_ms))

      answer = JSON.parse(json)
      SpawnResult.new(id: answer["id"], outcome: answer["outcome"].to_sym)
    end
  end
end
