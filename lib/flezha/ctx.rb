# frozen_string_literal: true

require "json"

require_relative "builder"
require_relative "codec"
require_relative "errors"
require_relative "identity"

module Flezha
  # The invocation context: every deterministic effect a handler is allowed.
  #
  # The context accumulates one item's effects — recorded changes, outputs,
  # timer operations, the checkpoint flag — and the dispatcher turns them into
  # the item effect the core folds. Nothing here reaches the wire on its own;
  # an emit is a value in a list until the completion carries it.
  class Ctx
    # Metadata keys the runtime owns. A handler that sets one is refused.
    RESERVED_PREFIX = "flz."

    attr_reader :changes, :outputs, :timers, :entity_key, :replay

    def initialize(spec:, node:, outgoing:, operative:, entity_key:, change_seq:,
                   now:, replay:, state:, bridge:, token:)
      @token = token
      @spec = spec
      @node = node
      @outgoing = outgoing
      @operative = operative
      @entity_key = entity_key
      @change_seq = change_seq
      @now = now
      @replay = replay
      @state = state
      @bridge = bridge
      @changes = []
      @outputs = []
      @timers = []
      @checkpointed = false
      @ordinal = 0
      @emit_ordinal = 0
      @identity = nil
    end

    # The entity's state. Mutated only through `change`: a field assigned
    # directly is live in this process and gone after a restart.
    attr_reader :state

    # The core-evaluated declared clock for this item. Never ambient time.
    def now = @now

    # A replay-stable identifier, distinct per invocation within the entity.
    def stable_id
      # Lazily, because most invocations never ask and the identity is the one
      # part of the derivation that costs an allocation.
      @identity ||= Identity.entity_identity(@operative, @node.name, @entity_key.to_s)
      id = Identity.stable_id(@identity, @change_seq, @ordinal)
      @ordinal += 1
      id
    end

    # Record one declared change: journaled, then folded into the state.
    def change(change)
      definition = @node.definition
      unless definition.apply_change?
        raise EmitError,
              "#{@node.name}: recorded a change but declares no apply_change; a change that " \
              "cannot be folded cannot rebuild the state it describes"
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

    # Emit one output value on the procedure's declared output.
    def emit(value, key: nil, channel: nil, metadata: nil)
      targets = channel ? @outgoing.select { |c| c.name == channel } : @outgoing
      if targets.empty?
        raise EmitError,
              channel ?
                "#{@node.name}: emits on channel `#{channel}`, which it does not produce on" :
                "#{@node.name}: emits but declares no output channel; wire its edge with " \
                "into(...) or sink(...)"
      end

      targets.each { |target| release(target, value, key, metadata) }
      nil
    end

    # Emit onto a named channel — the spelling for a procedure with several.
    def emit_to(channel, value, key: nil, metadata: nil)
      emit(value, key: key, channel: channel, metadata: metadata)
    end

    # Register a processing-time timer `ms` from `now`. Answers its time.
    def timer_after(milliseconds)
      # Anchored on `now`, never an ambient read: that is what makes a replayed
      # invocation register the timestamp the live one did.
      at = @now + Integer(milliseconds)
      @timers << { kind: "processing_time", scheduled_at: at, register: true }
      at
    end

    # Register an event-time timer at an absolute time. Answers it.
    def timer_at_event_time(at)
      at = Integer(at)
      @timers << { kind: "event_time", scheduled_at: at, register: true }
      at
    end

    # Cancel a registered timer by the value its registration answered.
    def cancel_timer(kind, at)
      @timers << { kind: kind.to_s, scheduled_at: Integer(at), register: false }
      nil
    end

    # Close the open checkpoint interval (checkpoint ack-granularity classes).
    def checkpoint
      @checkpointed = true
      nil
    end

    # Spawn a workflow instance of `factory_id` with `payload` as its first
    # input, answering a `SpawnHandle` at once. With an `id` already running,
    # `if_running: "deliver"` (the default) hands it `payload` instead, and
    # `"error"` resolves to `SpawnError::AlreadyRunning`. A replayed invocation
    # sends nothing and resolves to `:skipped_on_replay`.
    #
    # Without an `id` the controller generates one and always starts a new
    # instance, so a spawn reissued after a lost or timed-out answer starts a
    # second one, and a replayed invocation answers an empty id. Pass an id to
    # start at most one instance, or to get the id back on replay.
    def spawn_workflow(factory_id, payload, id: nil, if_running: nil)
      if if_running && id.to_s.empty?
        raise ArgumentError,
              "#{@node.name}: spawn_workflow was given if_running without an id; if_running " \
              "decides what a running instance named by id gets, and a spawn without an id " \
              "always starts a new one"
      end

      handle = @bridge.spawn_request(@token, factory_id, id.to_s, Codec.encode(payload),
                                     if_running.to_s, @replay)
      SpawnHandle.new(@bridge, handle)
    end

    # The effect record for this item, as the core folds it.
    def effect(consumed_offset)
      {
        verdict: "ok",
        changes: @changes,
        outputs: @outputs,
        consumed_offset: consumed_offset,
        event_time: @now,
        timers: @timers,
        checkpoint: @checkpointed
      }
    end

    private

    def release(channel, value, key, metadata)
      # A channel with no consumer releases nothing on the edge path: on a
      # workflow that is the plugin-bound terminal, and seeding the next stage
      # from it would hand the driver a terminal output.
      if @spec.structured_workflow?
        first = channel.consumers.first
        @outputs << workflow_output(channel, first, value) if first
      else
        channel.consumers.each do |edge|
          @outputs.concat(edge_outputs(channel, edge, value, key, metadata))
        end
      end
      @outputs << plugin_output(channel, value, key, metadata) if channel.sink
    end

    def workflow_output(channel, edge, value)
      payload = Codec.encode(value)
      {
        target: "edge", channel: channel.index, consumer: edge.procedure,
        plugin_channel: nil, key: nil, event_time: nil, metadata: [], payload: payload
      }
    end

    # One consumer's frames, with its own transforms, key and clock: none when
    # a filter dropped the value, one per value when a flat_map fanned it.
    def edge_outputs(channel, edge, value, key, metadata)
      definition = @spec.procedures[edge.procedure].definition
      # Producer-side, in declaration order: the core routes on `(edge, key)`
      # and never evaluates a host closure.
      Flezha.apply_transforms(edge.transforms, value).map do |transformed|
        {
          target: "edge",
          channel: channel.index,
          consumer: edge.procedure,
          plugin_channel: nil,
          key: key || (edge.key_by && edge.key_by.call(transformed)) || @entity_key,
          event_time: definition.event_time? ? definition.event_time_of(transformed) : nil,
          metadata: meta_list(metadata),
          payload: Codec.encode(transformed)
        }
      end
    end

    # The plugin frame, serialized once from the value as emitted: an edge's
    # transforms belong to that edge, so they never reach the sink.
    def plugin_output(channel, value, key, metadata)
      resolved = key || @entity_key
      if channel.keyed && resolved.nil?
        raise EmitError,
              "#{@node.name}: emits on keyed channel `#{channel.name}` with no key, and has " \
              "no entity key of its own to fall back on; pass `key:` on the emit"
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
    # Written only off the tier: under it the controller derives the identity
    # from the durable effect record, and a second answer here would contradict
    # it. The ordinal advances on every plugin output, stamped or not, so hosts
    # of one topology agree on the sequence.
    def stamp_identity(pairs, channel, key)
      if channel.keyed && !@spec.effectively_once
        pairs.push(
          ["flz.entity", "#{@operative}/#{@node.name}/#{@entity_key}"],
          ["flz.seq", "#{@change_seq}.#{@emit_ordinal}"],
          ["flz.key", key.to_s]
        )
      end
      @emit_ordinal += 1
    end

    def meta_list(metadata)
      return [] unless metadata

      metadata.map do |key, value|
        if key.to_s.start_with?(RESERVED_PREFIX)
          raise EmitError,
                "#{@node.name}: metadata key `#{key}` is inside the reserved " \
                "`#{RESERVED_PREFIX}` namespace, which the runtime stamps itself (entity " \
                "identity, sequence, key and offset); choose a key outside it"
        end

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

    # Wait for the answer: a `SpawnResult`, or raises the `SpawnError` naming
    # why the spawn failed.
    #
    # The wait is in the core with the GVL released, as every waiting core
    # call is, so under the fiber executor it holds the dispatch thread until
    # the answer arrives. A fiber that yielded there instead would not be
    # resumed until the executor's next activation.
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
      if (kind = answer["error"])
        raise SpawnError::KINDS.fetch(kind.to_sym).new(answer["id"], answer["message"])
      end

      SpawnResult.new(id: answer["id"], outcome: answer["outcome"].to_sym)
    end
  end

  # The context a run-sugar workflow body receives.
  class WorkflowCtx
    # How long a workflow body's `recv` parks when the caller names no timeout.
    DEFAULT_RECV_TIMEOUT_MS = 1_000

    attr_reader :instance_id, :result

    def initialize(instance_id, native)
      @instance_id = instance_id
      @native = native
      @completed = false
      @result = nil
    end

    # Emit one output payload. Best-effort and at-least-once.
    def emit(value)
      @native.workflow_emit(@instance_id, Codec.encode(value))
      nil
    end

    # The next inbox item, or nil on timeout.
    def recv(timeout_ms = DEFAULT_RECV_TIMEOUT_MS)
      payload = @native.workflow_recv(@instance_id, timeout_ms)
      payload && Codec.decode(payload)
    end

    # Whether the body's cancellation token has tripped.
    def cancelled? = @native.workflow_cancelled(@instance_id)

    # Set the terminal result; the body is completed once this is called.
    def complete(result = nil)
      @completed = true
      @result = result
      nil
    end

    def completed? = @completed

    def now = @native.monotonic_nanos
  end
end
