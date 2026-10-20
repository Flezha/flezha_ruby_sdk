# frozen_string_literal: true

require_relative "codec"
require_relative "ctx"
require_relative "graph"
require_relative "logger"
require_relative "state"

module Flezha
  # A declared class, with the lookups dispatch needs on the hot path, and the
  # live state of every cell it owns.
  class RegisteredClass
    attr_reader :spec, :outgoing

    def initialize(spec)
      @spec = spec
      @outgoing = spec.tasklets.map do |node|
        spec.channels.select { |channel| channel.producer == node.index }
      end
      @cells = {}
      # The identity bytes the core hands with a cell's first activation.
      @identities = {}
      # Cells of one class are touched from every worker thread, and the
      # core's per-cell exclusivity does not extend to this Hash: two
      # different cells of the same class run concurrently by design.
      @lock = Mutex.new
    end

    # Where the cell an activation names keeps its state. The relay is part
    # of it: one class serves every relay of its factory on this runner,
    # such as each steps instance, and each has cells of its own.
    def self.address(activation, node) = [activation[:relay], node.index, activation[:cell_key]]

    # The cell's live state, created from the declaration on first touch.
    def state_of(node, address)
      @lock.synchronize do
        @cells.fetch(address) { @cells[address] = node.definition.build_initial_state(address.last) }
      end
    end

    def set_state(address, state)
      @lock.synchronize { @cells[address] = state }
    end

    def keep_identity(address, identity) = @lock.synchronize { @identities[address] = identity }
    def identity_of(address) = @lock.synchronize { @identities.fetch(address) }

    # Drop a cell that left this runner, or every cell of its relay.
    def forget(address, whole_relay)
      @lock.synchronize do
        [@cells, @identities].each do |table|
          if whole_relay
            table.delete_if { |(relay, _, _), _| relay == address.first }
          else
            table.delete(address)
          end
        end
      end
    end
  end

  # The declared classes this runner serves.
  class Registry
    def initialize
      @classes = {}
    end

    def add(spec)
      if @classes.key?(spec.factory_id)
        raise Error.new("topology.duplicate_factory_id", factory_id: spec.factory_id)
      end

      @classes[spec.factory_id] = RegisteredClass.new(spec)
    end

    def remove(factory_id) = @classes.delete(factory_id)
    def get(factory_id) = @classes[factory_id]
    def all = @classes.values
    def empty? = @classes.empty?
  end

  # Turning one activation into one answer, exactly once.
  #
  # Per-cell exclusivity IS the handout: the core holds a cell's next
  # activation until this one is answered, so an activation that returns
  # unanswered retires that cell for the life of the process. Every path out
  # of `dispatch` therefore ends in a completion or an abandon, through an
  # `ensure` rather than a rule applied at each return site — a raised handler,
  # an encoding failure and a killed thread all reach the same guard.
  module Dispatch
    module_function

    def failed(token, message, non_retryable: false)
      { token: token, outcome: "failed", error: message, non_retryable: non_retryable }
    end

    # What a failure says on the wire. `class: message`, in family with what
    # the other SDKs send — this string is stored in a dead-letter entry and
    # read by a user, and a multi-line backtrace there is noise the retry
    # ladder multiplies. The backtrace is logged host-side instead, where it is
    # a diagnostic rather than a record.
    def error_message(error) = "#{error.class}: #{error.is_a?(Error) ? error.detailed_message : error.message}"

    # A handler declares an error non-retryable by answering `non_retryable?`.
    def non_retryable?(error)
      error.respond_to?(:non_retryable?) && error.non_retryable?
    end

    # An entry channel's decoder, or the entry edge's transforms over what it
    # decoded, rejected a message. Only the entrance raises this: the verdict
    # it becomes tells the core the message is unreadable rather than that the
    # handler failed, so a steps relay parks it and keeps waiting instead of
    # ending.
    class UnreadableMessage < StandardError
      def initialize(cause)
        super(Dispatch.error_message(cause))
      end
    end

    # An item's failure as its verdict and the message the wire carries.
    def failure_of(error)
      return ["unreadable", error.message] if error.is_a?(UnreadableMessage)

      [non_retryable?(error) ? "non_retryable" : "retryable", error_message(error)]
    end

    # A payload as the handler sees it: the entry channel's decoder, or the
    # class codec for a payload between tasklets, then its transforms in
    # declaration order. Empty when a filter rejected it, several values when
    # a flat_map fanned it.
    #
    # One function for the two places a payload crosses in — an input item, and
    # a payload at the prepare crossing — so a filter cannot mean one thing on
    # one path and another on the other.
    def values_of(channel, edge, payload)
      decoded = channel&.decode ? channel.decode.call(payload) : Codec.decode(payload)
      Flezha.apply_transforms(edge&.transforms || [], decoded)
    end

    # The channel's edge that feeds `node`, or nil when it feeds none.
    def edge_into(channel, node)
      channel&.consumers&.find { |edge| edge.tasklet == node }
    end

    # Produce the answer for one activation. Raising is not an option here:
    # every failure becomes a result the core can act on, because the
    # alternative is a cell that never receives work again.
    def answer(activation, deps)
      registered = deps.registry.get(activation[:factory])
      unless registered
        return failed(activation[:token],
                      "no class registered for factory `#{activation[:factory]}`",
                      non_retryable: true)
      end

      node = registered.spec.tasklets[activation[:tasklet]]
      unless node
        return failed(activation[:token],
                      "class `#{activation[:factory]}` has no tasklet at index " \
                      "#{activation[:tasklet]}", non_retryable: true)
      end
      if activation[:cell_identity]
        registered.keep_identity(RegisteredClass.address(activation, node), activation[:cell_identity])
      end

      case activation[:kind]
      when :work then run_work(activation, registered, node, deps)
      when :restore_fold then restore_fold(activation, registered, node)
      when :snapshot_request then snapshot(activation, registered, node)
      when :live_state_read then state_json(activation, registered, node)
      when :apply_state
        registered.set_state(RegisteredClass.address(activation, node),
                             Codec.from_state_json(activation[:state_json]))
        { token: activation[:token], outcome: "state_applied" }
      when :key_request then prepare(activation, registered)
      when :compensate_run then compensate(activation, registered, node, deps)
      when :leave
        registered.forget(RegisteredClass.address(activation, node), activation[:whole_relay])
        { token: activation[:token], outcome: "state_applied" }
      else
        failed(activation[:token], "unknown activation kind `#{activation[:kind]}`",
               non_retryable: true)
      end
    rescue StandardError => e
      failed(activation[:token], error_message(e), non_retryable: non_retryable?(e))
    end

    # Run user code against one cell, and keep the state it produced.
    #
    # Every activation kind that reaches a handler goes through here — an
    # input, a timer firing, a compensation — so the context they receive and
    # the state they leave behind cannot differ between them. The state is
    # written back whether or not the body raised: a change recorded before the
    # raise is already journaled, and dropping it here would make the live
    # cell disagree with its own log.
    def invoke(activation, registered, node, deps, now, replay, offset = nil, item: nil)
      address = RegisteredClass.address(activation, node)
      ctx = Ctx.new(
        spec: registered.spec, node: node, outgoing: registered.outgoing[node.index],
        registered: registered, address: address, cell_key: activation[:cell_key],
        change_seq: activation[:change_seq], now: now, replay: replay,
        state: registered.state_of(node, address), bridge: deps.bridge,
        token: activation[:token], offset: offset, item: item,
        compensating: activation[:kind] == :compensate_run
      )
      begin
        yield(ctx)
        ctx
      ensure
        registered.set_state(address, ctx.state)
      end
    end

    def run_work(activation, registered, node, deps)
      effects = []

      Array(activation[:items]).each_with_index do |item, index|
        now = item[:now] || 0
        begin
          ctx = invoke(activation, registered, node, deps, now, item[:replay] || false,
                       item[:consumed_offset], item: index) do |c|
            run_item(registered, node, item, now, c)
          end
          effects << ctx.effect_record(item[:consumed_offset])
        rescue StandardError => e
          # The backtrace is a host diagnostic, not a record: the verdict below
          # travels to the dead-letter entry and the controller's log.
          Flezha.log.debug("#{node.name} failed an item: #{e.class}: #{e.message}\n" \
                           "#{Array(e.backtrace).first(8).join("\n")}")
          # Items after this one are re-handed by the core, so the batch stops
          # here rather than reporting effects the cell never saw.
          verdict, message = failure_of(e)
          effects << {
            verdict: verdict,
            message: message,
            event_time: now,
            consumed_offset: item[:consumed_offset]
          }
          break
        end
      end

      { token: activation[:token], outcome: "work", effects: effects }
    end

    # One item against the handler its kind names, inside an invocation.
    def run_item(registered, node, item, now, ctx)
      definition = node.definition
      if item[:kind] == :timer
        # Registration is a runtime call, so this cannot be refused at
        # declaration time.
        unless definition.on_timer?
          raise Error.new("runtime.timer_without_on_timer", tasklet: node.name)
        end

        definition.instance.on_timer(
          { kind: item[:timer_kind], scheduled_at: item[:scheduled_at] || now }, ctx
        )
        return nil
      end

      channel = registered.spec.channels[item[:channel] || 0]
      # This node's own edge off the channel: a transform chain declared before
      # one `into` belongs to that consumer and to no other. It runs only on a
      # payload the core marks untransformed: a flow producer already ran it,
      # and running it again would compose the chain with itself.
      edge = item[:form] == :untransformed ? edge_into(channel, node.index) : nil
      # The entrance (the entry channel's decoder and the entry edge's
      # transforms) is where a message the relay cannot read is told
      # apart from a handler that fails on one it can.
      values =
        begin
          values_of(channel, edge, item[:payload] || "".b)
        rescue StandardError => e
          raise unless channel&.kind == :entry

          raise UnreadableMessage, e
        end
      values = values.select { |value| routes_to?(edge, definition, value, ctx.cell_key) } if edge
      # A filtered value reaches no handler and a fanned one reaches it once
      # per value. Either way the item is one effect, so its offset advances
      # once and nothing is retried. What `run` answers is not output: a value
      # leaves through `ctx.emit`.
      values.each { |input| definition.instance.run(input, ctx) }
      nil
    end

    # Whether one value of an untransformed payload belongs to this Cell.
    # The crossing answered the keys of every value, so a fanned payload
    # reaches each Cell it named whole, and each keeps only its own values.
    def routes_to?(edge, definition, value, cell_key)
      keys = cell_key && keys_of(edge, definition, value)
      keys.nil? || keys.include?(cell_key)
    end

    # The Cell keys one value routes to: its edge's key, else the tasklet's
    # `cells_for`, else nil when neither names one. The prepare crossing
    # and the delivery filter both read it, so they cannot disagree.
    def keys_of(edge, definition, value)
      if edge&.key_by then [edge.key_by.call(value).to_s]
      elsif definition&.cells_for? then definition.cells_for(value).map(&:to_s)
      end
    end

    def restore_fold(activation, registered, node)
      registered.set_state(
        RegisteredClass.address(activation, node),
        State.fold_restore(
          node.definition,
          { snapshot: activation[:snapshot], changes: activation[:changes] },
          activation[:cell_key], node.name
        )
      )
      { token: activation[:token], outcome: "restored" }
    end

    def state_json(activation, registered, node)
      state = registered.state_of(node, RegisteredClass.address(activation, node))
      { token: activation[:token], outcome: "state_json", json: Codec.to_state_json(state).b }
    end

    def snapshot(activation, registered, node)
      codec = node.definition.state_codec
      state = registered.state_of(node, RegisteredClass.address(activation, node))
      { token: activation[:token], outcome: "snapshot",
        state: (codec ? codec.encode(state).b : Codec.encode(state)) }
    end

    def prepare(activation, registered)
      request = activation[:key_request]
      return failed(activation[:token], "a key request carried no payloads", non_retryable: true) unless request

      channel = registered.spec.channels[request[:channel]]
      # The crossing answers one key set for the whole payload, which the core
      # hands every keyed consumer, so it reads the first keyed consumer's edge:
      # an unkeyed one's filter must not drop the key.
      edges = channel&.consumers || []
      definition_of = ->(e) { registered.spec.tasklets[e.tasklet]&.definition }
      edge = edges.find { |e| e.key_by || definition_of.(e)&.cells_for? } || edges.first
      definition = edge && definition_of.(edge)

      answers = request[:payloads].map do |payload|
        begin
          values = values_of(channel, edge, payload)
          # A filtered payload answers no key. For a dynamic consumer that is
          # what tells the core not to route it at all — the drop happens
          # before a cell is minted rather than inside one.
          next { keys: [] } if values.empty?

          # A fanned payload answers every value's key, once each.
          keys = request[:want_keys] ? values.flat_map { |value| keys_of(edge, definition, value) || [] }.uniq : []
          event_time =
            request[:want_event_time] && definition&.event_time? ? definition.event_time_of(values.first) : nil
          { keys: keys, event_time: event_time }
        rescue StandardError => e
          # A per-payload refusal enters the input channel's retry ladder; it
          # is not a failure of the crossing, whose other payloads still have
          # answers.
          { error: error_message(e), non_retryable: non_retryable?(e) }
        end
      end

      { token: activation[:token], outcome: "prepared", prepared: answers }
    end

    def compensate(activation, registered, node, deps)
      definition = node.definition
      unless definition.compensate?
        return failed(activation[:token], "#{node.name} declares no compensate hook",
                      non_retryable: true)
      end

      outputs = Array(activation[:outputs]).map { |bytes| Codec.decode(bytes) }
      ctx = invoke(activation, registered, node, deps, 0, false) do |c|
        definition.instance.compensate(outputs, c)
      end
      { token: activation[:token], outcome: "work", effects: [ctx.effect_record(nil)] }
    end

    # Dispatch one activation and answer it exactly once.
    #
    # The guard is the contract, not politeness. An unanswered activation
    # retires its cell for the life of the process, so a raise from the
    # dispatcher, from encoding the result, or from `complete` itself abandons
    # rather than logging and returning.
    def dispatch(activation, native, deps)
      answered = false
      begin
        native.complete(answer(activation, deps))
        answered = true
      rescue StandardError => e
        Flezha.log.error("activation processing failed: #{error_message(e)}")
      ensure
        unless answered
          begin
            native.abandon(activation[:token])
          rescue StandardError => e
            # The core refuses an unknown token: `complete` had already landed
            # and only the bookkeeping after it raised. Nothing is wedged.
            Flezha.log.warn("abandoning activation #{activation[:token]} failed: " \
                            "#{error_message(e)}")
          end
        end
      end
    end
  end

  # What a dispatch loop needs to answer an activation, over one extension.
  DispatchDeps = Struct.new(:registry, :bridge, keyword_init: true)
end
