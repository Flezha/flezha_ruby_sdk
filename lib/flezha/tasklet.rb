# frozen_string_literal: true

require_relative "codec"
require_relative "declarative"
require_relative "errors"

module Flezha
  # The codec for an author-declared State or Change class.
  #
  # `to_h` on the way out and `.from_h` on the way back, over MessagePack —
  # never `Marshal`, which embeds the class name and the instance-variable
  # layout, so a rename would break the restore of every record written before
  # it. Encoding the Hash instead is what makes the rename survivable.
  #
  # Built on the same `state_codec` / `change_codec` seam a hand-written codec
  # uses, rather than beside it: the dispatcher and the restore fold already
  # ask that seam, and a second one would be a second answer to one question.
  class TypeCodec
    attr_reader :type

    def initialize(type, which)
      unless type.method_defined?(:to_h)
        raise Codec.unrepresentable(type, which.to_s, "it declares no `to_h`; a durable #{which} is encoded as the " \
                                                      "Hash it answers")
      end
      unless type.respond_to?(:from_h)
        raise Codec.unrepresentable(type, which.to_s,
                                    "it declares no `.from_h`; a durable #{which} is rebuilt from the Hash it " \
                                    "was encoded as")
      end

      @type = type
    end

    def encode(value) = Codec.encode(value.to_h)
    def decode(bytes) = @type.from_h(Codec.decode(bytes))
  end

  # What a declared tasklet resolves to: the scalar declarations, and the one
  # instance whose methods the dispatcher calls.
  #
  # Tasklets hold no state of their own — a cell's state lives in the
  # registry and is handed to each invocation — so one instance per class is
  # enough, and the dispatcher never allocates per activation.
  class Definition
    attr_reader :name, :cell_keys, :idle_ttl_ms, :max_cells, :clock,
                :mailbox_capacity, :schema_version, :min_readable_schema_version,
                :state_type, :state_codec, :change_codec, :instance

    # Read straight off the class rather than through a mirrored option hash:
    # the hash was the declaration written twice, and a field added to one half
    # and not the other is a declaration that silently does not reach the core.
    # Captured here rather than delegated, because the dispatcher reads these
    # on the hot path.
    def initialize(klass)
      @body = klass.body
      @name = klass.tasklet_name
      @cell_keys = klass.cell_keys
      @idle_ttl_ms = klass.idle_ttl_seconds&.*(1000)
      @max_cells = klass.max_cells
      @mailbox_capacity = klass.mailbox_capacity
      @schema_version = klass.schema_version
      @min_readable_schema_version = klass.min_readable_schema_version
      @state_type = klass.state_type
      @state_codec = klass.state_codec
      @change_codec = klass.change_codec
      @instance = @body.new
      @clock = if event_time? then :event_time
               elsif klass.logical_clock? then :logical
               else :wall
               end
    end

    def declares?(method) = @body.method_defined?(method) || @body.private_method_defined?(method)

    def init_state? = declares?(:init_state)
    def apply_change? = declares?(:apply_change)
    def cells_for? = declares?(:cells_for)
    def event_time? = declares?(:event_time)
    def on_timer? = declares?(:on_timer)
    def compensate? = declares?(:compensate)
    def migrate_state? = declares?(:migrate_state)
    def migrate_change? = declares?(:migrate_change)

    # Whether a Cell of this tasklet holds a state at all.
    def stateful? = init_state? || !@state_type.nil?

    def build_initial_state(cell_key)
      if init_state? then @instance.init_state(cell_key)
      elsif @state_type then @state_type.new
      end
    end

    def fold_change(state, change) = @instance.apply_change(state, change)
    def cells_for(input) = Array(@instance.cells_for(input))
    def event_time_of(input) = Integer(@instance.event_time(input))
  end

  # A tasklet's class-level declarations. A tasklet class carries them,
  # and so does a short-form relay class, which hands them to the
  # tasklet it generates.
  module TaskletSettings
    # The scalar declarations. Each is read by its bare name and written by
    # naming a value: `max_cells` answers, `max_cells 100` declares.
    SCALARS = %i[mailbox_capacity schema_version min_readable_schema_version
                 state_codec change_codec max_cells].freeze
    # Written by the declarations below, which check their value or write
    # more than one variable; named so a subclass inherits them.
    WRITTEN = %i[cell_keys idle_ttl_seconds state_type change_type].freeze
    # Declared by a bare mention in the class body.
    FLAGS = %i[logical_clock].freeze
    NAMES = (SCALARS + WRITTEN + FLAGS).freeze

    # Class-body declarations that no longer exist, with what replaced each.
    RETIRED = {
      dynamic_cells: "a tasklet fed by `key_by` has one Cell per key; write `idle_ttl_seconds` " \
                     "and `max_cells` on their own",
      clock: "defining `event_time` makes the clock event time and `logical_clock` makes it " \
             "logical; otherwise it is the wall clock",
      idle_ttl_ms: "write `idle_ttl_seconds`, a whole number of seconds",
      migrate_state: "define `migrate_state(from_version, bytes)` as an instance method",
      migrate_change: "define `migrate_change(from_version, bytes)` as an instance method"
    }.freeze

    def self.extended(base)
      base.setting(*SCALARS)
      FLAGS.each { |name| base.flag(name) }
      base.declares(*WRITTEN)
    end

    RETIRED.each do |retired, instead|
      define_method(retired) do |*, **|
        raise NoMethodError.new("`#{retired}` is not a tasklet declaration any more: #{instead}",
                                retired)
      end
    end

    def cell_keys(*keys)
      return @cell_keys if keys.empty?

      @cell_keys = keys.flatten.map(&:to_s)
    end

    # How long a keyed Cell may sit idle before it is evicted, in whole
    # seconds. Unset, a Cell is never evicted for being idle.
    def idle_ttl_seconds(seconds = nil)
      return @idle_ttl_seconds if seconds.nil?

      unless seconds.is_a?(Integer) && seconds.positive?
        raise Error.new("topology.invalid_setting", declaration: "tasklet '#{self}'", setting: "idle_ttl_seconds",
                                                    value: seconds.inspect,
                                                    expected: "a positive whole number of seconds")
      end

      @idle_ttl_seconds = seconds
    end

    # Whether the class body declared the logical clock.
    def logical_clock? = @logical_clock || false

    # Declare the class a cell's State is. Its `to_h` is what is encoded
    # and its `.from_h` is what rebuilds it, so a rename of the class does
    # not break the restore of records written before it: the durable form
    # is the Hash, not the class. A tasklet that defines no `init_state`
    # starts each Cell from `type.new`.
    def state_type(type = nil)
      return @state_type if type.nil?

      @state_type = type
      @state_codec = TypeCodec.new(type, "state")
    end

    # Declare the class a recorded Change is. Same contract as `state_type`.
    def change_type(type = nil)
      return @change_type if type.nil?

      @change_type = type
      @change_codec = TypeCodec.new(type, "change")
    end
  end

  # The base every authored tasklet subclasses.
  #
  # The class body carries the `tasklet_name` and the scalar declarations; the
  # instance methods carry the behaviour. Which optional hooks a tasklet
  # declares is read from the methods it defines — `apply_change` present means
  # it folds changes, `event_time` present means its clock is event time — so a
  # declaration and its implementation cannot disagree. A value leaves only
  # through `ctx.emit`; what `run` answers is not output.
  class Tasklet
    extend Declarative
    extend TaskletSettings

    # `tasklet_name` is deliberately NOT inherited: a node identity is per
    # class, and a subclass silently taking its parent's would make two
    # tasklets claim one node — which the builder refuses, at the far end of
    # the declaration rather than here.

    class << self
      # The node identity within its relay, and the name every Cell's
      # saved state is kept under. Written on every concrete tasklet, never
      # derived: a derived name would move the saved state when the class is
      # renamed. Not `name`, which is `Class#name`.
      def tasklet_name(name = nil)
        return @tasklet_name if name.nil?

        @tasklet_name = name.to_s
      end

      # The class whose instance methods run for this tasklet: the tasklet
      # itself, or the short-form relay whose `run` body it was generated
      # from.
      def body = self

      # A tasklet has no output: the one-tasklet short form is a relay,
      # and its output is written there.
      def output(*, **)
        raise Error.new("topology.relay_field_on_tasklet",
                        tasklet: to_s, fields: ["`output`"],
                        instead: "a Flezha::Relay subclass with a `run` body")
      end

      # The resolved declaration. Built once: it instantiates the tasklet,
      # and the dispatcher reads it on the hot path.
      def definition
        @definition ||= build_definition
      end

      private

      def build_definition
        check_name
        definition = Definition.new(self)
        check_hooks(definition)
        if @cell_keys&.empty?
          raise Error.new("topology.static_tasklet_without_cell_keys", tasklet: @tasklet_name)
        end
        raise Error.new("topology.two_clocks", tasklet: @tasklet_name) if definition.event_time? && logical_clock?

        check_state(definition)
      end

      def check_name
        return unless @tasklet_name.to_s.empty?

        raise Error.new("topology.tasklet_name_missing", tasklet: name || "an anonymous tasklet class")
      end

      def check_hooks(definition)
        unless body.method_defined?(:run)
          raise Error.new("topology.missing_body", declaration: "tasklet '#{@tasklet_name}'", body: "`run`")
        end
        return unless definition.declares?(:initial_state)

        raise Error.new("topology.retired_callback", tasklet: @tasklet_name, callback: "`initial_state`",
                                                     instead: "`init_state(cell_key)`")
      end

      # State without a fold is legitimate: a value seeded at start and read
      # but never mutated has nothing to journal, and `ctx.change` already
      # refuses at the point of use. A fold with no state to fold into is not.
      def check_state(definition)
        if definition.apply_change? && !definition.stateful?
          raise Error.new("topology.change_without_state", tasklet: @tasklet_name)
        end

        # A state that cannot be built or encoded fails at the first Cell or
        # the first restart. Build the starting state now instead.
        if definition.init_state? && !state_codec
          Codec.assert_representable("#{@tasklet_name}.state", definition.build_initial_state(nil))
        elsif !definition.init_state? && state_type
          check_state_type
        end
        definition
      end

      def check_state_type
        state_type.new
      rescue StandardError
        raise Error.new("topology.state_not_constructible", tasklet: @tasklet_name, state: state_type.to_s)
      end
    end
  end
end
