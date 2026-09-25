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
        raise DeclarationError.new(type.to_s, which,
                                   "declares no `to_h`; a durable #{which} is encoded as the " \
                                   "Hash it answers")
      end
      unless type.respond_to?(:from_h)
        raise DeclarationError.new(type.to_s, which,
                                   "declares no `.from_h`; a durable #{which} is rebuilt from " \
                                   "the Hash it was encoded as")
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
                :state_codec, :change_codec, :migrate_state, :migrate_change, :instance

    # Read straight off the class rather than through a mirrored option hash:
    # the hash was the declaration written twice, and a field added to one half
    # and not the other is a declaration that silently does not reach the core.
    # Captured here rather than delegated, because the dispatcher reads these
    # on the hot path.
    def initialize(klass)
      @body = klass.body
      @name = klass.tasklet_name
      @cell_keys = klass.cell_keys
      @dynamic = klass.dynamic_cells?
      @idle_ttl_ms = klass.idle_ttl_ms
      @max_cells = klass.max_cells
      @clock = (klass.clock || :wall).to_sym
      @mailbox_capacity = klass.mailbox_capacity
      @schema_version = klass.schema_version
      @min_readable_schema_version = klass.min_readable_schema_version
      @state_codec = klass.state_codec
      @change_codec = klass.change_codec
      @migrate_state = klass.migrate_state
      @migrate_change = klass.migrate_change
      @instance = @body.new
    end

    def cell_mode
      return :static if @cell_keys
      return :dynamic if @dynamic

      :single
    end

    def declares?(method) = @body.method_defined?(method) || @body.private_method_defined?(method)

    def initial_state? = declares?(:initial_state)
    def apply_change? = declares?(:apply_change)
    def cells_for? = declares?(:cells_for)
    def event_time? = declares?(:event_time)
    def on_timer? = declares?(:on_timer)
    def compensate? = declares?(:compensate)

    def build_initial_state(cell_key)
      initial_state? ? @instance.initial_state(cell_key) : nil
    end

    def fold_change(state, change) = @instance.apply_change(state, change)
    def cells_for(input) = Array(@instance.cells_for(input))
    def event_time_of(input) = Integer(@instance.event_time(input))
  end

  # A tasklet's class-level declarations. A tasklet class carries them,
  # and so does a short-form operator class, which hands them to the
  # tasklet it generates.
  module TaskletSettings
    # The scalar declarations. Each is read by its bare name and written by
    # naming a value: `clock` answers, `clock :event_time` declares.
    SCALARS = %i[mailbox_capacity schema_version min_readable_schema_version
                 state_codec change_codec clock].freeze
    BLOCKS = %i[migrate_state migrate_change].freeze
    # Written by the declarations below, which take more than one value or
    # write more than one variable; named so a subclass inherits them.
    WRITTEN = %i[cell_keys dynamic idle_ttl_ms max_cells state_type change_type].freeze
    NAMES = (SCALARS + BLOCKS + WRITTEN).freeze

    def self.extended(base)
      base.setting(*SCALARS)
      base.block_setting(*BLOCKS)
      base.declares(*WRITTEN)
    end

    def cell_keys(*keys)
      return @cell_keys if keys.empty?

      @cell_keys = keys.flatten.map(&:to_s)
    end

    def dynamic_cells(idle_ttl_ms: nil, max_cells: nil)
      @dynamic = true
      @idle_ttl_ms = idle_ttl_ms
      @max_cells = max_cells
    end

    # Whether cells appear on first key rather than from a declared set.
    def dynamic_cells? = @dynamic || false

    attr_reader :idle_ttl_ms, :max_cells

    # Declare the class a cell's State is. Its `to_h` is what is encoded
    # and its `.from_h` is what rebuilds it, so a rename of the class does
    # not break the restore of records written before it: the durable form
    # is the Hash, not the class.
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
  # The class body carries the scalar declarations; the instance methods carry
  # the behaviour. Which optional hooks a tasklet declares is read from the
  # methods it defines — `apply_change` present means it folds changes,
  # `on_timer` present means it may register timers — so a declaration and its
  # implementation cannot disagree.
  class Tasklet
    extend Declarative
    extend TaskletSettings

    # `tasklet_name` is deliberately NOT inherited: a node identity is per
    # class, and a subclass silently taking its parent's would make two
    # tasklets claim one node — which the builder refuses, at the far end of
    # the declaration rather than here.

    class << self
      # The node identity within its operator. Defaults to the class name in
      # snake_case, which is what an author would have typed anyway.
      def tasklet_name(name = nil)
        return @tasklet_name || default_name if name.nil?

        @tasklet_name = name.to_s
      end

      # The class whose instance methods run for this tasklet: the tasklet
      # itself, or the short-form operator whose `run` body it was generated
      # from.
      def body = self

      # A tasklet has no sink: the one-tasklet short form is an operator,
      # and its sink is written there.
      def sink(*, **)
        raise DeclarationError.new(
          "<tasklet>", to_s,
          "declares a `sink`, and a tasklet has none; a one-tasklet operator is a " \
          "Flezha::UnboundedOperator or Flezha::BoundedOperator subclass with a `run` body, " \
          "and its `sink` is written there"
        )
      end

      # The resolved declaration. Built once: it instantiates the tasklet,
      # and the dispatcher reads it on the hot path.
      def definition
        @definition ||= build_definition
      end

      private

      def build_definition
        unless body.method_defined?(:run)
          stale = body.method_defined?(:process) ? " (`process` is not read)" : ""
          raise DeclarationError.new("<tasklet>", tasklet_name,
                                     "#{body} declares no `run` method#{stale}; a tasklet's " \
                                     "body is its `run`, what runs against an input")
        end
        if @cell_keys && @dynamic
          raise DeclarationError.new("<tasklet>", tasklet_name,
                                     "declares both a static key set and dynamic cells; the " \
                                     "two modes are exclusive")
        end
        if @cell_keys&.empty?
          raise DeclarationError.new("<tasklet>", tasklet_name,
                                     "declares an empty static key set; a static tasklet " \
                                     "names its cells")
        end
        # State without a fold is legitimate: a value seeded at start and read
        # but never mutated has nothing to journal, and `ctx.change` already
        # refuses at the point of use. A fold with no state to fold into is not.
        if !body.method_defined?(:initial_state) && body.method_defined?(:apply_change)
          raise DeclarationError.new("<tasklet>", tasklet_name,
                                     "declares apply_change but no initial_state; there is " \
                                     "nothing for a change to fold into")
        end

        definition = Definition.new(self)
        # A state that cannot be encoded cannot be restored, and the first
        # anyone would learn of it is a restart. Walk the declared starting
        # state now.
        if definition.initial_state? && !state_codec
          Codec.assert_representable("#{tasklet_name}.state",
                                     definition.build_initial_state(nil))
        end
        definition
      end

      def default_name
        name.to_s.split("::").last
            .gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
            .gsub(/([a-z\d])([A-Z])/, '\1_\2')
            .downcase
      end
    end
  end
end
