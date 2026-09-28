# frozen_string_literal: true

require_relative "builder"
require_relative "declarative"
require_relative "errors"
require_relative "tasklet"

module Flezha
  # The base an authored operator subclasses. Its class body declares the
  # factory id and one body: a `channels` graph, or, for the single-tasklet
  # short form, that tasklet's `run` instance method, which emits through its
  # `ctx` as a tasklet's does. `spec` resolves the declaration once.
  class Operator
    extend Declarative
    # A short form's tasklet settings, handed to the tasklet it generates.
    extend TaskletSettings

    # The channels graph. The block receives the builder.
    block_setting :channels
    # The short form's entry channel name, its codec and its optional plugin
    # sink. A full form names its channels in `channels` instead.
    SHORT_FORM_KEYS = %i[input codec sink].freeze
    setting(*SHORT_FORM_KEYS)
    # The declarations only a short form reads.
    SHORT_FORM_ONLY = (SHORT_FORM_KEYS + TaskletSettings::NAMES).freeze
    # The one class setting a short form keeps: a loop is still one tasklet.
    SHORT_FORM_SETTINGS = %i[loop_mode].freeze
    declares :persistent
    # Whether an idle instance is written out and dropped from memory until
    # its next input. Unwritten, a bounded operator sleeps when it persists and a
    # stream never does.
    flag :sleep_when_idle
    # A bounded operator's setting. An unbounded operator carries it too, so that the core refuses
    # it there by name rather than this SDK dropping it.
    flag :sync_durability
    # `factory_id` is deliberately NOT inherited: two operators cannot share
    # one, and a subclass silently taking its parent's would be refused by the
    # core at declare time rather than here.

    class << self
      def factory_id(id = nil)
        return @factory_id if id.nil?

        @factory_id = id.to_s
      end

      # Whether change bytes are flushed to the per-cell WAL. Default true.
      def persistent(flag = nil)
        return @persistent.nil? ? true : @persistent if flag.nil?

        @persistent = flag
      end

      def spec
        @spec ||= build_spec
      end

      # The tasklet a short form generates, or `nil` for a full form. Named
      # after the factory id, verbatim: a tasklet's name is part of every
      # cell's state path, so the full form that replaces this one keeps
      # its cells by naming its tasklet the same.
      def tasklet
        return nil unless short_form?

        check_declaration!
        @tasklet ||= Class.new(Tasklet).tap do |generated|
          operator = self
          generated.tasklet_name(@factory_id)
          generated.define_singleton_method(:body) { operator }
          hand_down(generated, TaskletSettings::NAMES)
        end
      end

      # The refusals a class earns before anything is built, raised by
      # `Runner#register_factory` at the call the author wrote and again when
      # the spec is built.
      def check_declaration!
        unless @factory_id
          raise DeclarationError.new(to_s, nil, "declares no `factory_id`; an operator's " \
                                                "factory id is written, never derived")
        end

        if short_form?
          if channels
            raise DeclarationError.new(
              @factory_id, "run",
              "ShortFormWithTopology: defines a `run` body beside `channels`; a short form's " \
              "one tasklet is `run`, and a graph of tasklets is `channels`, so keep one",
              ["ShortFormWithTopology"]
            )
          end

          # Everything else a class declares is a class setting, left at its
          # default on a short form.
          refuse_written(settings - SHORT_FORM_ONLY - SHORT_FORM_SETTINGS,
                         " on a short form, whose class settings but the loop mode are the " \
                         "defaults; write the full form in `channels` to change one")
        elsif channels
          refuse_written(SHORT_FORM_ONLY,
                         ", which only a short form reads; a full form wires it in `channels`, " \
                         "a channel key on the channel and a tasklet setting on the tasklet")
        else
          raise DeclarationError.new(@factory_id, nil,
                                     "declares no body; write the graph in `channels`, or a " \
                                     "single tasklet's body as `run`")
        end
      end

      private

      # A short form carries its one tasklet's `run` body instead of a
      # `channels` graph.
      def short_form? = method_defined?(:run)

      # Refuse the declarations among +names+ this class wrote, naming each.
      def refuse_written(names, reason)
        names = written(names).keys
        return if names.empty?

        raise DeclarationError.new(@factory_id, nil, "declares `#{names.join('`, `')}`#{reason}")
      end

      def build_builder
        check_declaration!
        builder = Builder.new(@factory_id)
        short_form? ? wire_short_form(builder) : channels.call(builder)
        builder
      end

      # `input -> tasklet`, and on to the plugin sink when one is declared.
      def wire_short_form(builder)
        edge = builder.input(input || "input", codec: codec).into(tasklet)
        edge.sink(sink) if sink
      end

      # Every flag the builder recorded, for either kind: the core judges which
      # a kind may carry and refuses the rest by name.
      def class_spec(kind, bounded)
        # Sealed before the channels are read: filing every declared sequence
        # on its channel is what can still create one.
        builder = build_builder.seal
        ClassSpec.new(
          factory_id: @factory_id, kind: kind, persistent: persistent,
          sleep_when_idle: @sleep_when_idle.nil? ? kind == :bounded && persistent : @sleep_when_idle,
          effectively_once: builder.effectively_once_flag,
          checkpoint_ack: builder.checkpoint_ack_flag,
          checkpoint_every: builder.checkpoint_every_value,
          checkpoint_outputs: builder.checkpoint_outputs_value,
          checkpoint_max_delay_ms: builder.checkpoint_max_delay_ms_value,
          correction_window_ms: builder.correction_window_ms_value,
          beyond_window_policy: builder.beyond_window_policy_value,
          compact_after_bytes: builder.compact_after_bytes_value,
          sync_durability: @sync_durability || false,
          tasklets: builder.tasklets, channels: builder.channels, bounded: bounded
        )
      end
    end
  end

  # An unbounded operator: fed by the controller through its entry channels.
  class UnboundedOperator < Operator
    class << self
      private

      def build_spec = class_spec(:unbounded, nil)
    end
  end

  # A bounded operator: a chain of tasklets over a channels graph, run
  # once or looping. Retry is the edges', as on an unbounded operator.
  class BoundedOperator < Operator
    setting :loop_mode, one_of: %i[once per_input chained]
    setting :rollback_mode, one_of: %i[all_or_nothing keep_progress]
    setting :max_duration_ms

    class << self
      private

      def build_spec
        facts = {
          loop_mode: (loop_mode || :once).to_sym,
          rollback_mode: (rollback_mode || :all_or_nothing).to_sym,
          max_duration_ms: max_duration_ms
        }
        class_spec(:bounded, facts)
      end
    end
  end
end
