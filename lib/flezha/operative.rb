# frozen_string_literal: true

require_relative "builder"
require_relative "declarative"
require_relative "errors"
require_relative "tasklet"

module Flezha
  # The base an authored operative subclasses. Its class body declares the
  # factory id and one body: a `channels` graph, or, for the single-tasklet
  # short form, that tasklet's `run` instance method. `spec` resolves the
  # declaration once.
  class Operative
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
    declares :persistent
    # `factory_id` is deliberately NOT inherited: two operatives cannot share
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
          operative = self
          generated.tasklet_name(@factory_id)
          generated.define_singleton_method(:body) { operative }
          hand_down(generated, TaskletSettings::NAMES)
        end
      end

      # The refusals a class earns before anything is built, raised by
      # `Runner#register_factory` at the call the author wrote and again when
      # the spec is built.
      def check_declaration!
        unless @factory_id
          raise DeclarationError.new(to_s, nil, "declares no `factory_id`; an operative's " \
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
          refuse_written(settings - SHORT_FORM_ONLY,
                         " on a short form, whose class settings are the defaults; write the " \
                         "full form in `channels` to change one")
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
    end
  end

  # A stream operative: fed by the controller through its entry channels.
  class StreamOperative < Operative
    class << self
      private

      def build_spec
        # Sealed before the channels are read: filing every declared sequence
        # on its channel is what can still create one.
        builder = build_builder.seal
        ClassSpec.new(
          factory_id: @factory_id, kind: :stream, persistent: persistent,
          effectively_once: builder.effectively_once_flag,
          checkpoint_ack: builder.checkpoint_ack_flag,
          checkpoint_every: builder.checkpoint_every_value,
          checkpoint_outputs: builder.checkpoint_outputs_value,
          checkpoint_max_delay_ms: builder.checkpoint_max_delay_ms_value,
          correction_window_ms: builder.correction_window_ms_value,
          beyond_window_policy: builder.beyond_window_policy_value,
          compact_after_bytes: builder.compact_after_bytes_value,
          sync_durability: builder.sync_durability_flag,
          tasklets: builder.tasklets, channels: builder.channels
        )
      end
    end
  end

  # A workflow operative: a chain of tasklets over a channels graph, run
  # once or looping.
  class WorkflowOperative < Operative
    setting :loop_mode, :rollback_mode, :retry_attempts, :retry_backoff_ms,
            :barrier_retry_attempts, :max_duration_ms, :wake_after_ms
    flag :sync_durability

    class << self
      private

      def build_spec
        builder = build_builder.seal
        ClassSpec.new(
          factory_id: @factory_id, kind: :workflow, persistent: persistent,
          effectively_once: builder.effectively_once_flag, checkpoint_ack: false,
          checkpoint_outputs: :as_produced, beyond_window_policy: :process_as_arrival,
          sync_durability: sync_durability_declared?,
          tasklets: builder.tasklets, channels: builder.channels,
          workflow: {
            loop_mode: (loop_mode || :once).to_sym,
            rollback_mode: (rollback_mode || :all_or_nothing).to_sym,
            retry_attempts: retry_attempts, retry_backoff_ms: retry_backoff_ms,
            barrier_retry_attempts: barrier_retry_attempts,
            max_duration_ms: max_duration_ms, wake_after_ms: wake_after_ms
          }
        )
      end

      def sync_durability_declared? = @sync_durability || false
    end
  end
end
