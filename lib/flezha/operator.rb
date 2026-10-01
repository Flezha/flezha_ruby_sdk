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
    # The class settings, the tree's own data: `run :steps`,
    # `run steps: { each: :per_input }`, `durable false`,
    # `durable local: { sleep_after: 2_000 }`. Either form takes them; the core
    # reads the tree when the class registers and refuses a misplaced setting
    # by name.
    setting :run, :durable, :max_duration_ms, :description
    # `factory_id` is deliberately NOT inherited: two operators cannot share
    # one, and a subclass silently taking its parent's would be refused by the
    # core at declare time rather than here.

    class << self
      def factory_id(id = nil)
        return @factory_id if id.nil?

        @factory_id = id.to_s
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

      # Declare this class's graph on +builder+: its own, or another class's
      # that inlines it.
      def declare_on(builder)
        short_form? ? wire_short_form(builder) : channels.call(builder)
      end

      # The refusals a class earns before anything is built, raised by
      # `Runner#register_factory` at the call the author wrote and again when
      # the spec is built.
      def check_declaration!
        # A factory id is written, never derived from the class.
        raise Error.new("topology.missing_factory_id", operator: to_s) unless @factory_id

        if short_form?
          raise Error.new("topology.short_form_with_topology", factory_id: @factory_id) if channels
        elsif channels
          refuse_written("topology.short_form_field_on_full_form", SHORT_FORM_ONLY)
        else
          raise Error.new("topology.missing_body", declaration: "operator '#{@factory_id}'",
                                                   body: "`channels`, or one tasklet's `run` as the short form")
        end
      end

      private

      # A short form carries its one tasklet's `run` body instead of a
      # `channels` graph.
      def short_form? = method_defined?(:run)

      # Refuse the declarations among +names+ this class wrote, naming each.
      def refuse_written(code, names)
        names = written(names).keys
        return if names.empty?

        raise Error.new(code, factory_id: @factory_id, fields: names.map { |name| "`#{name}`" })
      end

      def build_builder
        check_declaration!
        builder = Builder.new(@factory_id)
        declare_on(builder)
        builder
      end

      # `input -> tasklet`, and on to the plugin sink when one is declared.
      def wire_short_form(builder)
        edge = builder.input(input || "input", codec: codec).into(tasklet)
        edge.sink(sink) if sink
      end

      # The graph and the settings tree as written: the core judges the tree
      # and refuses a misplaced setting by name.
      def build_spec
        # Sealed before the channels are read: filing every declared sequence
        # on its channel is what can still create one.
        builder = build_builder.seal
        ClassSpec.new(
          factory_id: @factory_id, description: description, max_duration_ms: max_duration_ms,
          run: Flezha.tree_json(run), durable: Flezha.tree_json(durable),
          compact_after_bytes: builder.compact_after_bytes_value,
          tasklets: builder.tasklets, channels: builder.channels
        )
      end
    end
  end

  # A settings tree as its JSON: symbols are strings, `nil` stays absent.
  def self.tree_json(tree)
    case tree
    when Hash then tree.to_h { |name, member| [name.to_s, tree_json(member)] }
    when Symbol then tree.to_s
    else tree
    end
  end
end
