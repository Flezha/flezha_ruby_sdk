# frozen_string_literal: true

require_relative "test_helper"

# The one Ruby tasklet form: a written name, the settings in the class body,
# the clock and the initial state read from what the class defines, the cell
# mode read from the wiring, and output only through the context.
class TaskletFormTest < Minitest::Test
  def refusal(&) = assert_raises(Flezha::Error, &)

  def relay(factory, &channels)
    Class.new(Flezha::Relay).tap do |klass|
      klass.factory_id factory
      klass.channels(&channels)
    end
  end

  def tasklet(name = nil, &body)
    Class.new(Flezha::Tasklet) do
      tasklet_name name if name
      class_eval(&body) if body
      define_method(:run) { |input, ctx| ctx.emit(input) } unless method_defined?(:run)
    end
  end

  def record(klass) = Flezha::Graph.to_host_class_decl(klass.spec)
  def tasklet_record(klass, name) = record(klass)["tasklets"].find { |t| t["name"] == name }

  def native!
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?
  end

  # The dispatcher's answer to one activation over a judged declaration.
  def answer(klass, **activation)
    native!
    Flezha::Dispatch.answer(Flezha::TestSupport.activation(factory: klass.factory_id, **activation),
                            Flezha::TestSupport.wired_deps(klass))
  end

  def work(klass, payload) = answer(klass, items: [Flezha::TestSupport.input_item(payload)])[:effects].first

  # What the prepare crossing answers for each payload on the first channel.
  def prepared(klass, payloads, want_keys: false, want_event_time: false)
    request = { channel: 0, payloads: payloads.map(&:b), want_keys: want_keys,
                want_event_time: want_event_time }
    answer(klass, kind: :key_request, key_request: request)[:prepared]
  end

  # -- the name ----------------------------------------------------------------

  class Nameless < Flezha::Tasklet
    cell_keys "a"
    def run(input, ctx) = ctx.emit(input)
  end

  def test_a_tasklet_without_a_name_is_refused_where_it_is_wired
    named = refusal { relay("nameless") { |c| c.input("in").into(Nameless).output("out") }.spec }
    anonymous = refusal { relay("anonymous") { |c| c.input("in").into(tasklet).output("out") }.spec }

    assert_equal %w[topology.tasklet_name_missing TaskletFormTest::Nameless], [named.code, named.details["tasklet"]]
    assert_equal ["topology.tasklet_name_missing", "an anonymous tasklet class"],
                 [anonymous.code, anonymous.details["tasklet"]]
    assert_equal "topology.tasklet_name_missing", refusal { tasklet("").definition }.code
  end

  class Totals < Flezha::Tasklet
    tasklet_name "totals"
    def init_state(_cell_key) = { sum: 0 }
    def apply_change(state, change) = state[:sum] += change
    def run(input, ctx) = ctx.change(input)
  end

  class RenamedTotals < Flezha::Tasklet
    tasklet_name "totals"
    def init_state(_cell_key) = { sum: 0 }
    def apply_change(state, change) = state[:sum] += change
    def run(input, ctx) = ctx.change(input)
  end

  def test_a_renamed_class_keeps_its_state
    before = Flezha::Testing::Harness.new(Totals)
    [3, 4].each { |value| before.feed(value) }
    after = Flezha::Testing::Harness.new(RenamedTotals)
    after.restore(changes: before.log)

    assert_equal({ sum: 7 }, after.state)
    assert_equal %w[totals totals], [Totals, RenamedTotals].map { |klass| klass.definition.name }
  end

  class Parent < Flezha::Tasklet
    tasklet_name "parent"
    cell_keys "a", "b"
    def cells_for(_input) = ["a"]
    def run(input, ctx) = ctx.emit(input)
  end

  class SilentChild < Parent
    mailbox_capacity 8
  end

  class NamedChild < Parent
    tasklet_name "named_child"
    mailbox_capacity 8
  end

  def test_a_subclass_writes_its_own_name_and_inherits_the_rest
    assert_includes refusal { SilentChild.definition }.message, "TaskletFormTest::SilentChild"
    assert_equal ["named_child", %w[a b], 8],
                 [NamedChild.definition.name, NamedChild.definition.cell_keys,
                  NamedChild.definition.mailbox_capacity]
  end

  # -- the settings ------------------------------------------------------------

  def test_the_idle_ttl_is_whole_seconds_on_the_record
    sessions = tasklet("sessions") do
      idle_ttl_seconds 3_600
      max_cells 100
    end
    klass = relay("sessions_op") { |c| c.input("in").key_by { |v| v }.into(sessions).output("out") }

    assert_equal [3_600_000, 100], tasklet_record(klass, "sessions").values_at("idle_ttl_ms", "max_cells")
    assert_nil tasklet_record(relay("forever") { |c| c.input("in").into(tasklet("forever")).output("out") },
                              "forever")["idle_ttl_ms"], "unset means no eviction"
  end

  def test_an_idle_ttl_that_is_not_a_positive_whole_number_is_refused_where_it_is_written
    [0.2, "60", 0, -1].each do |value|
      error = refusal { tasklet("bad_ttl") { idle_ttl_seconds value } }
      assert_equal ["topology.invalid_setting", "idle_ttl_seconds", value.inspect],
                   [error.code, error.details["setting"], error.details["value"]]
    end
  end

  # -- the clock ---------------------------------------------------------------

  def clock_of(klass) = tasklet_record(relay("clock_op") { |c| c.input("in").into(klass).output("out") },
                                       klass.tasklet_name)["clock"]

  def test_the_clock_follows_from_what_the_class_writes
    event = tasklet("event") { def event_time(input) = Integer(input) }
    ledger = tasklet("ledger") do
      logical_clock
      def init_state(_cell_key) = { seen: 0 }
      def apply_change(state, _change) = state[:seen] += 1
    end

    assert_equal %w[event_time logical wall], [event, ledger, tasklet("wall")].map { |klass| clock_of(klass) }
  end

  EventTimed = Class.new(Flezha::Tasklet) do
    tasklet_name "event_timed"
    def event_time(input) = Integer(input.split(":").first)
    def run(input, ctx) = ctx.emit(input)
  end

  # The core reads the event time a Ruby tasklet extracts at the prepare
  # crossing, and hands it back as the item's `now`.
  def test_an_event_time_method_is_what_the_core_reads_for_the_clock
    klass = relay("event_timed_op") { |c| c.input("in", codec: :text).into(EventTimed).output("out") }

    assert_equal [1_700], prepared(klass, ["1700:x"], want_event_time: true).map { |a| a[:event_time] }
  end

  def test_event_time_beside_the_logical_clock_is_refused_naming_both
    both = tasklet("both_clocks") do
      logical_clock
      def event_time(input) = Integer(input)
    end

    error = refusal { both.definition }
    assert_equal ["topology.two_clocks", { "tasklet" => "both_clocks" }], [error.code, error.details]
  end

  def test_a_logical_clock_on_a_stateless_tasklet_keeps_the_cores_refusal
    native!
    stateless = tasklet("stateless_logical") { logical_clock }
    klass = relay("stateless_logical_op") { |c| c.input("in").into(stateless).output("out") }

    assert_includes Flezha::TestSupport.codes(refusal { Flezha::TestSupport.validate(klass.spec) }),
                    "topology.unservable_clock"
  end

  # -- the initial state -------------------------------------------------------

  def test_the_initial_state_comes_from_init_state_with_the_cell_key
    keyed = tasklet("keyed_init") { def init_state(cell_key) = { key: cell_key } }

    assert_equal({ key: "A" }, Flezha::Testing::Harness.new(keyed, cell_key: "A").state)
    assert_equal({ key: nil }, Flezha::Testing::Harness.new(keyed).state)
  end

  Cart = Struct.new(:items) do
    def initialize(items: []) = super(items)
    def self.from_h(hash) = new(**hash)
  end

  def test_a_state_type_alone_gives_the_initial_state
    typed = tasklet("typed_cart") { state_type Cart }
    both = tasklet("typed_cart_init") do
      state_type Cart
      def init_state(_cell_key) = Cart.new(items: ["first"])
    end

    assert_equal Cart.new, Flezha::Testing::Harness.new(typed).state
    assert_equal Cart.new(items: ["first"]), Flezha::Testing::Harness.new(both).state
    refute record(relay("typed_op") { |c| c.input("in").into(typed).output("out") })["tasklets"]
      .first["change_is_nochange"], "a state_type is a state"
  end

  Required = Struct.new(:total, keyword_init: true) do
    def initialize(total:) = super
    def self.from_h(hash) = new(**hash)
  end

  def test_a_state_type_that_cannot_be_built_is_refused
    error = refusal { tasklet("unbuildable") { state_type Required }.definition }

    assert_equal ["topology.state_not_constructible", { "tasklet" => "unbuildable", "state" => "TaskletFormTest::Required" }],
                 [error.code, error.details]
  end

  def test_a_hash_change_needs_no_class_and_folds_again_on_restore
    deadlines = tasklet("deadlines") do
      def init_state(_cell_key) = { events: [] }
      def apply_change(state, change) = state[:events] << change
      def run(at, ctx) = ctx.change({ kind: "event", deadline: at })
    end
    live = Flezha::Testing::Harness.new(deadlines)
    live.feed(1_000)
    restored = Flezha::Testing::Harness.new(deadlines)
    restored.restore(changes: live.log)

    assert_equal({ events: [{ kind: "event", deadline: 1_000 }] }, live.state)
    assert_equal live.state, restored.state
    assert live.replay_equals!
  end

  # -- migrations --------------------------------------------------------------

  def test_migrations_are_methods_that_receive_the_stored_bytes
    calls = []
    migrating = tasklet("migrating") do
      schema_version 2
      min_readable_schema_version 1
      def init_state(_cell_key) = { total: 0 }
      define_method(:migrate_state) do |from_version, bytes|
        calls << [from_version, bytes]
        { total: Flezha::Codec.decode(bytes)[:sum] }
      end
    end
    stored = Flezha::Codec.encode({ sum: 9 })

    state = Flezha::State.fold_restore(migrating.definition,
                                       { snapshot: { from_version: 1, bytes: stored } }, nil, "migrating")

    assert_equal({ total: 9 }, state)
    assert_equal [[1, stored]], calls
  end

  # -- what no longer exists ---------------------------------------------------

  def test_a_retired_declaration_raises_naming_what_replaced_it
    {
      -> { dynamic_cells } => "key_by",
      -> { clock :event_time } => "event_time",
      -> { idle_ttl_ms 1_000 } => "idle_ttl_seconds",
      -> { migrate_state { |_from, bytes| bytes } } => "migrate_state(from_version, bytes)",
      -> { migrate_change { |_from, bytes| bytes } } => "migrate_change(from_version, bytes)"
    }.each do |declaration, instead|
      error = assert_raises(NoMethodError) { Class.new(Flezha::Tasklet) { instance_exec(&declaration) } }
      assert_includes error.message, instead
    end
  end

  def test_a_retired_initial_state_method_is_refused_naming_init_state
    leftover = tasklet("leftover") { def initial_state(_cell_key) = {} }
    hidden = tasklet("hidden_leftover") { private def initial_state(_cell_key) = {} }

    [leftover, hidden].each do |klass|
      error = refusal { klass.definition }
      assert_equal ["topology.retired_callback", "`init_state(cell_key)`"], [error.code, error.details["instead"]]
    end
  end

  # -- the cell mode is the wiring's ------------------------------------------

  def test_key_by_alone_routes_a_cell_per_key
    counter = tasklet("counter")
    klass = relay("orders_op") { |c| c.input("orders").key_by { |o| o[:customer] }.into(counter).output("out") }
    keys = prepared(klass, ['{"customer":"ann"}', '{"customer":"bo"}'], want_keys: true)
           .map { |a| Flezha::TestSupport.keys_answered(a) }

    assert_equal [["ann"], ["bo"]], keys
  end

  def test_a_static_key_set_fed_by_key_by_is_refused_by_the_core
    native!
    routed = tasklet("routed") do
      cell_keys "a", "b"
      def cells_for(_input) = ["a"]
    end
    klass = relay("routed_op") { |c| c.input("in").key_by(&:to_s).into(routed).output("out") }

    error = refusal { Flezha::TestSupport.validate(klass.spec) }
    assert_includes Flezha::TestSupport.codes(error), "topology.key_by_on_unkeyable_target"
    assert_includes error.message, "routed"
  end

  # -- output only through the context ----------------------------------------

  def test_a_returned_value_is_not_emitted
    answers = tasklet("answers") { def run(input, _ctx) = "echo: #{input}" }
    folds = tasklet("folds") do
      def init_state(_cell_key) = { seen: 0 }
      def apply_change(state, change) = state[:seen] += change[:seen]
      def run(_input, ctx) = ctx.change({ seen: 1 })
    end

    [answers, folds].each do |klass|
      op = relay("#{klass.tasklet_name}_op") { |c| c.input("in", codec: :text).into(klass).output("out") }
      effect = work(op, "hi")
      assert_equal ["ok", []], [effect[:verdict], effect[:outputs]], klass.tasklet_name
      assert_empty Flezha::Testing::Harness.new(klass).feed("hi").outputs, klass.tasklet_name
    end
  end

  def test_a_value_emitted_through_the_context_leaves_once_beside_an_ignored_answer
    both = tasklet("both_ways") do
      def run(_input, ctx)
        ctx.emit("a")
        ctx.emit("b", key: "k")
        "c"
      end
    end
    op = relay("both_ways_op") { |c| c.input("in", codec: :text).into(both).output("out") }

    assert_equal [["a", nil], ["b", "k"]], work(op, "x")[:outputs].map { |o| [o[:payload], o[:key]] }
    assert_equal %w[a b], Flezha::Testing::Harness.new(both).feed("x").outputs
  end

  # The text-codec scenario every dynamic SDK shares: the bytes the plugin
  # frame carries are the string the tasklet emitted.
  def test_a_text_channel_hands_a_string_and_sends_what_was_emitted
    echo = tasklet("echo") { def run(input, ctx) = ctx.emit("echo: #{input}") }
    op = relay("text_echo_op") { |c| c.input("in", codec: :text).into(echo).output("out") }

    assert_equal [["out", "echo: hello"]], work(op, "hello")[:outputs].map { |o| o.values_at(:plugin_channel, :payload) }
  end

  class Place < Flezha::Relay
    factory_id "place"
    run :steps
    output "placed"
    input "order"
    codec :text
    def run(order, ctx) = ctx.emit("placed:#{order}")
  end

  class Upcase < Flezha::Relay
    factory_id "upcase"
    output "out"
    codec :text
    def run(value, _ctx) = value.upcase
  end

  class EmittingUpcase < Flezha::Relay
    factory_id "emitting_upcase"
    output "out"
    codec :text
    def run(value, ctx) = ctx.emit(value.upcase)
  end

  def test_a_short_form_emits_through_its_context_and_its_answer_is_ignored
    assert_equal [%w[placed placed:o1]], work(Place, "o1")[:outputs].map { |o| o.values_at(:plugin_channel, :payload) }
    assert_empty work(Upcase, "hi")[:outputs]
    assert_equal ["HI"], work(EmittingUpcase, "hi")[:outputs].map { |o| o[:payload] }
  end

  class Ticks < Flezha::Relay
    factory_id "ticks_counter"
    input "ticks"
    codec :text
    def init_state(_cell_key) = { count: 0 }
    def apply_change(state, change) = state[:count] += change
    def run(_tick, ctx) = ctx.change(1)
  end

  def test_a_short_form_with_init_state_and_no_output_keeps_its_state
    harness = Flezha::Testing::Harness.new(Ticks.tasklet)
    2.times { harness.feed("t") }

    assert_equal [%w[ticks entry]], record(Ticks)["channels"].map { |c| c.values_at("name", "kind") }
    assert_equal({ count: 2 }, harness.state)
    assert harness.replay_equals!
  end
end
