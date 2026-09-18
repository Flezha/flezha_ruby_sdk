# frozen_string_literal: true

require_relative "test_helper"

Flezha::TestSupport.require_test_app("fixtures")

# The entry point's contract: declare, warm up, then serve — and the JIT
# decision the operator can always take back.
class RunnerTest < Minitest::Test
  class Echo < Flezha::Procedure
    procedure_name "runner_echo"
    def run(input, _ctx) = input
  end

  class EchoOperative < Flezha::StreamOperative
    factory_id "runner_echo_op"
    channels { |c| c.input("input").into(Echo).sink("out") }
  end

  def boot_recording
    core = Flezha::TestSupport::FakeCore.new
    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperative)
    Flezha::TestSupport.without_a_terminal do
      Process.stub(:warmup, -> { core.events << :warmup }) do
        runner.boot(core: core)
        core.polled.pop
        runner.drain
      end
    end
    [core.events, core]
  end

  def test_warmup_runs_after_the_last_declaration_and_before_the_first_poll
    events, = boot_recording

    declared = events.index(:declare)
    warmed = events.index(:warmup)
    polled = events.index(:poll)

    refute_nil declared
    refute_nil warmed
    refute_nil polled
    assert_operator declared, :<, warmed, "warmup must follow the last declaration"
    assert_operator warmed, :<, polled, "warmup must precede the first poll"
  end

  def test_declaration_precedes_serving_for_every_declared_class
    core = Flezha::TestSupport::FakeCore.new
    events = core.events
    second = Class.new(Flezha::StreamOperative)
    second.factory_id "runner_second_op"
    second.channels { |c| c.input("in").into(Echo, "second_node").sink("o") }

    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperative).register_factory(second)
    Flezha::TestSupport.without_a_terminal do
      Process.stub(:warmup, -> { core.events << :warmup }) do
        runner.boot(core: core)
        core.polled.pop
        runner.drain
      end
    end

    assert_equal 2, core.declarations.size
    assert_operator events.rindex(:declare), :<, events.index(:poll),
                    "both declarations reach the core before any activation is polled"
  end

  def test_the_core_is_released_on_drain
    events, = boot_recording

    assert_equal :free, events.last
  end

  def test_await_termination_waits_for_a_stop_then_drains
    core = Flezha::TestSupport::FakeCore.new
    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperative)
    Flezha::TestSupport.without_a_terminal do
      Process.stub(:warmup, -> { core.events << :warmup }) do
        runner.boot(core: core)
        core.polled.pop
        waiter = Thread.new { runner.await_termination }
        Thread.pass until waiter.status == "sleep" || !waiter.alive?
        runner.stop

        assert_same runner, waiter.value
        assert_equal :free, core.events.last
      ensure
        runner.drain unless core.events.last == :free
      end
    end
  end

  def test_a_runner_with_no_class_is_refused
    runner = Flezha::Runner.new("test-ns")

    assert_raises(Flezha::Error) { runner.boot(core: Flezha::TestSupport::FakeCore.new) }
  end

  def test_an_unknown_executor_is_refused_by_name
    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperative).executor(:ractors)

    Flezha::TestSupport.without_a_terminal do
      assert_includes(
        assert_raises(Flezha::Error) { runner.boot(core: Flezha::TestSupport::FakeCore.new) }.message,
        ":threads and :fibers"
      )
    end
  end

  # YJIT is on by default because handler bodies are the one thing the GVL
  # bounds, so the JIT's gain lands exactly where the ceiling is — and the
  # resulting state is reported so a measurement can never be ambiguous.
  def test_yjit_is_reported_and_the_runner_override_wins
    runner = Flezha::Runner.new("test-ns")

    assert_equal "disabled (runner override)",
                 runner.yjit(false).send(:resolve_yjit)
  end

  def test_the_yjit_decision_names_why
    state = Flezha::Runner.new("test-ns").send(:resolve_yjit)

    assert_kind_of String, state
    assert_match(/\A(enabled|disabled|unavailable)/, state)
    if defined?(RubyVM::YJIT)
      assert RubyVM::YJIT.enabled?, "this Ruby has YJIT, so the default decision enables it"
      assert_match(/enabled/, state)
    else
      assert_match(/unavailable/, state)
    end
  end

  def test_the_operators_environment_decision_wins_over_the_default
    previous = ENV["FLEZHA_YJIT"]
    ENV["FLEZHA_YJIT"] = "0"
    # Only observable on a Ruby where YJIT is not already on; where it is, the
    # earlier "already on" answer is the honest one and is asserted above.
    state = Flezha::Runner.new("test-ns").send(:resolve_yjit)

    assert_match(/enabled \(already on\)|disabled \(FLEZHA_YJIT=0\)|unavailable/, state)
  ensure
    ENV["FLEZHA_YJIT"] = previous
  end

  # The harness manages a runner as an Erlang Port and stops it by closing the
  # port, which is this process's standard input — so a boot with no terminal
  # asks the core to watch for end of file, and a runner that stops asking is a
  # runner nothing can stop. This is the assertion that says so: the name check
  # below would stay green if `observe_shutdown` dropped the call entirely.
  def test_a_boot_with_no_terminal_asks_the_core_to_watch_for_end_of_file
    events, = boot_recording

    assert_includes events, :watch_stdin_eof
  end

  # The double stands in for a whole core, so it answers every call a host can
  # make on one. `Core` generates its delegations from `ENTRY_POINTS` and
  # `native_test.rb` asserts that list against the veneer's exports in both
  # directions; nothing asserted it against the double, which is hand written.
  # A miss is silent until a runner reaches the entry point, and then it is a
  # `NoMethodError` in whichever suite boots first.
  #
  # This checks NAMES and nothing else, and the double generates the same set it
  # is checked against — so it holds the list closed and proves no behaviour.
  # What it cannot see: arity (a veneer entry point that gains a parameter
  # leaves `Core` forwarding `*args` while a hand-written fixed-arity method
  # here raises `ArgumentError`), return types, and a scripted method whose
  # answer has drifted from the core's. Each of those is the job of the test
  # that drives the call.
  def test_the_double_answers_every_call_a_host_can_make_on_a_core
    # `free` is the one call `Core` does not generate: it releases what
    # `create` made, so the wrapper names it rather than listing it.
    reachable = Flezha::Core::ENTRY_POINTS + %i[free]

    assert_empty reachable - Flezha::TestSupport::FakeCore.instance_methods(false),
                 "the double is missing calls a runner can make on its core"
  end

  # -- the §5 short form ---------------------------------------------------

  # The factory ids a runner currently declares. Read off the runner's own
  # list: the Ruby runner exposes no accessor for it.
  def declared(runner) = runner.instance_variable_get(:@classes).map(&:factory_id)

  def decl(operative) = Flezha::Graph.to_host_class_decl(operative.spec)

  def refused(operative)
    assert_raises(Flezha::DeclarationError) { Flezha::Runner.new("test-ns").register_factory(operative) }
  end

  # The short forms under test are the test app's own, the classes the
  # end-to-end run registers. Each twin is the full form an author would write
  # instead.
  class DoNothingProcedure < Fixtures::PassThrough
    procedure_name "sugar_do_nothing"
  end

  class DoNothingTwin < Flezha::StreamOperative
    factory_id "sugar_do_nothing"
    channels { |c| c.input("input", codec: :text).into(DoNothingProcedure).sink("sugar_do_nothing_echo") }
  end

  class WorkflowProcedure < Fixtures::PassThrough
    procedure_name "sugar_workflow"
  end

  class WorkflowTwin < Flezha::WorkflowOperative
    factory_id "sugar_workflow"
    channels { |c| c.input("order", codec: :text).into(WorkflowProcedure).sink("sugar_workflow_placed") }
  end

  class StateOnlyProcedure < Flezha::Procedure
    procedure_name "sugar_state_only"
    def initial_state(_entity_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]
    def run(_input, _ctx) = nil
  end

  class StateOnlyTwin < Flezha::StreamOperative
    factory_id "sugar_state_only"
    channels { |c| c.input("ticks", codec: :text).into(StateOnlyProcedure) }
  end

  def test_a_stream_short_form_is_its_hand_written_twin
    short = Fixtures::SugarDoNothing

    assert_equal decl(DoNothingTwin), decl(short)
    assert_equal :stream, short.spec.kind
    assert_equal "sugar_do_nothing", short.procedure.procedure_name,
                 "the generated procedure is named after the factory id, verbatim"
    assert_equal "héllo", short.spec.channels.first.decode.call("héllo".b),
                 "the entry decodes text"
    refute_operator short, :<, Flezha::Procedure, "a short form is an operative, not a procedure"
  end

  def test_a_workflow_short_form_is_a_once_workflow_equal_to_its_twin
    short = Fixtures::SugarWorkflow

    assert_equal decl(WorkflowTwin), decl(short)
    assert_equal :workflow, short.spec.kind
    assert_equal :once, short.spec.workflow[:loop_mode]
    assert_equal %w[order sugar_workflow_placed], short.spec.channels.map(&:name)
  end

  def test_a_short_form_with_no_sink_declares_its_entry_and_its_procedure_only
    short = Fixtures::SugarStateOnly

    assert_equal decl(StateOnlyTwin), decl(short)
    assert_equal [%w[ticks entry]], decl(short)["channels"].map { |c| [c["name"], c["kind"]] },
                 "no plugin channel"
    assert_equal ["sugar_state_only"], short.spec.procedures.map(&:name)
    assert_equal %w[sugar_state_only], declared(Flezha::Runner.new("test-ns").register_factory(short)),
                 "an entry name and a codec need no sink"
  end

  def test_the_core_accepts_a_sinkless_short_form
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    derived = Flezha::TestSupport.validate(Fixtures::SugarStateOnly.spec)

    assert_equal [%w[ticks entry]], derived["channels"].map { |c| [c["name"], c["kind"]] }
  end

  def test_the_generated_procedure_runs_the_class_s_run_body_and_journals_its_changes
    harness = Flezha::Testing::Harness.new(Fixtures::SugarStateOnly.procedure)
    harness.feed("one")
    harness.feed("two")

    assert_equal({ seen: %w[one two] }, harness.state)
    harness.replay_equals!
  end

  def test_a_channels_body_still_declares_a_full_form
    assert_nil DoNothingTwin.procedure, "a full form generates no procedure"
    assert_equal [DoNothingProcedure], DoNothingTwin.spec.procedures.map(&:klass)
  end

  def test_register_factory_takes_a_short_form_of_either_kind
    runner = Flezha::Runner.new("test-ns")
                           .register_factory(Fixtures::SugarDoNothing)
                           .register_factory(Fixtures::SugarWorkflow)

    assert_equal %w[sugar_do_nothing sugar_workflow], declared(runner)

    runner.unregister_factory(Fixtures::SugarWorkflow)
    assert_equal %w[sugar_do_nothing], declared(runner)
  end

  def test_unregistering_something_never_registered_is_refused
    runner = Flezha::Runner.new("test-ns").register_factory(Fixtures::SugarDoNothing)

    error = assert_raises(Flezha::Error) { runner.unregister_factory(Fixtures::SugarWorkflow) }
    assert_match(/not registered/, error.message)
  end

  def test_a_run_body_beside_a_channels_body_is_refused
    both = Class.new(Flezha::StreamOperative) do
      factory_id "both"
      channels { |c| c.input("input").into(DoNothingProcedure) }
      def run(input, _ctx) = input
    end

    error = refused(both)
    assert_equal ["ShortFormWithTopology"], error.refusals
    assert error.defect.start_with?("ShortFormWithTopology"), error.defect
  end

  def test_a_short_form_with_no_factory_id_is_refused_rather_than_derived
    derived = Class.new(Flezha::StreamOperative) do
      sink "out"
      def run(input, _ctx) = input
    end

    assert_includes refused(derived).message, "factory_id"
  end

  def test_a_malformed_short_form_is_refused_before_its_procedure_is_generated
    derived = Class.new(Flezha::StreamOperative) do
      sink "out"
      def run(input, _ctx) = input
    end
    chained = Class.new(Flezha::WorkflowOperative) do
      factory_id "chained"
      loop_mode :chained
      def run(input, _ctx) = input
    end

    assert_includes assert_raises(Flezha::DeclarationError) { derived.procedure }.message,
                    "factory_id"
    assert_includes assert_raises(Flezha::DeclarationError) { chained.procedure }.message,
                    "loop_mode"
  end

  def test_an_operative_with_neither_body_is_refused_naming_both
    [Flezha::StreamOperative, Flezha::WorkflowOperative].each do |base|
      bodiless = Class.new(base) { factory_id "bodiless" }

      message = refused(bodiless).message
      assert_includes message, "`channels`"
      assert_includes message, "`run`"
    end
  end

  def test_a_class_level_setting_on_a_short_form_is_refused
    chained = Class.new(Flezha::WorkflowOperative) do
      factory_id "chained"
      loop_mode :chained
      def run(input, _ctx) = input
    end

    assert_includes refused(chained).message, "loop_mode"
  end

  def test_a_short_form_key_or_a_procedure_setting_on_a_full_form_is_refused_naming_it
    { sink: "out", input: "ticks", codec: :text, clock: :logical }.each do |key, value|
      full = Class.new(Flezha::StreamOperative) do
        factory_id "full"
        channels { |c| c.input("input").into(DoNothingProcedure) }
        public_send(key, value)
      end

      message = refused(full).message
      assert_includes message, "`#{key}`"
      assert_includes message, "`channels`"
    end
  end

  def test_a_short_form_hands_its_procedure_settings_to_the_generated_procedure
    keyed = Class.new(Flezha::StreamOperative) do
      factory_id "keyed_short"
      entity_keys "a", "b"
      mailbox_capacity 8
      def entities_for(_input) = ["a"]
      def run(input, _ctx) = input
    end

    Flezha::Runner.new("test-ns").register_factory(keyed)
    definition = keyed.procedure.definition
    assert_equal %w[a b], definition.entity_keys
    assert_equal 8, definition.mailbox_capacity
    procedure = decl(keyed)["procedures"].first
    assert_equal ["keyed_short", "static", 8, true],
                 procedure.values_at("name", "entity_mode", "mailbox_capacity", "entities_for_overridden")
  end

  def test_a_duplicate_factory_id_across_kinds_is_refused_by_name
    twin = Class.new(Flezha::WorkflowOperative) do
      factory_id "sugar_do_nothing"
      sink "twin_sink"
      def run(input, _ctx) = input
    end
    registry = Flezha::Registry.new
    registry.add(Fixtures::SugarDoNothing.spec)

    error = assert_raises(Flezha::Error) { registry.add(twin.spec) }
    assert error.message.start_with?("DuplicateFactoryId"), error.message
    assert_includes error.message, "sugar_do_nothing"
  end

  def test_a_sink_on_a_procedure_is_refused_naming_the_operative_bases
    error = assert_raises(Flezha::DeclarationError) do
      Class.new(Flezha::Procedure) { sink "out" }
    end

    assert_includes error.message, "Flezha::StreamOperative"
    assert_includes error.message, "Flezha::WorkflowOperative"
  end

  def test_registering_a_procedure_is_refused
    error = assert_raises(Flezha::Error) do
      Flezha::Runner.new("test-ns").register_factory(DoNothingProcedure)
    end
    assert error.message.start_with?("UnknownOperativeKind"), error.message
  end
end
