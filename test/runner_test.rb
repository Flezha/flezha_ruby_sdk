# frozen_string_literal: true

require_relative "test_helper"

Flezha::TestSupport.require_test_app("fixtures")

# The entry point's contract: declare, warm up, then serve — and the JIT
# decision the user can always take back.
class RunnerTest < Minitest::Test
  class Echo < Flezha::Tasklet
    tasklet_name "runner_echo"
    def run(input, ctx) = ctx.emit(input)
  end

  class EchoOperator < Flezha::Operator
    factory_id "runner_echo_op"
    channels { |c| c.input("input").into(Echo).sink("out") }
  end

  def boot_recording
    core = Flezha::TestSupport::FakeCore.new
    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperator)
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
    second = Class.new(Flezha::Operator)
    second.factory_id "runner_second_op"
    second.channels { |c| c.input("in").into(Echo, "second_node").sink("o") }

    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperator).register_factory(second)
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
    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperator)
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

    error = assert_raises(Flezha::Error) { runner.boot(core: Flezha::TestSupport::FakeCore.new) }
    assert_equal "runtime.no_operators", error.code
  end

  def test_an_unknown_executor_is_refused_by_name
    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperator).executor(:ractors)

    Flezha::TestSupport.without_a_terminal do
      error = assert_raises(Flezha::Error) { runner.boot(core: Flezha::TestSupport::FakeCore.new) }
      assert_equal ["usage.unknown_word", "ractors", %w[threads fibers]],
                   [error.code, error.details["value"], error.details["allowed"]]
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

  def test_the_users_environment_decision_wins_over_the_default
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

  def decl(operator) = Flezha::Graph.to_host_class_decl(operator.spec)

  def refused(operator)
    assert_raises(Flezha::Error) { Flezha::Runner.new("test-ns").register_factory(operator) }
  end

  # The short forms under test are the test app's own, the classes the
  # end-to-end run registers. Each twin is the full form an author would write
  # instead.
  class DoNothingTasklet < Fixtures::PassThrough
    tasklet_name "sugar_do_nothing"
  end

  class DoNothingTwin < Flezha::Operator
    factory_id "sugar_do_nothing"
    channels { |c| c.input("input", codec: :text).into(DoNothingTasklet).sink("sugar_do_nothing_echo") }
  end

  class StepsTasklet < Fixtures::PassThrough
    tasklet_name "sugar_steps"
  end

  class StepsTwin < Flezha::Operator
    factory_id "sugar_steps"
    run :steps
    durable Fixtures::FIXTURE_DURABLE
    channels { |c| c.input("order", codec: :text).into(StepsTasklet).sink("sugar_steps_placed") }
  end

  class PerInputTasklet < Fixtures::PassThrough
    tasklet_name "sugar_steps_per_input"
  end

  class PerInputTwin < Flezha::Operator
    factory_id "sugar_steps_per_input"
    run steps: { each: :per_input }
    durable Fixtures::FIXTURE_DURABLE
    channels { |c| c.input("input", codec: :text).into(PerInputTasklet).sink("sugar_steps_per_input_out") }
  end

  class StateOnlyTasklet < Flezha::Tasklet
    tasklet_name "sugar_state_only"
    def init_state(_cell_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]
    def run(_input, _ctx) = nil
  end

  class StateOnlyTwin < Flezha::Operator
    factory_id "sugar_state_only"
    channels { |c| c.input("ticks", codec: :text).into(StateOnlyTasklet) }
  end

  def test_a_flow_short_form_is_its_hand_written_twin
    short = Fixtures::SugarDoNothing

    assert_equal decl(DoNothingTwin), decl(short)
    assert_nil short.spec.run
    assert_equal "sugar_do_nothing", short.tasklet.tasklet_name,
                 "the generated tasklet is named after the factory id, verbatim"
    assert_equal "héllo", short.spec.channels.first.decode.call("héllo".b),
                 "the entry decodes text"
    refute_operator short, :<, Flezha::Tasklet, "a short form is an operator, not a tasklet"
  end

  def test_a_steps_short_form_is_equal_to_its_twin
    short = Fixtures::SugarSteps

    assert_equal decl(StepsTwin), decl(short)
    assert_equal "steps", short.spec.run
    assert_equal %w[order sugar_steps_placed], short.spec.channels.map(&:name)
  end

  def test_a_steps_short_form_keeps_its_loop_shape
    short = Fixtures::SugarStepsPerInput

    assert_equal decl(PerInputTwin), decl(short)
    assert_equal({ "steps" => { "each" => "per_input" } }, decl(short)["run"])
    assert_equal %w[sugar_steps_per_input],
                 declared(Flezha::Runner.new("test-ns").register_factory(short))
  end

  def test_a_short_form_with_no_sink_declares_its_entry_and_its_tasklet_only
    short = Fixtures::SugarStateOnly

    assert_equal decl(StateOnlyTwin), decl(short)
    assert_equal [%w[ticks entry]], decl(short)["channels"].map { |c| [c["name"], c["kind"]] },
                 "no plugin channel"
    assert_equal ["sugar_state_only"], short.spec.tasklets.map(&:name)
    assert_equal %w[sugar_state_only], declared(Flezha::Runner.new("test-ns").register_factory(short)),
                 "an entry name and a codec need no sink"
  end

  def test_the_core_accepts_a_sinkless_short_form
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    derived = Flezha::TestSupport.validate(Fixtures::SugarStateOnly.spec)

    assert_equal [%w[ticks entry]], derived["channels"].map { |c| [c["name"], c["kind"]] }
  end

  def test_the_generated_tasklet_runs_the_class_s_run_body_and_journals_its_changes
    harness = Flezha::Testing::Harness.new(Fixtures::SugarStateOnly.tasklet)
    harness.feed("one")
    harness.feed("two")

    assert_equal({ seen: %w[one two] }, harness.state)
    harness.replay_equals!
  end

  def test_a_channels_body_still_declares_a_full_form
    assert_nil DoNothingTwin.tasklet, "a full form generates no tasklet"
    assert_equal [DoNothingTasklet], DoNothingTwin.spec.tasklets.map(&:klass)
  end

  def test_register_factory_takes_a_short_form_of_either_kind
    runner = Flezha::Runner.new("test-ns")
                           .register_factory(Fixtures::SugarDoNothing)
                           .register_factory(Fixtures::SugarSteps)

    assert_equal %w[sugar_do_nothing sugar_steps], declared(runner)

    runner.unregister_factory(Fixtures::SugarSteps)
    assert_equal %w[sugar_do_nothing], declared(runner)
  end

  def test_unregistering_after_start_is_refused
    core = Flezha::TestSupport::FakeCore.new
    runner = Flezha::Runner.new("test-ns").register_factory(EchoOperator)
    Flezha::TestSupport.without_a_terminal do
      Process.stub(:warmup, -> {}) do
        runner.boot(core: core)
        error = assert_raises(Flezha::Error) { runner.unregister_factory(EchoOperator) }

        assert_equal ["runtime.factory_in_service", EchoOperator.to_s], [error.code, error.details["module"]]
      ensure
        runner.stop
        runner.drain
      end
    end
  end

  def test_unregistering_something_never_registered_is_refused
    runner = Flezha::Runner.new("test-ns").register_factory(Fixtures::SugarDoNothing)

    error = assert_raises(Flezha::Error) { runner.unregister_factory(Fixtures::SugarSteps) }
    assert_equal ["runtime.class_not_registered", { "factory" => "sugar_steps" }], [error.code, error.details]
  end

  def test_a_run_body_beside_a_channels_body_is_refused
    both = Class.new(Flezha::Operator) do
      factory_id "both"
      channels { |c| c.input("input").into(DoNothingTasklet) }
      def run(input, ctx) = ctx.emit(input)
    end

    error = refused(both)
    assert_equal ["topology.short_form_with_topology", { "factory_id" => "both" }], [error.code, error.details]
    assert_includes error.detailed_message, "(topology.short_form_with_topology)"
  end

  def test_a_short_form_with_no_factory_id_is_refused_rather_than_derived
    derived = Class.new(Flezha::Operator) do
      sink "out"
      def run(input, ctx) = ctx.emit(input)
    end

    error = refused(derived)
    assert_equal "topology.missing_factory_id", error.code
    assert_includes error.message, "declares no factory id"
  end

  def test_a_malformed_short_form_is_refused_before_its_tasklet_is_generated
    derived = Class.new(Flezha::Operator) do
      sink "out"
      def run(input, ctx) = ctx.emit(input)
    end
    both = Class.new(Flezha::Operator) do
      factory_id "both"
      channels { |c| c.input("in") }
      def run(input, ctx) = ctx.emit(input)
    end

    assert_equal "topology.missing_factory_id", assert_raises(Flezha::Error) { derived.tasklet }.code
    assert_equal "topology.short_form_with_topology", assert_raises(Flezha::Error) { both.tasklet }.code
  end

  def test_an_operator_with_neither_body_is_refused_naming_both
    [nil, :steps].each do |setting|
      bodiless = Class.new(Flezha::Operator) do
        factory_id "bodiless"
        run(setting) if setting
      end

      message = refused(bodiless).message
      assert_includes message, "`channels`"
      assert_includes message, "`run`"
    end
  end

  def test_a_short_form_takes_the_class_settings_the_full_form_takes
    short = Class.new(Flezha::Operator) do
      factory_id "kept_short"
      run steps: { each: :per_input, on_failure: :keep_progress }
      durable local: { sleep_after: 2_000 }
      max_duration_ms 60_000
      def run(input, ctx) = ctx.emit(input)
    end
    record = decl(short)

    assert_equal [{ "steps" => { "each" => "per_input", "on_failure" => "keep_progress" } },
                  { "local" => { "sleep_after" => 2_000 } }, 60_000],
                 record.values_at("run", "durable", "max_duration_ms")
  end

  def test_a_short_form_key_or_a_tasklet_setting_on_a_full_form_is_refused_naming_it
    { sink: "out", input: "ticks", codec: :text, idle_ttl_seconds: 60 }.each do |key, value|
      full = Class.new(Flezha::Operator) do
        factory_id "full"
        channels { |c| c.input("input").into(DoNothingTasklet) }
        public_send(key, value)
      end

      error = refused(full)
      assert_equal ["topology.short_form_field_on_full_form", ["`#{key}`"]], [error.code, error.details["fields"]]
    end
  end

  def test_a_short_form_hands_its_tasklet_settings_to_the_generated_tasklet
    keyed = Class.new(Flezha::Operator) do
      factory_id "keyed_short"
      cell_keys "a", "b"
      mailbox_capacity 8
      def cells_for(_input) = ["a"]
      def run(input, ctx) = ctx.emit(input)
    end

    Flezha::Runner.new("test-ns").register_factory(keyed)
    definition = keyed.tasklet.definition
    assert_equal %w[a b], definition.cell_keys
    assert_equal 8, definition.mailbox_capacity
    tasklet = decl(keyed)["tasklets"].first
    assert_equal ["keyed_short", "static", 8, true],
                 tasklet.values_at("name", "cell_mode", "mailbox_capacity", "cells_for_overridden")
  end

  def test_a_duplicate_factory_id_across_kinds_is_refused_by_name
    twin = Class.new(Flezha::Operator) do
      factory_id "sugar_do_nothing"
      run :steps
      sink "twin_sink"
      def run(input, ctx) = ctx.emit(input)
    end
    registry = Flezha::Registry.new
    registry.add(Fixtures::SugarDoNothing.spec)

    error = assert_raises(Flezha::Error) { registry.add(twin.spec) }
    assert_equal ["topology.duplicate_factory_id", { "factory_id" => "sugar_do_nothing" }],
                 [error.code, error.details]
    assert_includes error.message, "sugar_do_nothing"
  end

  def test_a_sink_on_a_tasklet_is_refused_naming_the_operator_bases
    error = assert_raises(Flezha::Error) do
      Class.new(Flezha::Tasklet) { sink "out" }
    end

    assert_equal ["topology.operator_field_on_tasklet", ["`sink`"]], [error.code, error.details["fields"]]
    assert_includes error.message, "Flezha::Operator"
  end

  def test_registering_a_tasklet_is_refused
    error = assert_raises(Flezha::Error) do
      Flezha::Runner.new("test-ns").register_factory(DoNothingTasklet)
    end
    assert_equal "topology.no_operator_exported", error.code
  end
end
