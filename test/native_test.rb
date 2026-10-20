# frozen_string_literal: true

require_relative "test_helper"

# The compiled veneer, driven from Ruby. What the Rust-side arm of
# `flz_core_ffi/tests/abi_scenario.rs` cannot see: that the extension loads
# under this Ruby, that the exception classes are the ones a host rescues, and
# that a blocking call really does let other Ruby threads run.
class NativeTest < Minitest::Test
  def setup
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?
  end

  def test_the_extension_speaks_the_schema_the_gem_was_built_against
    assert_equal Flezha::ACTIVATION_SCHEMA_VERSION, Flezha::Native.schema_version
  end

  def test_a_call_before_the_context_exists_names_what_the_caller_skipped
    error = assert_raises(Flezha::Error) { Flezha::Native.poll(1) }
    assert_equal "internal.uncatalogued", error.code
    assert_includes error.details["reason"], "context_new"
  end

  # The extension's table against the C ABI fixture, the loaded module and the
  # guarded wrapper. Every row is a function the module defines and names an
  # entry point `abi_surface.txt` lists, so the veneer adds no kernel path of
  # its own; and `Core` reaches every row, so an entry point added to the veneer
  # and not to `ENTRY_POINTS` is not a method that quietly does not exist on
  # `Core`, and one removed is not a delegation that raises `NoMethodError` at
  # the first call.
  def test_the_extension_exports_the_surface_the_gem_wraps
    listed = File.readlines(Flezha::TestSupport.contract_file("test/fixtures/abi_surface.txt"), chomp: true)
                 .map(&:strip).reject { |line| line.empty? || line.start_with?("#") }
    surface = Flezha::Native.native_surface

    refute_empty surface
    surface.each do |name, abi, _blocking|
      assert_includes listed, abi, "#{name} names #{abi}, which flz_core_ffi does not export"
      assert_respond_to Flezha::Native, name
    end
    exported = surface.map { |row| row.first.to_sym }
    # The ones the wrapper reaches other than by delegation: two are class
    # level (`create` negotiates the schema, `free` releases it),
    # `native_surface` is this assertion's own subject, `error_document` is
    # reached through `Core.error_document`, `knob` through `Env` and
    # `response_decode` through `Response.decode`.
    wrapped = Flezha::Core::ENTRY_POINTS +
              %i[schema_version context_new context_free native_surface error_document knob response_decode]

    assert_empty exported - wrapped, "the veneer exports entry points Core cannot reach"
    assert_empty wrapped - exported, "Core delegates to entry points the veneer does not export"
  end

  # The GVL is released around every blocking core call, through one function.
  # A call parked in the core with the lock held would freeze the whole VM,
  # fetchers included; `poll` is the one a test can park in without a
  # controller.
  def test_a_blocked_poll_does_not_stop_another_ruby_thread
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)
    ticks = 0
    running = true
    ticker = Thread.new do
      ticks += 1 while running
    end

    parked = Thread.new { Flezha::Native.poll(400) }
    assert_nil parked.value, "the poll timed out rather than finding work"

    running = false
    ticker.join

    assert_operator ticks, :>, 1_000,
                    "another Ruby thread ran only #{ticks} times while a poll was parked; the " \
                    "GVL was not released"
  ensure
    Flezha::Native.context_free
  end

  def test_the_pool_supervisor_releases_the_gvl_too
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)
    Flezha::Native.pool_open(1)
    ticks = 0
    running = true
    ticker = Thread.new { ticks += 1 while running }

    parked = Thread.new { Flezha::Native.pool_supervise }
    assert_equal 0, parked.value, "an idle pool's supervisor answers idle within its wait"

    running = false
    ticker.join

    assert_operator ticks, :>, 1_000
  ensure
    Flezha::Native.context_free
  end

  # The poll timeout is the documented interrupt granularity: the ABI has no
  # per-thread wake, so a bounded poll is what makes a runner close observable.
  def test_a_poll_returns_within_its_timeout
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)
    poll_ms = Flezha::Native.pool_open(1)[2]
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Flezha::Native.poll(poll_ms)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_operator elapsed, :<, (poll_ms / 1000.0) * 3
  ensure
    Flezha::Native.context_free
  end

  # A declaration the core cannot honour is refused at declaration time, with
  # an error naming the field — never accepted and degraded silently.
  def test_the_core_refuses_a_declaration_it_cannot_honour
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)

    error = assert_raises(Flezha::Error) do
      Flezha::Native.declare_class(JSON.generate({ "factory_id" => "x", "channels" => [],
                                                   "tasklets" => [] }))
    end

    assert_equal "topology.declaration_unreadable", error.code
    assert_includes error.details["reason"], "sdk_lang", "a class that declares no language is refused"
  ensure
    Flezha::Native.context_free
  end

  # The checkpoint acknowledgements the core accepts. Asserted against the
  # core, which reads the tree and refuses a malformed one.
  def checkpoint_class(factory_id, ack)
    tasklet = Class.new(Flezha::Tasklet) do
      tasklet_name "#{factory_id}_tasklet"
      def run(input, ctx) = ctx.emit(input)
    end
    relay = Class.new(Flezha::Relay)
    relay.factory_id factory_id
    relay.run flow: { exactly: { effectively_once: { ack: ack } } }
    relay.channels { |c| c.input("input").into(tasklet).keyed.output("sink") }
    Flezha::Graph.to_host_class_decl(relay.spec)
  end

  def test_holding_outputs_at_the_checkpoint_registers_with_its_delay
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)

    decl = checkpoint_class("outputs_only", { checkpoint: { outputs: :at_checkpoint, max_delay_ms: 1500 } })

    assert_equal({ "checkpoint" => { "outputs" => "at_checkpoint", "max_delay_ms" => 1500 } },
                 decl.dig("run", "flow", "exactly", "effectively_once", "ack"))
    Flezha::Native.declare_class(JSON.generate(decl))
    assert_equal "checkpoint", JSON.parse(Flezha::Native.check_class(JSON.generate(decl)))["ack_granularity"]
  ensure
    Flezha::Native.context_free
  end

  def test_a_bare_checkpoint_registers_with_no_cadence
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)

    decl = checkpoint_class("ack_only", :checkpoint)

    assert_equal "checkpoint", decl.dig("run", "flow", "exactly", "effectively_once", "ack")
    Flezha::Native.declare_class(JSON.generate(decl))
    assert_equal "checkpoint", JSON.parse(Flezha::Native.check_class(JSON.generate(decl)))["ack_granularity"]
  ensure
    Flezha::Native.context_free
  end

  # The read-only half of the declaration: a record the core cannot honour is
  # refused with the vocabulary every SDK renders, and nothing is registered.
  def test_a_refused_record_answers_the_verdict_and_registers_nothing
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)

    keyed = Class.new(Flezha::Tasklet) do
      tasklet_name "check_keyed"
      def run(input, ctx) = ctx.emit(input)
    end
    relay = Class.new(Flezha::Relay)
    relay.factory_id "check_dup_key"
    relay.channels { |c| c.input("input").key_by { "a" }.key_by { "b" }.into(keyed) }

    json = JSON.generate(Flezha::Graph.to_host_class_decl(relay.spec))
    error = assert_raises(Flezha::Error) { Flezha::Native.check_class(json) }

    assert_equal "topology.duplicate_key_by", error.code
    # Every issue rides the error as data too.
    assert_equal ["topology.duplicate_key_by"], Flezha::TestSupport.codes(error)
    assert_equal "topology.duplicate_key_by", assert_raises(Flezha::Error) { Flezha::Native.declare_class(json) }.code
  ensure
    Flezha::Native.context_free
  end

  def test_a_ruby_declaration_is_accepted_and_carries_the_allocated_language_byte
    Flezha::Native.context_new(Flezha::ACTIVATION_SCHEMA_VERSION)

    echo = Class.new(Flezha::Tasklet) do
      tasklet_name "native_echo"
      def run(input, ctx) = ctx.emit(input)
    end
    relay = Class.new(Flezha::Relay)
    relay.factory_id "native_echo_op"
    relay.channels { |c| c.input("input").into(echo).output("out") }

    decl = Flezha::Graph.to_host_class_decl(relay.spec)
    assert_equal 5, decl["sdk_lang"]
    assert_equal 5, decl["change_format_id"], "MessagePack, shared with every MessagePack SDK"

    Flezha::Native.declare_class(JSON.generate(decl))
  ensure
    Flezha::Native.context_free
  end
end
