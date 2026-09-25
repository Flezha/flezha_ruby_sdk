# frozen_string_literal: true

require_relative "test_helper"

# What this SDK's authoring surface answers, and what it hands the core.
#
# The topology itself is judged once, in `flz_core::host::register::check`, and
# the reference refusals are pinned by `test/fixtures/reference_topologies.txt`
# — not here. What is left here is the ergonomics: which shapes the fluent
# builder BUILDS, the two defects a record cannot carry (a Ruby class that does
# not define the method its declaration promises, a plugin sink on a channel no
# tasklet produces on), and that the core's verdict reaches an author as this
# SDK's own refusal.
class AuthoringTest < Minitest::Test
  def refusal(&block)
    assert_raises(Flezha::DeclarationError, &block)
  end

  def pass_through(name)
    Class.new(Flezha::Tasklet) do
      tasklet_name name
      def run(input, _ctx) = input
    end
  end

  def operator(factory, base: Flezha::UnboundedOperator, &channels)
    klass = Class.new(base)
    klass.factory_id factory
    klass.channels(&channels)
    klass
  end

  # The submission needs a live core; without the compiled veneer the case says
  # so rather than passing vacuously.
  def validate(spec)
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    Flezha::TestSupport.validate(spec)
  end

  def test_a_tasklet_without_a_run_method_is_refused_by_name
    klass = Class.new(Flezha::Tasklet) { tasklet_name "no_body" }

    message = refusal { klass.definition }.message
    assert_includes message, "no_body"
    assert_includes message, "`run`"
  end

  class StillProcess < Flezha::Tasklet
    def process(input, _ctx) = input
  end

  def test_a_tasklet_defining_process_and_no_run_is_refused_naming_run
    message = refusal { StillProcess.definition }.message

    assert_includes message, "AuthoringTest::StillProcess"
    assert_includes message, "`run`"
  end

  def test_a_tasklet_s_body_is_its_run_method
    upcase = Class.new(Flezha::Tasklet) do
      tasklet_name "upcase"
      def run(input, _ctx) = input.upcase
    end

    assert_equal ["A"], Flezha::Testing::Harness.new(upcase).feed("a").outputs
  end

  def test_a_fold_with_no_state_to_fold_into_is_refused
    klass = Class.new(Flezha::Tasklet) do
      tasklet_name "fold_only"
      def apply_change(state, change) = state
      def run(input, _ctx) = input
    end

    assert_includes refusal { klass.definition }.message, "initial_state"
  end

  def test_the_two_cell_modes_are_exclusive
    klass = Class.new(Flezha::Tasklet) do
      tasklet_name "both_modes"
      cell_keys "a"
      dynamic_cells
      def cells_for(_input) = ["a"]
      def run(input, _ctx) = input
    end

    assert_includes refusal { klass.definition }.message, "exclusive"
  end

  def test_a_state_that_cannot_be_encoded_is_refused_at_registration
    klass = Class.new(Flezha::Tasklet) do
      tasklet_name "unencodable"
      def initial_state(_key) = { handler: -> {} }
      def run(input, _ctx) = input
    end

    assert_raises(Flezha::UnrepresentableValueError) { klass.definition }
  end

  # A clock is a promise about a method, and a record carries the clock but not
  # the class: only this SDK can see that the method is missing.
  def test_an_event_time_clock_without_its_extractor_is_refused_here
    klass = Class.new(Flezha::Tasklet) do
      tasklet_name "no_extractor"
      clock :event_time
      def run(input, _ctx) = input
    end
    graph = operator("no_extractor_op") { |c| c.input("in").into(klass).sink }

    # No core at all, which is the proof it was refused before the record
    # could reach one.
    assert_includes refusal { Flezha::Graph.validate(graph.spec, nil) }.message,
                    "declares an event_time clock but no event_time method"
  end

  # The other refusal the builder keeps: a channel with no producer has no
  # value to serialize, and the core reads a sink as the terminal of the
  # channel it sits on — it cannot see that this one is fed by the controller.
  def test_a_plugin_sink_on_an_entry_channel_is_refused_here
    klass = operator("entry_sink") { |c| c.input("in").sink }

    assert_includes refusal { klass.spec }.message, "a plugin sink is fed by a tasklet"
  end

  # `validate` IS the submission: the record crosses the ABI and the core's
  # verdict comes back as this SDK's own refusal, in the core's vocabulary.
  def test_the_cores_verdict_reaches_the_author_as_a_declaration_refusal
    target = Class.new(Flezha::Tasklet) do
      tasklet_name "keyed"
      dynamic_cells
      def run(input, _ctx) = input
    end
    klass = operator("dup_key_by") do |c|
      c.input("in").key_by { "a" }.key_by { "b" }.into(target)
    end

    error = refusal { validate(klass.spec) }

    assert error.message.start_with?("dup_key_by: topology rejected [DuplicateKeyBy]:\n  - "), error.message
    assert_includes error.message, "declares .key_by(...) twice before one `.into(...)`"
    # And the names as DATA: the core hands the verdict out beside the rendered
    # message, so nothing here reads them out of the prose.
    assert_equal ["DuplicateKeyBy"], error.refusals
  end

  # The three checkpoint options each write their own raw flag now — the
  # granularity is the core's to imply — so none of them may clear another.
  def test_the_checkpoint_options_compose_in_any_order_and_none_clears_another
    klass = operator("compose") do |c|
      c.effectively_once
      c.outputs_at_checkpoint(250)
      c.checkpoint_ack
      c.checkpoint_every(4)
      c.input("in").into(pass_through("p")).keyed.sink
    end
    decl = Flezha::Graph.to_host_class_decl(klass.spec)

    assert_equal true, decl["checkpoint_ack"]
    assert_equal 4, decl["checkpoint_every"]
    assert_equal "at_checkpoint", decl["checkpoint_outputs"]
    assert_equal 250, decl["checkpoint_max_delay_ms"]
    refute_includes decl.keys, "ack_granularity", "the granularity is derived, never declared"
  end

  def test_a_bounded_declaring_no_channels_is_refused_naming_them
    klass = Class.new(Flezha::BoundedOperator)
    klass.factory_id "bodiless"

    error = refusal { klass.spec }

    assert_equal "bodiless", error.factory_id
    assert_includes error.message, "`channels`"
  end

  # The core's own check, reached through the one submission path.
  def test_a_bounded_declaring_no_tasklet_is_refused
    klass = Class.new(Flezha::BoundedOperator)
    klass.factory_id "empty"
    klass.channels { |_c| nil }

    assert_includes refusal { validate(klass.spec) }.message, "at least one tasklet"
  end

  def test_two_unnamed_mentions_of_one_tasklet_are_one_node
    shared = pass_through("shared")
    upstream = pass_through("upstream")
    klass = operator("diamond") do |c|
      edge = c.input("input").into(upstream)
      edge.named("left").into(shared)
      edge.named("right").into(shared)
    end
    validate(klass.spec)

    assert_equal 2, klass.spec.tasklets.size
    # And one edge: `named` renames the one channel `upstream` produces on, so
    # the second bare mention re-declares the first rather than wiring `shared`
    # a second time off it.
    assert_equal [1], klass.spec.channels[1].consumers.map(&:tasklet)
  end

  def test_a_second_tasklet_claiming_one_node_identity_is_refused
    first = pass_through("same_name")
    second = pass_through("same_name")
    klass = operator("collide") do |c|
      edge = c.input("input").into(first)
      edge.named("hop").into(second)
    end

    error = refusal { klass.spec }

    # The one refusal this builder raises under a core name reads the way the core's do.
    assert_equal ["NodeIdentityCollision"], error.refusals
    assert error.message.start_with?(
      "collide: same_name: topology rejected [NodeIdentityCollision]:\n  - two different tasklets"
    ), error.message
  end

  def test_an_undeclared_class_never_reaches_the_registry
    # Registration is explicit: defining a class does not register it.
    defined_but_unregistered = pass_through("never_registered")
    registry = Flezha::Registry.new

    assert_nil registry.get("never_registered")
    refute_nil defined_but_unregistered.definition
  end

  # `named` is honoured on the inter-task path and was dropped on the plugin
  # path, so `named("input").sink("relay_out")` produced a channel called
  # `relay_out` — and the channel name is what a forwarded frame carries and
  # what selects the receiving operator's entry channel.
  def test_a_named_edge_terminating_on_a_sink_names_the_channel_too
    klass = operator("named_sink") do |c|
      c.input("input").into(pass_through("relay")).sink("in")
    end

    assert_equal %w[input in], klass.spec.channels.map(&:name)
  end

  # `sdk-graph-authoring`: the no-argument `sink` names nothing, so the channel
  # keeps the PRODUCER NODE's name — the derivation a channel with no sink at
  # all would take.
  def test_an_unnamed_edge_terminating_on_a_sink_takes_the_producing_nodes_name
    klass = operator("unnamed_sink") do |c|
      c.input("input").into(pass_through("plain")).sink
    end

    assert_equal %w[input plain], klass.spec.channels.map(&:name)
  end
  # -- one output channel per producing tasklet -----------------------------
  #
  # The model Rust and Java carry: a tasklet node owns ONE output channel,
  # created on first wiring and reused after. A held edge wired twice appends a
  # second consumer to it — it does not declare a second channel claiming the
  # same producer, which the core resolves by first match
  # (`host/register.rs` `tasklet_wiring`) and so silently truncates.
  #
  # Read off the DERIVED declaration: the consumers and the plugin kind are
  # what the core made of the chains this builder recorded.

  def channels_of(klass)
    validate(klass.spec)["channels"].map do |channel|
      [channel["name"], channel["kind"], channel["producer"],
       channel["consumers"].map { |edge| edge["tasklet"] }, channel["sink"]]
    end
  end

  def test_a_held_edge_wired_twice_is_one_channel_with_two_consumers
    b = pass_through("b")
    c_task = pass_through("c")
    klass = operator("fan_out") do |c|
      edge = c.input("in").into(pass_through("producer"))
      edge.into(b)
      edge.into(c_task)
    end

    assert_equal [["in", "entry", nil, [0], nil], ["producer", "inter_task", 0, [1, 2], nil]],
                 channels_of(klass)
  end

  def test_branch_and_two_into_statements_build_the_identical_declaration
    producer = pass_through("producer")
    b = pass_through("b")
    c_task = pass_through("c")
    separate = operator("separate") do |c|
      edge = c.input("in").into(producer)
      edge.into(b)
      edge.into(c_task)
    end
    branched = operator("branched") do |c|
      c.input("in").into(producer).branch(->(arm) { arm.into(b) }, ->(arm) { arm.into(c_task) })
    end

    assert_equal channels_of(separate), channels_of(branched)
  end

  def test_sink_returns_the_edge_so_a_consumer_wires_behind_it
    klass = operator("hybrid") do |c|
      c.input("in").into(pass_through("producer"))
       .sink("hybrid_indicator").into(pass_through("consumer"))
    end

    assert_equal [["in", "entry", nil, [0], nil],
                  ["hybrid_indicator", "inter_task", 0, [1], "hybrid_indicator"]],
                 channels_of(klass)
  end

  def test_the_sink_names_the_channel_and_the_no_argument_form_does_not
    named = operator("sink_named") do |c|
      c.input("in").into(pass_through("producer")).sink("items_output")
    end
    derived = operator("sink_derived") do |c|
      c.input("in").into(pass_through("producer")).sink
    end

    assert_equal %w[in items_output], channels_of(named).map(&:first)
    assert_equal %w[in producer], channels_of(derived).map(&:first)
  end

  def test_a_channel_named_twice_is_refused_naming_both
    klass = operator("named_twice") do |c|
      c.input("in").into(pass_through("producer")).named("a").sink("b")
    end

    error = assert_raises(Flezha::DeclarationError) { validate(klass.spec) }
    assert_match(/ChannelNamedTwice/, error.message)
    assert_match(/both name it/, error.message)
  end

  def test_a_channel_option_with_nothing_wired_after_it_still_reaches_the_channel
    renamed = operator("trailing_named") { |c| c.input("in").into(pass_through("producer")).named("renamed") }
    keyed = operator("trailing_keyed") { |c| c.input("in").into(pass_through("producer")).keyed }

    assert_equal %w[in renamed], channels_of(renamed).map(&:first)
    error = assert_raises(Flezha::DeclarationError) { validate(keyed.spec) }
    assert_match(/KeyedChannelWithoutPluginWire/, error.message)
  end

  def test_two_channels_naming_one_sink_is_refused
    klass = operator("one_sink_two_channels") do |c|
      c.input("in").into(pass_through("a")).sink("shared")
       .into(pass_through("b")).sink("shared")
    end

    error = assert_raises(Flezha::DeclarationError) { validate(klass.spec) }
    assert_match(/DuplicateSinkName/, error.message)
  end

  def test_a_transform_before_one_into_reaches_that_consumer_only
    b = pass_through("b")
    c_task = pass_through("c")
    klass = operator("per_edge_transform") do |c|
      edge = c.input("in").into(pass_through("producer"))
      edge.map { |value| "#{value}!" }.into(b)
      edge.into(c_task)
    end
    validate(klass.spec)

    assert_equal [[1, 1], [2, 0]],
                 klass.spec.channels[1].consumers.map { |e| [e.tasklet, e.transforms.size] }
  end

  # A re-declaration carrying neither transform nor key is the fan-in /
  # repeated-`into` case both references make a true no-op: one consumer edge,
  # not two, and not a refusal.
  def test_a_bare_re_declared_edge_is_a_no_op_not_a_second_edge
    target = pass_through("target")
    klass = operator("edge_redeclared_bare") do |c|
      edge = c.input("in")
      edge.into(target)
      edge.into(target)
    end
    derived = validate(klass.spec)

    assert_equal [[0, 0]],
                 klass.spec.channels[0].consumers.map { |e| [e.tasklet, e.transforms.size] }
    assert_equal [0], derived["channels"][0]["consumers"].map { |e| e["tasklet"] }
  end

  # What an author writes reaches the core whatever the kind, and a setting of
  # the other kind is refused there by its own name rather than dropped here.
  UNBOUNDED_SETTINGS_ON_A_BOUNDED = {
    "EffectivelyOnceOnBounded" => ->(c) { c.effectively_once },
    "CheckpointOnBounded" => ->(c) { c.checkpoint_every(500) },
    "CorrectionWindowOnBounded" => ->(c) { c.correction_window(60_000) }
  }.freeze

  def test_a_stream_setting_on_a_bounded_is_refused_by_name
    UNBOUNDED_SETTINGS_ON_A_BOUNDED.each do |name, write|
      step = pass_through("producer")
      klass = operator("unbounded_setting_bounded", base: Flezha::BoundedOperator) do |c|
        write.call(c)
        c.input("in").into(step)
      end

      assert_equal [name], refusal { validate(klass.spec) }.refusals
    end
  end

  def test_sync_durability_on_a_stream_is_refused_by_name
    step = pass_through("producer")
    klass = operator("sync_unbounded") { |c| c.input("in").into(step) }
    klass.sync_durability

    assert_equal ["SyncDurabilityOnUnbounded"], refusal { validate(klass.spec) }.refusals
  end

  def test_sleeping_defaults_to_persistence_on_a_bounded_only
    step = pass_through("step")
    sleeps = lambda do |base, persistent|
      klass = operator("sleep_default", base: base) { |c| c.input("in").into(step) }
      klass.persistent(persistent)
      Flezha::Graph.to_host_class_decl(klass.spec)["sleep_when_idle"]
    end

    assert_equal [true, false, false],
                 [sleeps.call(Flezha::BoundedOperator, true),
                  sleeps.call(Flezha::BoundedOperator, false),
                  sleeps.call(Flezha::UnboundedOperator, true)]
  end

  def test_sleeping_without_persistence_is_refused_by_name
    step = pass_through("step")
    [Flezha::UnboundedOperator, Flezha::BoundedOperator].each do |base|
      klass = operator("sleep_no_persist", base: base) { |c| c.input("in").into(step) }
      klass.persistent(false)
      klass.sleep_when_idle

      assert_includes refusal { validate(klass.spec) }.message, "SleepWithoutPersistence"
    end
  end

  # A mode outside its declared values is refused where the class body writes
  # it, on the full form and the short form alike.
  def refused_mode(setting, value)
    [->(c) { c.channels { |b| b.input("in") } }, ->(c) { c.define_method(:run) { |i, _| i } }]
      .map do |body|
        refusal do
          Class.new(Flezha::BoundedOperator) do
            factory_id "mode"
            body.call(self)
            public_send(setting, value)
          end
        end
      end
  end

  def test_a_loop_mode_outside_its_values_is_refused_where_it_is_written
    refused_mode(:loop_mode, :forever).each do |error|
      assert_includes error.message, "loop_mode :forever"
      assert_includes error.message, "`once`, `per_input`, `chained`"
    end
  end

  def test_a_rollback_mode_outside_its_values_is_refused_where_it_is_written
    refused_mode(:rollback_mode, :best_effort).each do |error|
      assert_includes error.message, "rollback_mode :best_effort"
      assert_includes error.message, "`all_or_nothing`, `keep_progress`"
    end
  end

  def test_the_retired_class_retry_and_wake_settings_are_not_declared
    %i[retry_attempts retry_backoff_ms barrier_retry_attempts wake_after_ms].each do |retired|
      assert_raises(NoMethodError) { Class.new(Flezha::BoundedOperator).public_send(retired, 5) }
    end
  end

  # -- merge and the latest-document router -----------------------------------
  #
  # A merge is recorded, never judged: each source's mapping is its own edge
  # transform, the key rides every source's chain after it, and the core names
  # a merge of too few or repeated sources.

  def tagged(tag) = ->(value) { { source: tag, value: value } }

  def test_a_merge_wires_one_node_with_one_edge_per_source_each_under_its_own_mapping
    target = pass_through("label")
    klass = operator("merged") do |c|
      c.merge.from(c.input("ticks"), &tagged("ticks")).from(c.input("quotes"), &tagged("quotes"))
       .key_by { |value| value[:value] }.into(target)
    end
    validate(klass.spec)

    assert_equal ["label"], klass.spec.tasklets.map(&:name)
    assert_equal [[0], [0]], klass.spec.channels.map { |channel| channel.consumers.map(&:tasklet) }
    ticks, quotes = klass.spec.channels.map { |channel| channel.consumers.first }
    assert_equal [[{ source: "ticks", value: "x" }], [{ source: "quotes", value: "x" }]],
                 [ticks, quotes].map { |edge| Flezha.apply_transforms(edge.transforms, "x") }
    assert_equal %w[x x], [ticks, quotes].map { |edge| edge.key_by.call({ value: "x" }) }
  end

  def test_a_merge_of_one_source_is_refused_as_too_few_sources
    klass = operator("merge_one") do |c|
      c.merge.from(c.input("in")) { |value| value }.into(pass_through("plain"))
    end

    assert_includes refusal { validate(klass.spec) }.refusals, "MergeTooFewSources"
  end

  def test_a_merge_naming_one_source_twice_is_refused_as_a_duplicate_source
    klass = operator("merge_twice") do |c|
      source = c.input("in")
      c.input("other")
      c.merge.from(source) { |value| value }.from(source) { |value| value }.into(pass_through("plain"))
    end

    assert_includes refusal { validate(klass.spec) }.refusals, "DuplicateMergeSource"
  end

  def test_a_merge_source_already_wired_into_the_node_with_a_transform_is_refused
    target = pass_through("plain")
    klass = operator("merge_rewired") do |c|
      ticks = c.input("ticks")
      ticks.map { |value| value }.into(target)
      c.merge.from(ticks) { |value| value }.from(c.input("quotes")) { |value| value }.into(target)
    end

    assert_includes refusal { validate(klass.spec) }.refusals, "EdgeAlreadyWired"
  end

  def test_the_latest_document_router_is_a_route_latest_node_and_the_named_form_names_it
    klass = operator("routers") do |c|
      trades = c.input("trades")
      c.route_latest(c.input("config"), trades) { |trade| trade[:symbol] }.sink("routed")
      c.route_latest_named(c.input("limits"), trades, "limits_router") { |trade| trade[:symbol] }
       .sink("limited")
    end
    validate(klass.spec)

    assert_equal [["route_latest", Flezha::RouteLatest], ["limits_router", Flezha::RouteLatest]],
                 klass.spec.tasklets.map { |node| [node.name, node.klass] }
    # The traffic item's key is computed on its own edge, so the router never holds the item.
    trades_edges = klass.spec.channels[0].consumers
    assert_equal [[{ traffic: "A" }], [{ traffic: "A" }]],
                 trades_edges.map { |edge| Flezha.apply_transforms(edge.transforms, { symbol: "A" }) }
  end

  # -- channel codecs ---------------------------------------------------------
  #
  # A codec is resolved when the class is declared, into the two slots the
  # dispatcher and the plugin frame already read. What every SDK sends alike is
  # `test/fixtures/channel_codecs.txt`; what is Ruby's own is asserted here.

  CODEC_CASES = Flezha::TestSupport.contract_file("test/fixtures/channel_codecs.txt")

  def codec_slots(input = {}, sink = {})
    klass = operator("codecs") { |c| c.input("in", **input).into(pass_through("echo")).sink("out", **sink) }
    entry, out = klass.spec.channels
    [entry.decode, out.sink_encode]
  end

  def case_bytes(field)
    return "".b if field == "<empty>"

    field.start_with?("hex:") ? [field.delete_prefix("hex:")].pack("H*") : field.b
  end

  def case_outcome(decode, encode, payload)
    value = decode.call(payload)
  rescue StandardError
    "decode_error"
  else
    begin
      encode.call(value).b
    rescue StandardError
      "encode_error"
    end
  end

  def test_every_shared_codec_case_sends_the_bytes_the_fixture_records
    cases = File.readlines(CODEC_CASES, chomp: true).grep_v(/\A(#|\z)/)
    refute_empty cases
    codec = ->(name) { name == "none" ? {} : { codec: name.to_sym } }

    cases.each do |row|
      entry, sink, payload, expected = row.split("|", 4)
      decode, encode = codec_slots(codec.call(entry), codec.call(sink))
      want = expected.end_with?("_error") ? expected : case_bytes(expected)

      assert_equal want, case_outcome(decode, encode, case_bytes(payload)), row
    end
  end

  def test_a_text_entry_hands_over_utf8_and_a_json_entry_symbolizes_keys
    assert_equal Encoding::UTF_8, codec_slots({ codec: :text })[0].call("hello".b).encoding
    assert_equal({ a: 1 }, codec_slots[0].call('{"a":1}'.b))
  end

  def test_msgpack_is_the_class_codec
    decode, encode = codec_slots({ codec: :msgpack }, { codec: :msgpack })

    assert_equal({ a: 1 }, decode.call(Flezha::Codec.encode({ a: 1 })))
    assert_equal Flezha::Codec.encode({ a: 1 }), encode.call({ a: 1 })
  end

  # A Ruby String is its bytes, so a `bytes` sink takes one, where Python and
  # Node refuse a string there.
  def test_a_refusal_names_the_channel_and_a_bytes_sink_takes_a_string
    decode, text = codec_slots({ codec: :text }, { codec: :text })
    assert_includes assert_raises(EncodingError) { decode.call("\xFF".b) }.message, "`in`"
    assert_includes assert_raises(Flezha::EmitError) { text.call(42) }.message, "`out`"

    bytes = codec_slots({}, { codec: :bytes })[1]
    assert_includes assert_raises(Flezha::EmitError) { bytes.call(42) }.message, "`out`"
    assert_equal "hello", bytes.call("hello")
  end

  def test_an_unknown_codec_name_is_refused_naming_the_operator_the_channel_and_the_names
    [[{ codec: :yaml }, {}, "in"], [{}, { codec: "yaml" }, "out"]].each do |input, sink, channel|
      message = refusal { codec_slots(input, sink) }.message

      assert_includes message, "codecs: #{channel}: unknown codec `yaml`"
      assert_includes message, "bytes, text, json, msgpack"
    end
  end

  def test_a_codec_and_a_callable_together_are_refused
    [
      [{ codec: :text, decode: ->(bytes) { bytes } }, {}, "in"],
      [{}, { codec: :text, encode: ->(value) { value } }, "out"]
    ].each do |input, sink, channel|
      assert_includes refusal { codec_slots(input, sink) }.message, "codecs: #{channel}: names both a codec"
    end
  end
end
