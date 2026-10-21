# frozen_string_literal: true

# The conformance run: every row of `test/fixtures/reference_topologies.txt`
# driven through THIS SDK's builder.
#
# `crates/flz_core/src/host/check_tests.rs` is the same table driven against the
# record directly, because the core has no builder. This one maps the same step
# vocabulary onto the Ruby graph builder, so what is measured here is what an
# author of a Ruby relay actually writes.
#
# Two entry points, both off `runner.rb`:
#
#   --conformance-report <path>  drive every row and write the report the drill
#                                reads (test/system/topology_refusal_e2e_test.exs),
#                                then exit without connecting to a controller
#   FLEZHA_REFUSAL_FIXTURE=<id>  register ONLY that row's topology at startup,
#                                so a refused one kills the process with the
#                                refusal's own name in its output
#
# The palette tasklets live here rather than in `fixtures.rb`: they exist
# only for this run, and nothing registers them but a row that names them.

require "json"

require "flezha"
require_relative "fixtures"
require_relative "fixtures_steps"

module Conformance
  # The fixture, relative to the repository root the app is launched from.
  FIXTURE = "test/fixtures/reference_topologies.txt"

  # The authoring surfaces this builder carries, read from the app's
  # `conformance.json` so the skip and the manifest the gate reads are one
  # statement. A row needing anything else is skipped BY NAME, which is what
  # makes the skip checkable. The settings tree is a class-body declaration
  # written as data here, so a setting can sit under any parent.
  SURFACES = JSON.parse(File.read(File.join(__dir__, "conformance.json"))).fetch("topology_surfaces").freeze

  # The two topologies the fixture cites rather than re-spells: this app's own
  # fixtures, the twins of its seven-column table.
  CITED = {
    "hybrid_channel" => Fixtures::HybridChannelRelay,
    "commit_block_chain" => Fixtures::TierCommitBlockSteps
  }.freeze

  # The palette. One fixture tasklet per role the fixture header names; the
  # facts a role declares are the facts a rule reads. A row mints its own
  # subclass of each, because a Ruby tasklet's mailbox capacity is a class
  # declaration and a resolved definition is memoised per class.
  class PaletteTasklet < Flezha::Tasklet
    def run(input, ctx) = ctx.emit(input)
  end

  class Plain < PaletteTasklet; end
  class Second < PaletteTasklet; end
  class Twin < PaletteTasklet; end

  class StaticKeys < PaletteTasklet
    cell_keys "a", "b"
  end

  class Routed < PaletteTasklet
    cell_keys "a", "b"
    def cells_for(_input) = ["a"]
  end

  # Dynamic by its wiring: every row that names it keys the edge into it.
  class Dynamic < PaletteTasklet; end

  class NoChangeLogical < PaletteTasklet
    logical_clock
  end

  class EventTime < PaletteTasklet
    def event_time(input) = input.to_s.length
  end

  # role => [the class it is bound to, the node identity it declares]. `twin`
  # is a distinct class declaring the SAME name as `plain`: that IS the
  # collision.
  PALETTE = {
    "plain" => [Plain, "plain"],
    "second" => [Second, "second"],
    "twin" => [Twin, "plain"],
    "static" => [StaticKeys, "static"],
    "routed" => [Routed, "routed"],
    "dynamic" => [Dynamic, "dynamic"],
    "nochange" => [NoChangeLogical, "nochange"],
    "eventtime" => [EventTime, "eventtime"]
  }.freeze

  # `verb(arg)` -> ["verb", "arg"]; a bare word -> ["word", ""].
  def self.split_call(text)
    verb, arg = text.split("(", 2)
    [verb.strip, arg.to_s.chomp(")").strip]
  end

  # The record a row's steps describe, written the way an author writes it.
  class Interpreter
    def initialize(steps)
      @steps = steps.split(";").map(&:strip).reject(&:empty?)
      # A handle is a channel id from an `entry(...)` step, or a tasklet role
      # naming that tasklet's output channel.
      @handles = {}
      @classes = {}
      @mailbox = knobs("mailbox").transform_values { |arg| Integer(arg) }
      @schemas = knobs("schema").transform_values { |arg| JSON.parse(arg) }
    end

    def declare(builder)
      @steps.each { |step| run_step(builder, step) }
    end

    private

    def run_step(builder, step)
      verb, arg = Conformance.split_call(step)
      case verb
      when "entry" then @handles[arg] = builder.input(arg, schema: @schemas[arg])
      when "branch" then branch(arg)
      when "merge" then merge(builder, arg)
      else dotted(builder, step)
      end
    end

    # `<handle>.<link>…` — a chain declared off a handle, a channel knob or a
    # tasklet knob. A class setting was read when the row's class was minted.
    def dotted(builder, step)
      head, rest = step.split(".", 2)
      return if head == "class"

      rest.split(".").reduce(handle(head)) { |current, link| apply_link(current, link, head) }
    end

    def apply_link(edge, link, role)
      verb, arg = Conformance.split_call(link)
      case verb
      when "map" then edge.map { |value| value }
      when "filter" then edge.filter { true }
      when "flat_map" then edge.flat_map { |value| [value] }
      when "key" then edge.key_by(&:to_s)
      when "named" then edge.named(arg)
      when "keyed" then edge.keyed
      when "retry_max" then edge.retry_max(Integer(arg))
      when "on_full" then edge.on_full(arg.to_sym)
      when "output" then arg.empty? ? edge.output : edge.output(arg)
      when /\Acall/ then call_step(edge, verb, arg, role)
      when "into" then wire(edge, arg)
      # Declared on the tasklet or the entry in this SDK, and read before the
      # step that names it.
      when "mailbox", "schema" then edge
      else raise "the fixture spells a link this interpreter does not map: #{verb}"
      end
    end

    # `<x>.into(<role>)` — the palette role bound to its fixture tasklet. The
    # result is that tasklet's output channel, addressed by its role.
    def wire(edge, role)
      @handles[role] = edge.into(tasklet_class(role))
    end

    # `<x>.call…(<arg>)`: a 500 ms call whose Response channel feeds the role
    # that produces x, unless the step leaves it unread.
    def call_step(edge, verb, arg, role)
      response =
        case verb
        when "call_factory" then edge.call(factory: arg, deadline_ms: 500)
        when "call_factory_nocreate" then edge.call(factory: arg, deadline_ms: 500, create: false)
        when "call_deadline" then edge.call("pricing", deadline_ms: Integer(arg))
        when "call", "call_unread" then edge.call(arg, deadline_ms: 500)
        else raise "the fixture spells a call this interpreter does not map: #{verb}"
        end
      response.into(tasklet_class(role)) unless verb == "call_unread"
      edge
    end

    # `merge(<role>, <x>, <x>…)`: one merge over the listed sources, each
    # under the identity mapping, wired into the role's tasklet as `into` is.
    def merge(builder, arg)
      role, *sources = arg.split(",").map(&:strip)
      wire(sources.reduce(builder.merge) { |merge, source| merge.from(handle(source)) { |value| value } }, role)
    end

    # `branch(<x>, <n>)` — the branch sugar's arity. The arms are empty: what
    # the row states is how many there are.
    def branch(arg)
      target, arms = arg.split(",").map(&:strip)
      handle(target).branch(*Array.new(Integer(arms)) { ->(_arm) {} })
    end

    def handle(name)
      @handles.fetch(name) { raise "the row addresses `#{name}`, which nothing has declared" }
    end

    # The row's own subclass of the role's palette tasklet, minted once. The
    # identity is re-declared because a node identity is deliberately not
    # inherited, and the capacity because a definition is resolved once.
    def tasklet_class(role)
      @classes[role] ||= begin
        base, identity = PALETTE.fetch(role) do
          raise "the fixture names a palette role that is not one: #{role}"
        end
        klass = Class.new(base)
        klass.tasklet_name(identity)
        capacity = @mailbox[role]
        klass.mailbox_capacity(capacity) if capacity
        klass
      end
    end

    # The `<handle>.<knob>(<arg>)` steps a row declares, read BEFORE the steps
    # run: a tasklet is minted by the `into` that first names it, and an entry
    # by its `entry`, each one step earlier than the knob that configures it.
    def knobs(knob)
      @steps.filter_map do |step|
        head, rest = step.split(".", 2)
        next unless rest&.start_with?("#{knob}(")

        [head, Conformance.split_call(rest).last]
      end.to_h
    end
  end

  # Whether this builder carries the surface a row needs.
  def self.carried?(row) = row[:needs] == "-" || SURFACES.include?(row[:needs])

  # The refusal table, in file order.
  def self.rows
    File.read(fixture_path).each_line.filter_map do |line|
      line = line.strip
      next if line.empty? || line.start_with?("#")

      cols = line.split("|").map(&:strip)
      next unless cols.size == 4

      { refusal: cols[0], topology: cols[1], steps: cols[2], needs: cols[3] }
    end
  end

  # The fixture as seen from wherever the app was launched: the drill runs it
  # from the repository root, a developer from the app's own directory.
  def self.fixture_path
    base = "."
    5.times do
      candidate = File.join(base, FIXTURE)
      return candidate if File.exist?(candidate)

      base = File.join(base, "..")
    end
    raise "#{FIXTURE} is not under any parent of the working directory"
  end

  # A row's relay class. Fresh per row: a relay memoises its own spec,
  # and the class settings are class declarations rather than builder calls,
  # written as the row wrote them so the core judges the tree.
  def self.relay_for(row)
    interpreter = Interpreter.new(row[:steps])
    klass = Class.new(Flezha::Relay)
    klass.factory_id(row[:topology])
    class_settings(row[:steps]).each { |setting, tree| klass.public_send(setting, tree) }
    klass.channels { |builder| interpreter.declare(builder) }
    klass
  end

  # `class.run(<tree>)` and `class.durable(<tree>)`: the tree's JSON, which is
  # how the class body takes it, a misplaced setting and all.
  def self.class_settings(steps)
    steps.split(";").map(&:strip).filter_map do |step|
      head, rest = step.split(".", 2)
      next unless head == "class"

      setting, tree = split_call(rest)
      raise "the fixture spells a class setting this interpreter does not map: #{setting}" unless
        %w[run durable].include?(setting)

      [setting, JSON.parse(tree)]
    end
  end

  def self.class_for(row)
    CITED[row[:topology]] || relay_for(row)
  end

  # The §5 short form. Ruby refuses a malformed declaration where the author
  # registers it and a duplicate factory id where the runner declares it, so
  # every row this SDK can write boots for real.

  # The `shorthand(<id>|-, <sink>|-, <input>|-)` steps of a row. A `.noout`
  # row needs a surface this builder lacks and is skipped before it gets here.
  def self.shorthand_steps(row)
    row[:steps].split(";").filter_map do |raw|
      step = raw.strip
      topology = step.end_with?(".topology")
      verb, args = split_call(step.delete_suffix(".topology").delete_suffix(".noout"))
      next unless verb == "shorthand"

      fields = args.split(",").map(&:strip)
      raise "a shorthand step declares three fields: read `#{step}`" unless fields.size == 3

      written = ->(field) { field == "-" ? nil : field }
      { id: written[fields[0]], sink: written[fields[1]], input: written[fields[2]],
        topology: topology }
    end
  end

  # Declare a row's short forms exactly as an author writes them. The second
  # runs in steps, so a collision crosses runs.
  def self.declare_shorthands(declarations)
    declarations.each_with_index.map do |declaration, position|
      Class.new(Flezha::Relay) do
        factory_id(declaration[:id]) if declaration[:id]
        run(:steps) unless position.zero?
        output(declaration[:sink]) if declaration[:sink]
        input(declaration[:input]) if declaration[:input]
        channels { |c| c.input("input").into(Plain) } if declaration[:topology]
        def run(input, ctx) = ctx.emit(input)
      end
    end
  end

  # A short-form row's verdict: what declaring and registering it answered.
  #
  # A duplicate factory id is the registry's refusal, which a deployed runner
  # reaches at boot; the registry is driven here rather than restated, which is
  # what keeps the name the author reads and the name reported the same one.
  def self.judge_shorthand(row, declarations)
    registry = Flezha::Registry.new
    declare_shorthands(declarations).each { |relay| registry.add(relay.spec) }
    { "topology" => row[:topology], "outcome" => "accepted" }
  rescue Flezha::Error => e
    refused(row, e)
  end

  # Every code a refusal carries: each issue's, or its own.
  def self.codes_of(error)
    codes = Array(error.details["issues"]).map { |issue| issue["code"] }
    codes.empty? ? [error.code] : codes
  end

  def self.refused(row, error)
    { "topology" => row[:topology], "outcome" => "refused", "names" => codes_of(error) }
  end

  # One row's verdict, in the report's shape. The refusal codes are the core's
  # own, carried on the error this SDK raises; nothing here re-derives one.
  def self.judge(row, core)
    unless carried?(row)
      return { "topology" => row[:topology], "outcome" => "skipped", "needs" => row[:needs] }
    end

    declarations = shorthand_steps(row)
    return judge_shorthand(row, declarations) unless declarations.empty?

    derived = Flezha::Graph.validate(class_for(row).spec, core)
    { "topology" => row[:topology], "outcome" => "accepted", "derived" => channels_of(derived) }
  rescue Flezha::Error => e
    refused(row, e)
  end

  # The derived declaration in the columns the fixture's seven-column table
  # states. The core spells a consumer `{"tasklet":N,"chain":M}`; the report
  # carries the flat list of tasklet indices, which is what that table pins.
  def self.channels_of(derived)
    derived["channels"].map do |channel|
      {
        "index" => channel["index"],
        "name" => channel["name"],
        "kind" => channel["kind"],
        "producer" => channel["producer"],
        "consumers" => channel["consumers"].map { |edge| edge["tasklet"] },
        "sink" => channel["sink"]
      }
    end
  end

  # Drive the whole table and write the report. Nothing here connects to a
  # controller: the run is the builder's answer to a file.
  def self.write_report(path)
    core = Flezha::Core.create
    table = rows
    judged = table.map { |row| judge(row, core) }
    File.write(path, JSON.pretty_generate({ "runtime" => "ruby", "rows" => judged }))
    skipped = judged.count { |entry| entry["outcome"] == "skipped" }
    warn("conformance: #{table.size} rows, #{table.size - skipped} ran, #{skipped} skipped")
  ensure
    core&.free
  end

  # Register ONLY the named row's topology, the way the app registers any
  # class. A refused row never reaches a connected runner: the declaration is
  # judged before the core is started, and the process dies naming the refusal.
  # A row needing a surface this builder lacks exits naming the surface.
  def self.register_only(runner, topology)
    row = rows.find { |candidate| candidate[:topology] == topology }
    raise "#{FIXTURE} has no row `#{topology}`" unless row

    unless carried?(row)
      warn("conformance: #{topology} skipped: this builder carries no `#{row[:needs]}`")
      exit 0
    end

    declarations = shorthand_steps(row)
    relays = declarations.empty? ? [class_for(row)] : declare_shorthands(declarations)
    relays.each { |relay| runner.register_factory(relay) }
  end
end
