# frozen_string_literal: true

require_relative "errors"

module Flezha
  # A tasklet as the graph holds it, with its position in the declaration.
  # `output` is the one channel it produces on, created on first wiring.
  # `embeds` is the steps operator class the node runs one stepped instance of
  # per edge key, holding no cells of its own, and `input` that class's entry
  # channel the edge feeds when it declares several; both nil for a tasklet.
  # `encode` writes a value on an edge into the node: the embedded class's
  # entry reads it, so its codec; nil for a tasklet, which reads the class codec.
  TaskNode = Struct.new(:index, :name, :klass, :definition, :output, :embeds, :input, :encode)

  # A flow class whose declaration runs on the embedding builder: its entry is
  # `feed` and every node it names carries `prefix`.
  Inline = Struct.new(:prefix, :feed, :entries)

  # One consumer of a channel, with the transforms and key of its own edge.
  ConsumerEdge = Struct.new(:tasklet, :transforms, :key_by, keyword_init: true)

  # A channel's call end: the transforms and key of the chain that closed on
  # it, applied to a value before it becomes a request, and the request's
  # encoder.
  CallEnd = Struct.new(:transforms, :key_by, :encode, keyword_init: true)

  # One fluent sequence declared off a channel handle, as the record carries
  # it: `links` in the order the author called them, for the core to judge.
  # Every transform or `key_by` answers a new sequence over the previous,
  # which is what lets a shared prefix branch without the arms colliding;
  # `superseded` marks the prefix a later link built on, so only a tip reaches
  # the record. `edge` is the consumer an `into` sequence closes on, held aside
  # for the core to name back.
  Chain = Struct.new(:source, :links, :superseded, :edge, keyword_init: true)

  # A channel as the graph holds it. Its position in the list IS its id.
  # `sink` is the channel's name once a plugin end, a sink or a response, is
  # attached; nil means none, and the core's answer names no sink for a
  # response. `sink_encode` is that end's payload encoder and belongs to the
  # plugin frame alone: an inter-tasklet frame is decoded with the class codec,
  # so encoding one with it hands the consuming tasklet bytes it cannot read.
  # `call` is the channel's call end, when a chain closes on one.
  # `entry_encode` is an entry channel's writer, its codec's, JSON when it
  # names none: what a tasklet of a class embedding this one writes with.
  #
  # `chain_edges` runs parallel to `chains`: the closures each sequence closed
  # on, at the position the core reads that sequence at. `consumers` is filled
  # from the derived declaration — the core says which chains became edges.
  ChannelSpec = Struct.new(
    :index, :name, :kind, :producer, :consumers, :chains, :chain_edges, :overflow,
    :retry_max, :retry_backoff_ms, :keyed, :sink, :decode, :sink_encode, :call, :entry_encode,
    keyword_init: true
  )

  # Everything the graph knows about one declared class.
  # `run` and `durable` are the tree's JSON as written, `nil` when unwritten.
  ClassSpec = Struct.new(
    :factory_id, :description, :max_duration_ms, :run, :durable, :compact_after_bytes,
    :tasklets, :channels,
    keyword_init: true
  ) do
    # Whether this class runs in steps, whose producers release their values
    # untransformed.
    #
    # A steps stage releases ONE value per emit and the driver fans it out
    # to every consumer itself, so each consumer edge (its transforms and its
    # key) belongs to the consumer. The producer and the consumer read this one
    # predicate, so they cannot disagree about which side applies the edge.
    def runs_in_steps? = ClassSpec.branch(run) == "steps"

    # Whether an instance keeps its work anywhere: `durable` is not `false`.
    def keeps_work? = ClassSpec.branch(durable) != "false"

    # Whether each invocation's output rides one effect record the controller
    # stamps at flush: under the effectively-once tier, and for a durable
    # steps operator, whose stage commit names the record.
    def records_effects?
      exactly = run.is_a?(Hash) && run["flow"].is_a?(Hash) ? run["flow"]["exactly"] : nil
      ClassSpec.branch(exactly) == "effectively_once" || (runs_in_steps? && keeps_work?)
    end

    # The branch a tree setting names: its bare name, or its one key.
    def self.branch(tree)
      return "false" if tree == false
      return tree if tree.is_a?(String)

      tree.keys.first if tree.is_a?(Hash) && tree.size == 1
    end
  end

  # Run an edge's transform chain over one value and answer the values that
  # come out of it. Every link answers the list one value becomes: none when a
  # filter dropped it, one when it was mapped, several when a flat_map fanned
  # it. A later link runs over every value an earlier one yielded, and a value
  # that is itself an Array stays one value.
  #
  # One implementation for the three places a chain runs — an entry payload
  # before its handler, a payload at the prepare crossing, and an emit on its
  # way out — so a filter cannot mean one thing on one path and another
  # elsewhere.
  def self.apply_transforms(transforms, value)
    transforms.reduce([value]) do |values, transform|
      values.flat_map { |current| transform.call(current) }
    end
  end

  # The fluent channel builder an operator's `channels` block receives.
  class Builder
    attr_reader :factory_id, :tasklets, :channels, :compact_after_bytes_value

    def initialize(factory_id)
      @factory_id = factory_id
      @tasklets = []
      @channels = []
      # The links each fluent sequence was declared with, in declaration
      # order. Recorded, never judged: the core reads them and answers.
      @chains = []
      @merge_groups = 0
      # The flow classes being inlined, the innermost last.
      @inlining = []
      # One node per embedded steps class and name, so two unnamed mentions unify.
      @embedding_tasks = {}
    end

    def compact_after_bytes(bytes)
      @compact_after_bytes_value = bytes
      self
    end

    # Declare a controller-fed entry channel. `decode` turns the wire bytes
    # into the value a handler sees, `codec` names a format from
    # `Codec::CHANNEL_CODECS` instead, and with neither the bytes are JSON.
    #
    # Inside a flow class being inlined, it answers the edge that feeds it.
    def input(name, decode: nil, codec: nil)
      if (inline = @inlining.last)
        inline.entries << name.to_s
        return inline.feed
      end

      channel = ChannelSpec.new(
        index: @channels.size, name: name.to_s, kind: :entry, consumers: [],
        chains: [], chain_edges: [], overflow: :drop_nothing, keyed: false
      )
      named = channel_codec(channel, codec, decode)
      channel.decode = decode || Codec.channel_decoder(named, channel)
      channel.entry_encode = Codec.channel_encoder(named || :json, channel)
      @channels << channel
      Edge.new(self, [:entry, channel], {})
    end

    # The codec a channel names, refused when it is unknown or named beside
    # the channel's own decoder or encoder; nil when it names none.
    def channel_codec(channel, codec, callable)
      return nil if codec.nil?

      raise Error.new("topology.codec_with_callable", channel: channel.name) if callable

      known = Codec::CHANNEL_CODECS.find { |name| name.to_s == codec.to_s }
      return known if known

      raise Error.new("topology.unknown_codec", channel: channel.name, codec: codec.to_s,
                                                codecs: Codec::CHANNEL_CODECS.map(&:to_s))
    end

    # Merge two or more sources into one tasklet: see `Merge`.
    def merge = Merge.new(self)

    # The id that ties one merge's sources together in the record.
    def next_merge_group
      group = @merge_groups
      @merge_groups += 1
      group
    end

    # Turn an operator-wide document stream into per-key deliveries: every key
    # seen on `traffic` receives each distinct document once, as
    # `[key, document]`, on that key's own next traffic item. The block answers
    # a traffic item's key.
    #
    # A key never seen in traffic receives nothing, and a key learns of a new
    # document one traffic item late. Fanning a document out over a remembered
    # key set instead would emit keys this member may no longer own after a
    # rescale; a key its own traffic just reached is one it owns.
    #
    # Sugar over a merge into one packaged `RouteLatest` node named
    # `route_latest`, so a second router in one operator names its own node
    # with `route_latest_named`. Answers the router's output edge.
    def route_latest(documents, traffic, &key) = route_latest_named(documents, traffic, nil, &key)

    def route_latest_named(documents, traffic, node_name, &key)
      merge
        .from(documents) { |document| { document: document } }
        .from(traffic) { |item| { traffic: key.call(item).to_s } }
        .into(RouteLatest, node_name)
    end

    # The node for a tasklet. Two unnamed mentions of one tasklet are one
    # node — the naming convention every SDK mirrors — so a diamond that fans
    # out and back in has one instance, not two.
    def node_for(klass, name = nil)
      definition = klass.definition
      identity = prefixed((name || definition.name).to_s)
      existing = @tasklets.find { |node| node.name == identity && node.klass == klass }
      return existing if existing

      if @tasklets.any? { |node| node.name == identity }
        # The node table is keyed by identity and physically cannot hold two,
        # so the record the core would judge is unwritable. The refusal answers
        # under the core's own name all the same.
        raise Error.new("topology.node_identity_collision", name: identity)
      end

      node = TaskNode.new(@tasklets.size, identity, klass, definition, nil)
      @tasklets << node
      node
    end

    # `feed` into an operator class: a steps class becomes a node that runs
    # one stepped instance of it per key of the edge, its exit output
    # continuing on the edge answered; a flow class is inlined.
    def embed(feed, operator, name, input, max_cells)
      return inline(feed, operator, name, input) unless operator.spec.runs_in_steps?

      tasklet = @embedding_tasks[[operator, name]] ||= Class.new(Tasklet) do
        tasklet_name(operator.factory_id)
        max_cells(max_cells) if max_cells
        define_method(:run) { |_value, _ctx| nil }
      end
      node = node_for(tasklet, name)
      node.embeds = operator
      node.input = input&.to_s
      entries = operator.spec.channels.select { |channel| channel.kind == :entry }
      node.encode = (entries.find { |channel| channel.name == node.input } || entries.first)&.entry_encode
      feed.close_on(node, { "link" => "into", "tasklet" => node.index })
    end

    # Run a flow class's declaration on this builder: its entry is `feed` and
    # every node it names carries its prefix. The edge answered is its exit's,
    # the one node it added that nothing consumes (its last when every node
    # feeds another or a sink).
    def inline(feed, operator, name, input)
      first = @tasklets.size
      frame = Inline.new(prefixed((name || operator.factory_id).to_s), feed, [])
      @inlining << frame
      begin
        operator.declare_on(self)
      ensure
        @inlining.pop
      end
      entries = frame.entries.uniq.sort
      unless input.nil? ? entries.size == 1 : entries == [input.to_s]
        raise Error.new("topology.embedded_entry_ambiguous", inner: operator.factory_id,
                                                              channels: entries.join(", "))
      end
      added = @tasklets[first..]
      exits = added.select { |node| node.output.nil? }
      if exits.size > 1
        raise Error.new("topology.embedded_exit_ambiguous", inner: operator.factory_id,
                                                             channels: exits.map(&:name).sort.join(", "))
      end
      exit_node = exits.first || added.last
      exit_node ? Edge.new(self, [:tasklet, exit_node], {}) : feed
    end

    # The channel this node produces on, created on first use.
    #
    # Lazily, so a leaf node never carries a phantom channel. Named by the
    # node's identity until a `named(...)` override, and reused by every later
    # call on an edge whose source is this node — which is what makes a fan-out
    # one channel with two consumers rather than two channels claiming one
    # producer.
    def output_channel_for(node)
      return node.output if node.output

      channel = ChannelSpec.new(
        index: @channels.size, name: node.name, kind: :inter_task,
        producer: node.index, consumers: [], chains: [], chain_edges: [],
        overflow: :drop_nothing, keyed: false
      )
      @channels << channel
      node.output = channel
      channel
    end

    # A node's identity inside the class being inlined: `<prefix>.<name>`.
    def prefixed(name) = @inlining.empty? ? name : "#{@inlining.last.prefix}.#{name}"

    # Record one more link on the sequence at `base`, or open one on `source`,
    # and answer the new sequence's id.
    #
    # The links are copied rather than appended in place: a handle held over a
    # branch point extends the prefix twice, and two sequences sharing one
    # array would each see the other's links.
    def link_chain(source, base, link, edge: nil)
      links = base.nil? ? [] : @chains[base].links
      @chains[base].superseded = true unless base.nil?
      @chains << Chain.new(source: source, links: links + [link], superseded: false, edge: edge)
      @chains.size - 1
    end

    # File every sequence on the channel it was declared off, in declaration
    # order — the `chains` the record carries.
    #
    # A channel named only by a sequence no terminator closed is created here,
    # so a dangling chain is submitted rather than dropped on the way to the
    # core. Idempotent: a spec is assembled once, but a builder is reachable.
    def seal
      return self if @sealed

      @sealed = true
      @chains.each do |chain|
        next if chain.superseded

        channel = channel_of(chain.source)
        channel.chains << chain.links
        channel.chain_edges << chain.edge
      end
      self
    end

    # The channel an edge's values leave on: the entry's own, or the producing
    # node's, created on first use.
    def channel_of(source)
      source[0] == :entry ? source[1] : output_channel_for(source[1])
    end
  end

  # One edge under construction. Immutable: every fluent call answers a new
  # edge, so a shared prefix can be branched without the arms colliding.
  class Edge
    # The two links a fluent call adds. Frozen literals rather than a hash per
    # call: every `map` on every channel records the same one.
    TRANSFORM = { "link" => "transform" }.freeze
    KEY = { "link" => "key" }.freeze

    def initialize(builder, from, pending)
      @builder = builder
      @from = from
      @pending = pending
    end

    # Override the channel name this edge creates. The default is derived from
    # the producing node's identity.
    def named(name) = with(channel_name: name.to_s)

    # Transform every value on this edge. Evaluated producer-side.
    def map(&transform) = flat_map { |value| [transform.call(value)] }

    # Drop the values a predicate rejects, before this edge's key is computed.
    # A dropped value is consumed and reaches no handler.
    def filter(&predicate) = flat_map { |value| predicate.call(value) ? [value] : [] }

    # Turn every value on this edge into the values the block answers, each
    # keyed and delivered on its own. An empty answer drops the value. Every
    # transform link is one of these: `map` and `filter` are its one-value and
    # one-or-none cases.
    def flat_map(&fan)
      relink(TRANSFORM, transforms: transforms + [->(value) { fan.call(value).to_a }])
    end

    # Route by a computed key: the consumer becomes a dynamic keyed tasklet.
    def key_by(&key) = relink(KEY, key_by: key)

    def on_full(policy) = with(overflow: policy.to_sym)
    def retry_max(attempts) = with(retry_max: attempts)
    def retry_backoff_ms(milliseconds) = with(retry_backoff_ms: milliseconds)

    # Stamp `flz.cell` / `flz.seq` / `flz.key` onto this channel's plugin frames.
    def keyed = with(keyed: true)

    # Fan out: each arm receives this same edge, so `branch` builds exactly the
    # topology the separate `into` statements would.
    #
    # The arity is recorded as a sequence of its own, so the arms extend the
    # prefix this edge already carries rather than the branch's own record.
    def branch(*arms)
      @builder.link_chain(@from, nil, { "link" => "branch", "arms" => arms.size })
      arms.each { |arm| arm.call(self) }
      self
    end

    # Wire this edge into a tasklet, or into an operator class.
    #
    # A class that runs in steps becomes a node, named by its factory id, that
    # runs one stepped instance of it per key of this edge (one on an unkeyed
    # edge) with its own state and journal; the edge answered carries its exit
    # tasklet's output, and `max_cells` caps the instances a keyed edge keeps
    # at once. A class that runs in flow is inlined: its graph joins this one,
    # fed by this edge, its nodes named `<factory_id>.<node>`, and the edge
    # answered is its exit's. `input` names the class's entry channel when it
    # declares several.
    def into(klass, node_name = nil, input: nil, max_cells: nil)
      return @builder.embed(self, klass, node_name, input, max_cells) if klass < Operator

      node = @builder.node_for(klass, node_name)
      close_on(node, { "link" => "into", "tasklet" => node.index })
    end

    # Close this edge's chain on a resolved node with `link`, and answer the
    # node's output edge. The one body behind `into` and every merge source.
    def close_on(node, link)
      # Creates this edge's channel and applies its pending options, before the
      # link that closes on it is recorded.
      channel
      # The transforms and the key belong to this edge, not to the channel: one
      # channel may feed several tasklets, and a chain declared before one
      # `into` must not reach another. They ride the chain that closes on them,
      # and the core names that chain back on the edge it derived — so whether
      # a re-declaration is the legal fan-in no-op or `EdgeAlreadyWired` is
      # answered there and nowhere here.
      @builder.link_chain(
        @from, @pending[:chain], link,
        edge: ConsumerEdge.new(
          tasklet: node.index, transforms: transforms, key_by: @pending[:key_by]
        )
      )
      Edge.new(@builder, [:tasklet, node], {})
    end

    # Terminate this edge's channel on the plugin layer, and answer the edge.
    # A name written here IS the channel's name, everywhere one appears;
    # `sink` with none leaves it derived as it would be without a sink at all.
    #
    # `sink(...).into(...)` wires a consumer behind the sink, so one emit
    # reaches both. A channel with no consumer behind it is the plugin-bound
    # terminal — terminality is the absence of further wiring, not a different
    # method.
    # The one refusal left here: a channel with no producer has no value to
    # serialize, and the core reads a sink as the terminal of the channel it
    # sits on — it cannot see that this one is fed by the controller.
    #
    # `encode` turns a value into the plugin frame's bytes, and `codec` names a
    # format instead. With neither, a String is sent as its bytes and any other
    # value as JSON, so a sink needs neither for a string, bytes or a JSON value.
    def sink(name = nil, encode: nil, codec: nil)
      link = { "link" => "sink" }
      link["name"] = name.to_s unless name.nil?
      plug_end(link, name&.to_s, encode, codec)
    end

    # Terminate this edge's channel on the answer to the outside call its input
    # carried, and answer the edge. Each value emitted on it answers that
    # caller and reaches no plugin. It takes no name, so the channel keeps the
    # one it would have without it, and consumers may follow it exactly as
    # after `sink`. A channel ends in one sink or one response. `encode` and
    # `codec` are `sink`'s.
    def response(encode: nil, codec: nil)
      plug_end({ "link" => "response" }, nil, encode, codec)
    end

    # End this edge's chain in a call to another operator, and answer the edge
    # of its Response channel: the entry channel `<channel>.response`, whose
    # values are `Flezha::Response`s. Every value reaching the call, after this
    # chain's transforms and key, is one request. `target` names an instance,
    # or a partition group whose member the key chooses; `factory:` instead
    # names a factory whose instance the key names, started when absent unless
    # `create: false`. `channel:` names the callee's entry channel. `codec`
    # names the format of both the request and the answer, JSON by default. A
    # call is not an edge, so the Response channel may feed this tasklet.
    def call(target = nil, factory: nil, deadline_ms:, create: true, channel: nil, codec: nil)
      raise Error.new("topology.sink_on_entry", channel: @from[1].name) unless @from[0] == :tasklet

      source = self.channel
      named = @builder.channel_codec(source, codec || :json, nil)
      @builder.link_chain(@from, @pending[:chain], call_link(target, factory, deadline_ms, create, channel, source))
      # First call wins, as for a sink; a second end is the core's to refuse.
      source.call ||= CallEnd.new(transforms: transforms, key_by: @pending[:key_by],
                                  encode: Codec.channel_encoder(named, source))
      decode = Codec.channel_decoder(named, source)
      @builder.input("#{source.name}.response", decode: ->(bytes) { Response.decode(bytes, decode) })
    end

    protected

    # Attach a plugin end, a sink or a response: both leave as a plugin output
    # encoded the same way, and the core refuses a second end on a channel.
    def plug_end(link, name, encode, codec)
      unless @from[0] == :tasklet
        raise Error.new("topology.sink_on_entry", channel: name || "<derived>")
      end

      target = channel
      # The name written here IS the channel's name; `sink` with none leaves it
      # derived as it would be without a sink at all.
      target.name = name unless name.nil?
      named = @builder.channel_codec(target, codec, encode)
      @builder.link_chain(@from, @pending[:chain], link)
      # First call wins on the builder's own side, as the reference does; a
      # second end is what the core refuses the class for.
      if target.sink.nil?
        target.sink = target.name
        target.sink_encode = encode || Codec.channel_encoder(named, target)
      end
      self
    end

    # The channel this edge's values leave on, with the pending options
    # applied. An entry edge already has one; a tasklet edge's is created on
    # first use and reused after.
    def channel
      if @from[0] == :entry
        target = @from[1]
      else
        target = @builder.output_channel_for(@from[1])
        rename = @pending[:channel_name]
        # Recorded where it is APPLIED, so the core reads the overrides in the
        # order this builder took them and the two cannot disagree about which
        # name a channel ends up with.
        if rename && target.name != rename
          target.name = rename
          @builder.link_chain(@from, nil, { "link" => "named", "name" => rename })
        end
      end
      apply_pending(target)
      target
    end

    private

    def transforms = @pending[:transforms] || []

    # The `call` link as the record carries it; which of `target` and `factory`
    # is named, and whether exactly one is, is the core's to judge.
    def call_link(target, factory, deadline_ms, create, channel, source)
      link = { "link" => "call" }
      link["target"] = target.to_s unless target.nil?
      link["factory"] = factory.to_s unless factory.nil?
      link.merge!("deadline_ms" => deadline_ms, "create" => create)
      link["channel"] = channel.to_s unless channel.nil?
      link.merge("response" => "#{source.name}.response")
    end

    # Applied now, as the reference applies it: an option no terminator follows
    # would otherwise be dropped. This call is why `channel` is protected.
    def with(**next_pending)
      edge = Edge.new(@builder, @from, @pending.merge(next_pending))
      edge.channel
      edge
    end

    # This edge with one more chain link — the one place a transform's or a
    # key's result edge is built, so every link reaches the record and a chain
    # no `into(...)` closes is submitted rather than silently discarded.
    def relink(link, **next_pending)
      chain = @builder.link_chain(@from, @pending[:chain], link)
      Edge.new(@builder, @from, @pending.merge(next_pending, { chain: chain }))
    end

    def apply_pending(channel)
      channel.overflow = @pending[:overflow] if @pending[:overflow]
      channel.retry_max = @pending[:retry_max] unless @pending[:retry_max].nil?
      channel.retry_backoff_ms = @pending[:retry_backoff_ms] unless @pending[:retry_backoff_ms].nil?
      channel.keyed = true if @pending[:keyed]
    end
  end

  # Several sources into one tasklet node, each lifted into the merged value
  # by its own mapping. Recorded, never judged: a merge of one source, or of
  # one source twice, is the core's to refuse.
  class Merge
    def initialize(builder)
      @builder = builder
      @sources = []
      @keys = []
    end

    # One source and the block that maps its values into the merged value. The
    # mapping is that source's own edge transform, after any the handle
    # already carries, so it runs on the edge the core derives for the source.
    def from(source, &mapping)
      @sources << source.map(&mapping)
      self
    end

    # Route the merged value by a computed key, evaluated on the mapped value.
    def key_by(&key)
      @keys << key
      self
    end

    # Close every source on one node under one merge group, and answer the
    # node's output edge.
    def into(klass, node_name = nil)
      node = @builder.node_for(klass, node_name)
      link = { "link" => "merge", "tasklet" => node.index, "group" => @builder.next_merge_group }
      # The key rides each source's chain after its mapping; a second `key_by`
      # is one more link, which the core refuses as it would on one edge.
      @sources.each do |source|
        @keys.reduce(source) { |edge, key| edge.key_by(&key) }.close_on(node, link)
      end
      Edge.new(@builder, [:tasklet, node], {})
    end
  end
end
