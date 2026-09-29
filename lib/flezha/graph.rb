# frozen_string_literal: true

require "json"

require_relative "builder"
require_relative "errors"

module Flezha
  # The `HostClassDecl` the core is handed, and the verdict it answers.
  #
  # The topology is judged in ONE place, `flz_core::host::register::check`:
  # this module records what the author declared, submits it, and the core's
  # refusal reaches the author as the `Flezha::Error` it raises. What is refused here instead is
  # what a record cannot carry: whether a Ruby class supplies the migration a
  # schema floor needs, which the core cannot see.
  module Graph
    # The Ruby SDK's byte in the core's `sdk_lang` allocation table.
    SDK_LANG_RUBY = 5

    # This SDK's own released version, stamped into every durable record it
    # writes; the core packs it. `Flezha::VERSION` is the single literal
    # `lib/flezha/version.rb` holds and the gemspec already reads, so the gem
    # cannot ship a version its records disagree with.
    SDK_VERSION = Flezha::VERSION

    # MessagePack, shared with every SDK whose codec is MessagePack.
    FORMAT_ID_MSGPACK = 5

    module_function

    # Judge one declaration and answer what the core derived from it: the
    # channel names, kinds, consumers, sink names and acknowledgement
    # granularity it read off the record.
    #
    # Judge, register and declare one class, in the one order that works.
    #
    # `validate` writes the derived channel names and consumer edges back into
    # the spec, so a caller that skipped it would register a spec whose edges
    # route nowhere. Both the runner and local mode reach the core through
    # here, so there is one such order rather than two that agree.
    def declare(spec, core, registry)
      validate(spec, core)
      registry.add(spec)
      core.declare_class(JSON.generate(to_host_class_decl(spec)))
    end

    # The defects this SDK can see that the record cannot carry are refused
    # first, so an author reads "supplies no migration" rather than a topology
    # verdict about a record the class never wrote.
    def validate(spec, core)
      check_tasklets(spec)
      derived = submit(spec, core)
      bind_edges(spec, derived)
      derived
    end

    # The record, over the ABI, and the verdict back. A refusal raises the
    # core's `Flezha::Error`: its code is the first issue's, and
    # `details["issues"]` lists every issue as `{code, message, details}`.
    def submit(spec, core)
      JSON.parse(core.check_class(JSON.generate(to_host_class_decl(spec))))
    end

    # The consumers the CORE derived, each carrying the closures of the chain
    # that produced it: the builder held them aside per chain, and the derived
    # edge names the chain. Which edge survives a re-declaration is therefore
    # the core's answer, not a rule this SDK repeats.
    def bind_edges(spec, derived)
      derived["channels"].each do |channel|
        target = spec.channels[channel["index"]]
        target.consumers = channel["consumers"].map { |edge| target.chain_edges[edge["chain"]] }
      end
    end

    def check_tasklets(spec)
      spec.tasklets.each { |node| check_schema(node) }
    end

    def check_schema(node)
      definition = node.definition
      floor = definition.min_readable_schema_version
      return unless floor

      running = definition.schema_version || 1
      if floor > running
        raise Error.new("topology.schema_floor_above_version", tasklet: node.name, floor: floor, version: running)
      end
      return unless floor < running && !definition.migrate_state? && !definition.migrate_change?

      raise Error.new("topology.schema_floor_without_migration", tasklet: node.name, floor: floor)
    end

    # Drop the keys whose value is nil, so the JSON omits them.
    def compact(hash) = hash.reject { |_key, value| value.nil? }

    # The channel as the record carries it: the kind the author declared and
    # the sequences declared off its handle. The consumers, the plugin kind and
    # the sink name are the core's to DERIVE from those chains.
    def channel_json(channel)
      compact(
        "name" => channel.name,
        "kind" => channel.kind.to_s,
        "producer" => channel.producer,
        "chains" => channel.chains,
        "retry_max" => channel.retry_max,
        "retry_backoff_ms" => channel.retry_backoff_ms,
        "overflow" => channel.overflow.to_s,
        "keyed" => channel.keyed
      )
    end

    # The nodes some chain reaches through a `key` link. The cell mode is the
    # wiring's, so one tasklet can be keyed in one operator and single in
    # another.
    def keyed_nodes(spec)
      spec.channels.flat_map(&:chains).each_with_object(Set.new) do |links, keyed|
        key = false
        links.each do |link|
          case link["link"]
          when "key" then key = true
          when "into", "merge" then keyed << link["tasklet"] if key
          end
        end
      end
    end

    def cell_mode(node, keyed)
      if node.definition.cell_keys then "static"
      elsif keyed.include?(node.index) then "dynamic"
      else "single"
      end
    end

    def tasklet_json(node, keyed)
      definition = node.definition
      compact(
        "name" => node.name,
        "cell_mode" => cell_mode(node, keyed),
        "cell_keys" => definition.cell_keys || [],
        "idle_ttl_ms" => definition.idle_ttl_ms,
        "max_cells" => definition.max_cells,
        "cells_for_overridden" => definition.cells_for?,
        "compensate_overridden" => definition.compensate?,
        "clock" => definition.clock.to_s,
        "mailbox_capacity" => definition.mailbox_capacity || 1024,
        # A tasklet with no state journals nothing, so a logical clock over
        # it never advances. Ruby has no per-tasklet Change type; having a
        # state IS the same statement.
        "change_is_nochange" => !definition.stateful?,
        "schema_version" => definition.schema_version || 1,
        "min_readable_schema_version" => definition.min_readable_schema_version
      )
    end

    # The declaration the core deserializes. Every key the author wrote is here
    # whatever the class's kind: which keys a kind may carry is the core's
    # judgement, answered by name. Every key is pinned by
    # `test/fixtures/host_class_decl_keys.txt`, which this gem's own test
    # reads: a key the core learns that Ruby cannot spell fails that test.
    def to_host_class_decl(spec)
      keyed = keyed_nodes(spec)
      compact(
        "factory_id" => spec.factory_id,
        "persistent" => spec.persistent,
        "sleep_when_idle" => spec.sleep_when_idle,
        "sdk_lang" => SDK_LANG_RUBY,
        "sdk_version" => SDK_VERSION,
        "change_format_id" => FORMAT_ID_MSGPACK,
        "effectively_once" => spec.effectively_once,
        "checkpoint_ack" => spec.checkpoint_ack,
        "checkpoint_every" => spec.checkpoint_every,
        "checkpoint_outputs" => spec.checkpoint_outputs.to_s,
        "checkpoint_max_delay_ms" => spec.checkpoint_max_delay_ms,
        "correction_window_ms" => spec.correction_window_ms,
        "beyond_window_policy" => spec.beyond_window_policy.to_s,
        "compact_after_bytes" => spec.compact_after_bytes,
        "sync_durability" => spec.sync_durability,
        "channels" => spec.channels.map { |channel| channel_json(channel) },
        "tasklets" => spec.tasklets.map { |node| tasklet_json(node, keyed) },
        "bounded" => spec.bounded && compact(
          "loop_mode" => spec.bounded[:loop_mode].to_s,
          "rollback_mode" => spec.bounded[:rollback_mode].to_s,
          "max_duration_ms" => spec.bounded[:max_duration_ms]
        )
      )
    end
  end
end
