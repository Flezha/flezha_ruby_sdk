# frozen_string_literal: true

require "json"

require_relative "errors"
require_relative "version"

module Flezha
  # The compiled veneer over `flz_core`'s C ABI, and the two rules that hold
  # around it: one context per process, and no core call from a process that
  # did not start it.
  #
  # `Flezha::Native` itself is defined by the extension (`ruby_glue.rs`). This
  # wrapper adds no ABI surface — every method below forwards to exactly one
  # veneer function — it adds the fork guard, which is a host rule rather than
  # a core one.
  class Core
    # An absent extension is an installation defect rather than a refusal, and
    # nothing is left to render a catalogue message with, so it stays a
    # `LoadError`.
    def self.load_extension
      # Ruby derives `Init_flz_core_ffi` from the basename, which is what the
      # veneer exports. A precompiled gem carries one library per Ruby minor,
      # under `lib/flezha/<minor>/`; a source build puts its one in `lib/flezha/`.
      per_minor = "flezha/#{RUBY_VERSION[/\A\d+\.\d+/]}/flz_core_ffi"
      begin
        require per_minor
      rescue LoadError => e
        # Only an absent file falls through; one that is there and will not
        # load is the error to report.
        raise unless e.path == per_minor

        require "flezha/flz_core_ffi"
      end
    rescue LoadError => e
      raise LoadError,
            "the flezha native extension could not be loaded (#{e.message}). Install a " \
            "precompiled platform gem, or build the source gem with cargo and git."
    end

    # The JSON error document the catalogue renders for `code` and `details`:
    # what `Flezha::Error` is built from, for a refusal this SDK makes itself.
    def self.error_document(code, details)
      load_extension
      Native.error_document(code.to_s, JSON.generate(details))
    end

    # Create the process context, negotiating the activation schema version
    # before any controller connection is attempted.
    def self.create
      load_extension
      Native.context_new(ACTIVATION_SCHEMA_VERSION)
      new
    end

    # The entry points that take no context and therefore no guard: they read
    # a process-global clock, which a fork does not break. Reachable before
    # `create`, which is what lets a runner answer before it boots.
    def self.context_free
      load_extension
      Native
    end

    def initialize
      # Recorded at construction rather than at `start`, so a fork between the
      # two is caught by the same guard.
      @owner_pid = Process.pid
    end

    # The process that owns the core. A fork after this point does not carry
    # the core's Rust threads — transport, flusher, the runner loop — so the
    # child would connect to nothing and serve nothing, silently.
    def guard!
      return if Process.pid == @owner_pid

      raise Error.new("runtime.forked_process", owner: @owner_pid, pid: Process.pid)
    end

    # Every ABI entry point a running host reaches, in the veneer's own
    # spelling.
    #
    # Generated rather than written out fifteen times over: the fork guard then
    # provably applies to all of them — there is no site left to forget it at —
    # and `native_test.rb` asserts this list is exactly what the veneer
    # exports, so an entry point added there and not here is a red build rather
    # than a method that quietly does not exist.
    ENTRY_POINTS = %i[
      declare_class check_class start poll wait_starvation complete abandon shutdown
      local_start local_on_sink local_feed local_next_output local_run_until_idle local_stop
      watch_stdin_eof spawn_request spawn_result part admin_call send_message relays
      monotonic_nanos last_error
    ].freeze

    ENTRY_POINTS.each do |entry|
      define_method(entry) do |*args|
        guard!
        Native.public_send(entry, *args)
      end
    end

    # The one entry point not named after its own: `context_free` releases what
    # `context_new` created, and `create` is what made this object.
    def free
      guard!
      Native.context_free
    end
  end
end
