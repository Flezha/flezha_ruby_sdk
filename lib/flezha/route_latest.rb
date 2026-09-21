# frozen_string_literal: true

require_relative "codec"
require_relative "tasklet"

module Flezha
  # The single-cell tasklet `Builder#route_latest` expands to. Its input is
  # what the merge's two mappings make of a value, `{ document: }` or
  # `{ traffic: key }`; the traffic item itself never reaches it.
  #
  # The state is `{ document:, version:, delivered: { key => version } }` and
  # rides the change journal like any tasklet's: `{ retain: document }`
  # replaces the document and advances the version, and
  # `{ delivered: key, version: }` records one key's delivery.
  class RouteLatest < Tasklet
    tasklet_name "route_latest"

    # Version 0 is "nothing retained", so the first document lands at 1 and
    # every key's absent entry is already behind it.
    def initial_state(_cell_key) = { document: nil, version: 0, delivered: {} }

    def apply_change(state, change)
      if change.key?(:retain)
        state[:document] = change[:retain]
        state[:version] += 1
      else
        # A Symbol, because a Hash key decodes as one: a state restored from a
        # snapshot reads the key the live state wrote.
        state[:delivered][change[:delivered].to_sym] = change[:version]
      end
    end

    def run(input, ctx)
      state = ctx.state
      if input.key?(:document)
        # The document is opaque: equal is the class codec's bytes, so a
        # re-poll of an unchanged document is inert.
        document = input[:document]
        ctx.change({ retain: document }) unless Codec.encode(document) == Codec.encode(state[:document])
        return nil
      end

      key = input[:traffic]
      version = state[:version]
      return nil if version.zero? || state[:delivered].fetch(key.to_sym, 0) >= version

      # Emit first: a failed emit rides the channel's retry ladder, and
      # recording the delivery before it would make the retry silent.
      ctx.emit([key, state[:document]])
      ctx.change({ delivered: key, version: version })
      nil
    end
  end
end
