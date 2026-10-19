# frozen_string_literal: true

require_relative "errors"
require_relative "native"

module Flezha
  # One outcome of a `call`, as its Response channel delivers it: the `request`
  # the call end sent, decoded back with the call's codec; the answer as
  # `value`, or the `error` the call ended in (a missed deadline is
  # `call.timeout`); `target`, the callee's instance id; and `created`, true
  # when this call started the callee.
  Response = Data.define(:request, :value, :error, :target, :created) do
    def ok? = error.nil?

    # The Response a controller's envelope carries, read by the core's
    # decoder; a payload that is no envelope raises `call.malformed_response`.
    # `decode` reads the request's and the answer's bytes.
    def self.decode(payload, decode)
      Core.load_extension unless defined?(Native)
      target, created, request, value, error = Native.response_decode(payload)
      new(
        request: decode.call(request), value: error ? nil : decode.call(value),
        error: error && Error.from_document(error), target:, created:
      )
    end
  end
end
