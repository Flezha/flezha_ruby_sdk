# frozen_string_literal: true

require "json"

require_relative "errors"

module Flezha
  # One outcome of a `call`, as its Response channel delivers it: the `request`
  # the call end sent, decoded back with the call's codec; the answer as
  # `value`, or the `error` the call ended in (a missed deadline is
  # `call.timeout`); `target`, the callee's instance id; and `created`, true
  # when this call started the callee.
  Response = Data.define(:request, :value, :error, :target, :created) do
    def ok? = error.nil?

    # The Response a controller's envelope carries: a JSON object whose
    # `request` and `value` are padded base64 and whose `error` is an error
    # object. `decode` reads the request's and the answer's bytes.
    def self.decode(payload, decode)
      envelope = JSON.parse(payload)
      error = envelope["error"] && Error.from_object(envelope["error"])
      new(
        request: decode.call(envelope.fetch("request").unpack1("m0")),
        value: error ? nil : decode.call(envelope.fetch("value").unpack1("m0")),
        error: error, target: envelope.fetch("target"), created: envelope.fetch("created")
      )
    end
  end
end
