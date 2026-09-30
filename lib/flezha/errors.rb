# frozen_string_literal: true

require "json"

module Flezha
  # Anything Flezha refuses: the controller, the core, or this SDK itself.
  #
  # `code` (`family.snake`) and `error_class` (`invalid`, `not_found`,
  # `denied`, ...) are what a program branches on; `message` is one sentence
  # for a reader, `details` names what the message is about, and `hint` is the
  # next command to run, when there is one. An uncaught one prints the code
  # after the message.
  #
  # A refusal this SDK makes is built from its code and details, and its
  # message is the catalogue's, rendered by the core:
  #
  #   raise Flezha::Error.new("topology.duplicate_factory_id", factory_id: "echo")
  class Error < StandardError
    attr_reader :code, :error_class, :details, :hint

    # The error a JSON error document `{"error": {...}}` carries. The native
    # extension raises every failure through here.
    def self.from_document(document) = from_object(JSON.parse(document).fetch("error"))

    # The error a parsed error object (`code`, `class`, `message`, `details`,
    # `hint`) carries, as a controller writes one into a call's outcome.
    def self.from_object(fields)
      allocate.tap { |error| error.send(:fill, fields) }
    end

    def initialize(code, **details)
      super()
      fill(JSON.parse(Flezha::Core.error_document(code, details)).fetch("error"))
    end

    def to_s = @message

    # How an uncaught one prints: the message, then the code.
    def detailed_message(**) = "#{message} (#{code})"

    private

    def fill(fields)
      @code = fields.fetch("code")
      @error_class = fields.fetch("class")
      @message = fields.fetch("message")
      @details = fields["details"] || {}
      @hint = fields["hint"]
    end
  end

  # A handler error the retry ladder must not retry. Raise this (or set
  # `non_retryable` on any error) to park an item immediately. An author's own
  # steering type, not a refusal, so it is no `Flezha::Error`.
  class NonRetryableError < StandardError
    def non_retryable? = true
  end
end
