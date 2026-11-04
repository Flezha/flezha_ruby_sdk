# frozen_string_literal: true

module Flezha
  VERSION = "0.1.1"

  # The activation schema this gem was built against. Passed at context
  # creation and compared with the native library's, so a skewed pair fails
  # naming both versions before any controller connection is attempted.
  ACTIVATION_SCHEMA_VERSION = 8
end
