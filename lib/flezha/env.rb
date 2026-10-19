# frozen_string_literal: true

module Flezha
  # A knob read from the environment and parsed by the core's one rule, so a
  # Ruby runner answers `"0"`, `"on"` or a typo exactly as every other runtime
  # does. A knob's name, type and default are declared once in
  # `Flezha.Config.Keys`.
  module Env
    module_function

    # A count from the environment, with `fallback` for anything that is not one.
    def positive(name, fallback)
      Core.load_extension
      Native.knob("positive", ENV.fetch(name, "")) || fallback
    end
  end
end
