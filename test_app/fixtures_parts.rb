# frozen_string_literal: true

# Twins of the reference's part fixtures (test/apps/rust_runner/src/part.rs)
# under the same factory ids, tasklet names, channel names and payloads: for an
# input `n` the tasklet sends the parts "1" .. "n" and then emits "done:<n>" on
# its output `answer`. `retry:<n>` sends its parts and then fails retryably the
# first time this process sees it, so the retry's parts number from 0 again
# under the next attempt.

require "flezha"

require_relative "fixtures_state"

module Fixtures
  class PartSender < Flezha::Tasklet
    # The `retry:` inputs that have already failed once in this process.
    FAILED_ONCE = {}
    LOCK = Mutex.new

    def run(input, ctx)
      count = input.delete_prefix("retry:")
      n = Integer(count, 10, exception: false)
      raise Flezha::NonRetryableError, "not a part count: #{input}" unless n

      (1..n).each { |part| ctx.part(part.to_s) }
      raise "first attempt of #{input}" if input.start_with?("retry:") && first_failure?(input)

      ctx.emit("done:#{n}")
    end

    private

    def first_failure?(input)
      LOCK.synchronize { !FAILED_ONCE.key?(input) && (FAILED_ONCE[input] = true) }
    end
  end

  class PartEchoStep < PartSender
    tasklet_name "part_echo"
  end

  class PartEcho < Flezha::Relay
    factory_id "part_echo"
    channels do |c|
      c.input("in", codec: :text).retry_backoff_ms(25).into(PartEchoStep).output("answer")
    end
  end

  class PartEchoTurn < PartSender
    tasklet_name "part_echo_turn"
  end

  class PartEchoSteps < Flezha::Relay
    factory_id "part_echo_steps"
    run steps: { each: :per_input }
    durable FIXTURE_DURABLE
    max_duration_ms 600_000
    channels do |c|
      c.input("in", codec: :text).retry_backoff_ms(25).into(PartEchoTurn).output("answer")
    end
  end

  PARTS = [PartEcho, PartEchoSteps].freeze
end
