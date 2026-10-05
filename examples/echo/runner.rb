# frozen_string_literal: true

# Starts a runner: a process that connects to the controller and serves your
# relays.
#
# Declare every relay you want served before `boot`. One runner can serve
# several, and the same call takes a steps relay.

require "flezha"
require_relative "echo_relay"

namespace = ARGV[0] || "default"

# the relays this runner serves
runner = Flezha.create_runner(namespace).register_factory(EchoRelay)

# `run` is `boot` followed by `await_termination`. Splitting them leaves room
# for the calls below, and still blocks until shutdown.
runner.boot # connects to the controller

# Everything `flz relay` and `flz plugin` do is also a method of `control`,
# and `send_to` hands a message to a relay. Uncomment to drive this
# namespace from the application instead of from a terminal:
#
# control = runner.control
# control.start_relay("echo", id: "my_echo")
# control.enable_plugin("writer", "relay_file_sink", opts: { "path" => "echo-out.txt" })
# control.bind_plugin("writer", "my_echo", hooks: [["on_output", {}]])
# runner.send_to("my_echo", "hello world", "in", msg_id: "hello-1")
#
# docs/topics/advanced/sending_from_runner_code.md covers sending. An enforced
# controller refuses all of it until `flz role bind runner@default app default`.

# Blocks until shutdown: a SIGTERM, or standard input closing.
runner.await_termination
