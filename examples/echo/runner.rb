# frozen_string_literal: true

# Starts a runner: a process that connects to the controller and serves your
# operators.
#
# Declare every operator you want served before `boot`. One runner can serve
# several, and the same call takes a steps operator.

require "flezha"
require_relative "echo_operator"

namespace = ARGV[0] || "default"

# the operators this runner serves
runner = Flezha.create_runner(namespace).register_factory(EchoOperator)

# `run` is `boot` followed by `await_termination`. Splitting them leaves room
# for the admin calls below, and still blocks until shutdown.
runner.boot # connects to the controller

# Everything `flz operator` and `flz plugin` do is also a method here.
# Uncomment to drive this namespace from the application instead of from a
# terminal:
#
# admin = runner.admin
# admin.start_operator("echo", id: "my_echo")
# admin.enable_plugin("writer", "operator_file_sink", opts: { "path" => "echo-out.txt" })
# admin.bind_plugin("writer", "my_echo", hooks: [["on_output", {}]])
# admin.send_to("my_echo", "hello world", channel: "in")
#
# docs/sdk/admin.md has the whole surface. An enforced controller refuses all
# of it until `flz role bind runner@default app default`.

# Blocks until shutdown: a SIGTERM, or standard input closing.
runner.await_termination
