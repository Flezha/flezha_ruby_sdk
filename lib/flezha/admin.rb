# frozen_string_literal: true

require "json"

module Flezha
  # What a start answered: the id the operator runs under, the generated one
  # when the start named none, and `:started` or `:already_running`.
  StartResult = Struct.new(:id, :outcome, keyword_init: true)

  # Administering the namespace this runner's connection proved, from ordinary
  # application code.
  #
  # No tasklet has to be running and the application holds no credential of
  # its own: the controller judges every call as the identity the connection
  # already established, so nothing an unattended process holds expires. An
  # user grants a namespace's applications the authority once and revokes
  # it the same way, and a revocation takes effect on the next call:
  #
  #     flz role bind runner@team-orders app team-orders
  #
  # Every method here is one line over a single native entry point, because the
  # verb travels as data. Adding a verb costs a method and no native work.
  class Admin
    def initialize(core)
      @core = core
    end

    # Start an operator of `factory_id`, on the specification
    # `flz operator start` takes, answering a `StartResult`.
    #
    # Without an `id` the controller generates one, so a start retried after a
    # lost answer starts a second operator, and a second stream runs until it
    # is stopped. With an `id`, a start of a running one leaves it untouched
    # and answers `:already_running`.
    def start_operator(factory_id, id: nil, init_values: nil, opts: nil)
      fields = { "factory_id" => factory_id }
      fields["id"] = id if id
      fields["init_values"] = init_values if init_values
      fields["opts"] = opts if opts
      answer = call("Operator.start", fields)
      StartResult.new(id: answer.fetch("id"), outcome: answer.fetch("outcome").to_sym)
    end

    # Stop an operator, or the whole partition group it names.
    def stop_operator(operator_id) = call("Operator.stop", operator_id)

    # Inject a message into an operator's input stream, blocking for the verdict.
    def send_to(operator_id, message, channel: "")
      call("Operator.send", operator_id, message, channel)
    end

    # Enable a plugin, built from the fields `flz plugin enable` takes.
    def enable_plugin(plugin_id, fields) = call("PluginManager.enable", plugin_id, fields)

    # Disable a plugin, or every partitioned instance of it.
    def disable_plugin(plugin_id) = call("PluginManager.disable", plugin_id)

    # The operator ids running in this namespace.
    def list_operators = call("Operator.list_operators")

    # The plugins enabled on the controller node answering this call.
    def list_plugins = call("PluginManager.list")

    # One cell's live state, and the change sequence it was read at.
    def cell_state(operator_id, cell_key)
      call("Operator.cell_state", operator_id, cell_key)
    end

    # Any published function, by name. What succeeds follows from the role the
    # namespace is bound to, never from a list kept here. The arguments are the
    # call's own other than the namespace, which the controller fills from the
    # connection.
    def call(verb, *args)
      JSON.parse(@core.admin_call(verb, JSON.generate(args)))
    end
  end
end
