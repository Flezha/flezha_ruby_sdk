# frozen_string_literal: true

require_relative "test_helper"

# A relay's API as the declaration announces it: each channel's codec, an
# entry's description and JSON Schema, and the schemas the core refuses.
class ApiTest < Minitest::Test
  ORDER = {
    "type" => "object",
    "properties" => { "pair" => { "type" => "string" }, "amount" => { "type" => "number" } },
    "required" => %w[pair amount]
  }.freeze

  class Place < Flezha::Tasklet
    tasklet_name "place"
    def run(input, ctx) = ctx.emit(input)
  end

  class Audit < Flezha::Tasklet
    tasklet_name "audit"
    def run(_input, _ctx) = nil
  end

  def orders(**entry)
    klass = Class.new(Flezha::Relay)
    klass.factory_id "orders"
    klass.channels do |c|
      c.input("place", **entry).into(Place).output("placed")
      c.input("audit", codec: :text).into(Audit)
    end
    klass.spec
  end

  def api_of(spec)
    Flezha::Graph.to_host_class_decl(spec)["channels"].to_h do |channel|
      [channel["name"], channel.slice("codec", "schema", "description")]
    end
  end

  def validate(spec)
    skip "needs the compiled veneer (rake compile)" unless Flezha::TestSupport.native?

    Flezha::TestSupport.validate(spec)
  end

  def test_an_entry_announces_its_codec_description_and_schema_an_output_its_codec
    spec = orders(description: "Places an order", schema: ORDER)
    validate(spec)

    assert_equal({ "place" => { "codec" => "json", "schema" => ORDER, "description" => "Places an order" },
                   "audit" => { "codec" => "text" },
                   "placed" => { "codec" => "json" } }, api_of(spec))
  end

  def test_a_decoder_of_its_own_makes_an_entry_custom
    assert_equal "custom", api_of(orders(decode: ->(bytes) { bytes }))["place"]["codec"]
  end

  def test_a_schema_that_is_not_an_object_with_a_type_is_refused_at_registration
    ["object", { "properties" => {} }].each do |schema|
      error = assert_raises(Flezha::Error) { validate(orders(schema: schema)) }
      issue = error.details["issues"].first

      assert_equal %w[topology.schema_invalid place], [issue["code"], issue["details"]["channel"]]
    end
  end

  def test_a_schema_on_a_channel_that_is_not_json_is_refused_at_registration
    error = assert_raises(Flezha::Error) { validate(orders(codec: :text, schema: ORDER)) }

    assert_equal ["topology.schema_invalid"], Flezha::TestSupport.codes(error)
  end
end
