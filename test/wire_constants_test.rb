# frozen_string_literal: true

require "test_helper"

# This runtime's wire constants, held to the shared constants fixture.
class WireConstantsTest < Minitest::Test
  FIXTURE = Flezha::TestSupport.contract_file("test/fixtures/wire_constants.txt")

  def test_the_language_byte_codec_and_schema_are_the_fixtures
    assert_equal Integer(row("sdk_lang", "ruby")), Flezha::Graph::SDK_LANG_RUBY
    assert_equal Integer(row("format_id", "msgpack")), Flezha::Graph::FORMAT_ID_MSGPACK
    assert_equal Integer(row("schema_version", "activation")), Flezha::ACTIVATION_SCHEMA_VERSION
  end

  def test_the_output_stamps_and_the_reserved_prefix_are_the_fixture_keys
    assert_equal [row("flz_key", "cell"), row("flz_key", "seq"), row("flz_key", "key")],
                 [Flezha::Ctx::CELL_STAMP, Flezha::Ctx::SEQ_STAMP, Flezha::Ctx::KEY_STAMP]
    assert_equal row("flz_key", "key")[/\A[^.]*\./], Flezha::Ctx::RESERVED_PREFIX
  end

  private

  def row(table, name)
    line = File.readlines(FIXTURE, chomp: true).find { |l| l.start_with?("#{table}|#{name}|") }
    refute_nil line, "#{FIXTURE} has no row #{table}|#{name}"
    line.split("|")[2]
  end
end
