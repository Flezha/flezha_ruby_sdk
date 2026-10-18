# frozen_string_literal: true

require "test_helper"

# The activation record keys this SDK reads, held to the veneer that writes them.
class VeneerKeysTest < Minitest::Test
  READERS = %w[dispatch.rb state.rb].freeze

  def test_every_record_key_read_is_one_the_veneer_writes
    shape = File.read(Flezha::TestSupport.contract_file("flz_core_ffi/src/veneer/shape.rs"))
    written = shape.scan(/Key::(?:flat|of)\("([a-z_]+)"/).flatten.uniq
    read = READERS.flat_map do |name|
      File.read(File.expand_path("../lib/flezha/#{name}", __dir__))
          .scan(/\b(?:activation|item|request|body)\[:([a-z_]+)\]/).flatten
    end.uniq
    refute_empty read, "the record readers were found"
    assert_empty read - written, "read but never written by the veneer"
  end
end
