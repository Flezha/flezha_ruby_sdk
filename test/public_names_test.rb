# frozen_string_literal: true

require_relative "test_helper"
require "flezha/testing"

# Every public concept spelled as `test/fixtures/sdk_public_names.txt` spells it.
#
# A Ruby cell is a constant path, then `#method` for a public instance method
# or `.method` for a singleton one, optionally followed by `(keyword)` for a
# keyword the method takes. A cell starting with `!` names something that must
# not be public, so a re-exported internal fails here as a renamed concept does.
class PublicNamesTest < Minitest::Test
  FIXTURE = Flezha::TestSupport.contract_file("test/fixtures/sdk_public_names.txt")
  COLUMN = 2 # concept|python|ruby|php|node|elixir|java|rust
  CELL = /\A(?<owner>[\w:]+)(?:(?<kind>[#.])(?<member>\w+[?!=]?))?(?:\((?<keyword>\w+)\))?\z/

  def test_every_concept_has_its_one_public_spelling
    wrong = cells.filter_map do |concept, path|
      name = path.delete_prefix("!")
      hidden = name != path
      "#{concept}: `#{name}` is #{hidden ? 'public' : 'missing'}" if resolves?(name) == hidden
    end
    assert_empty wrong
  end

  private

  def cells
    File.readlines(FIXTURE, chomp: true).flat_map do |line|
      next [] if line.strip.empty? || line.start_with?("#")

      concept, *columns = line.split("|")
      columns[COLUMN - 1].split.reject { |path| path == "-" }.map { |path| [concept, path] }
    end
  end

  def resolves?(path)
    cell = CELL.match(path) or raise ArgumentError, "unreadable cell #{path}"
    return false unless Object.const_defined?(cell[:owner])
    return true unless cell[:kind]

    method = public_method(Object.const_get(cell[:owner]), cell[:kind], cell[:member].to_sym)
    !method.nil? && (cell[:keyword].nil? || method.parameters.any? { |_, name| name.to_s == cell[:keyword] })
  end

  def public_method(owner, kind, name)
    if kind == "#"
      owner.public_method_defined?(name) ? owner.instance_method(name) : nil
    else
      owner.respond_to?(name) ? owner.method(name) : nil
    end
  end
end
