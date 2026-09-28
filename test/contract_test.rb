# frozen_string_literal: true

require_relative "test_helper"
require "fileutils"
require "tmpdir"

class ContractTest < Minitest::Test
  def test_an_in_tree_copy_does_not_shadow_the_checkout
    Dir.mktmpdir do |checkout|
      package = File.join(checkout, "sdk/ruby")
      [checkout, File.join(package, "contract")].each { |tree| carry_manifest(tree) }

      assert_equal checkout, Flezha::TestSupport.locate_contract(package, nil)

      File.delete(File.join(checkout, Flezha::TestSupport::MANIFEST))
      assert_equal File.join(package, "contract"), Flezha::TestSupport.locate_contract(package, nil)
    end
  end

  private

  def carry_manifest(tree)
    FileUtils.mkdir_p(File.join(tree, "test/fixtures"))
    File.write(File.join(tree, Flezha::TestSupport::MANIFEST), "")
  end
end
