# frozen_string_literal: true

require "rake/testtask"
require "rubygems/package_task"

SPEC = Gem::Specification.load("flezha.gemspec")

Rake::TestTask.new(:test) do |t|
  t.libs = %w[lib test]
  t.test_files = FileList["test/**/*_test.rb"]
  t.warning = true
  # `COVERAGE=1 rake test` measures the one suite this task defines. SimpleCov
  # has to be tracking before `flezha` is loaded, so it rides the interpreter's
  # own -r rather than a require inside the suite.
  t.ruby_opts << "-rsimplecov/autostart" if ENV["COVERAGE"]
end

# The platforms a release publishes a precompiled gem for, one per supported
# triple. `rake compile` builds the veneer for this Ruby into lib/flezha/;
# `rake native gem` packs it into a gem for this platform; and inside an
# rb-sys-dock image, which sets RUBY_TARGET and the Ruby minors to build for,
# `rake native:$RUBY_TARGET gem` packs one library per minor.
CROSS_PLATFORMS = %w[x86_64-linux aarch64-linux x86_64-darwin arm64-darwin x64-mingw-ucrt].freeze

# `rake gem` is the source gem; the native tasks hang their platform gems off it.
Gem::PackageTask.new(SPEC).define

# Only the native tasks need rake-compiler; `rake test` runs without it.
begin
  require "rake/extensiontask"
rescue LoadError
  warn "rake-compiler is not installed: the compile and native tasks are not defined"
end

if defined?(Rake::ExtensionTask)
  Rake::ExtensionTask.new("flz_core_ffi", SPEC) do |ext|
    ext.lib_dir = "lib/flezha"
    ext.cross_compile = true
    ext.cross_platform = CROSS_PLATFORMS
    # The library is in the gem, so nothing is built at install and the gem that
    # builds it is not needed there.
    ext.cross_compiling do |spec|
      spec.dependencies.reject! { |dependency| dependency.name == "rb_sys" }
    end
  end
end

task default: :test
