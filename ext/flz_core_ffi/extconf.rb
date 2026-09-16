# frozen_string_literal: true

# Build the native veneer for the source gem.
#
# A precompiled platform gem ships the library and needs no toolchain at all;
# that is the primary distribution, and this file is the fallback for platforms
# it does not cover. The fallback builds the wrapper crate beside this file,
# whose one dependency is the core at a git tag, so it needs cargo and git and
# says so rather than failing inside cargo.

require "mkmf"

ext_dir = __dir__
prebuilt = File.expand_path("../../lib/flezha/flz_core_ffi.#{RbConfig::CONFIG['DLEXT']}", ext_dir)

# A checkout whose library is already built, installed in place by Bundler.
# Only in place: rake-compiler builds in a directory of its own and copies what
# make produced, so a Makefile that produces nothing would fail it.
if File.exist?(prebuilt) && File.expand_path(Dir.pwd) == ext_dir
  File.write("Makefile", "all:\n\t@true\ninstall:\n\t@true\nclean:\n\t@true\n")
  exit 0
end

missing = %w[cargo git].reject { |tool| find_executable(tool) }
unless missing.empty?
  abort <<~MESSAGE
    flezha: building the source gem needs cargo and git (#{missing.join(' and ')} not found):
    it compiles the core from its git tag. Install a Rust toolchain and git, or install
    the precompiled platform gem instead: gem install flezha --platform #{Gem::Platform.local}
  MESSAGE
end

require "rb_sys/mkmf"

create_rust_makefile("flezha/flz_core_ffi") do |r|
  # rb-sys runs bindgen over ruby.h, and bindgen needs clang's builtin headers.
  # Without libclang-dev they are absent, and gcc's builtin include directory
  # carries the same ones.
  if ENV["BINDGEN_EXTRA_CLANG_ARGS"].nil? && Dir.glob("/usr/lib/llvm-*/lib/clang/*/include/stdarg.h").empty?
    gcc_include = `gcc -print-file-name=include 2>/dev/null`.strip
    r.env = { "BINDGEN_EXTRA_CLANG_ARGS" => "-I#{gcc_include}" } if File.file?(File.join(gcc_include, "stdarg.h"))
  end
end
