#require "bundler/gem_tasks"

require 'rake/extensiontask'
require 'rubygems/package_task'

##
# Rake::ExtensionTask comes from the rake-compiler and understands how to
# build and cross-compile extensions.
#
# See https://github.com/luislavena/rake-compiler for details

Rake::ExtensionTask.new 'aerospike_native' do |ext|

  # This causes the shared object to be placed in lib/my_malloc/my_malloc.so
  #
  # It allows lib/my_malloc.rb to load different versions if you ship a
  # precompiled extension that supports multiple ruby versions.

  ext.lib_dir = 'lib/aerospike_native'
end

s = Gem::Specification.new 'aerospike_native', '0.0.1' do |s|
  s.summary = 'simple aerospike extenstion'
  s.authors = %w[zyablitskiy@gmail.com]

  # this tells RubyGems to build an extension upon install

  s.extensions = %w[ext/aerospike_native/extconf.rb]

  # naturally you must include the extension source in the gem

  s.files = `git ls-files`.split($/)
end

# The package task builds the gem in pkg/my_malloc-1.0.gem so you can test
# installing it.

Gem::PackageTask.new s do end

# This isn't a good test, but does provide a sanity check

task test: %w[compile] do
  ruby '-Ilib', '-raerospike_native', '-e', "p AerospikeNative::Key.new('namespace', 'set', 'value')"
end

task default: :test

