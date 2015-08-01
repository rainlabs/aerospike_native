#require "bundler/gem_tasks"

require 'rake/extensiontask'
require 'rubygems/package_task'
require_relative './lib/aerospike_native/version'

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

s = Gem::Specification.new 'aerospike_native', AerospikeNative::VERSION do |spec|
  spec.name          = "aerospike_native"
  spec.version       = AerospikeNative::VERSION
  spec.platform      = Gem::Platform::RUBY
  spec.authors       = ["Vladimir Ziablitskii"]
  spec.email         = ["zyablitskiy@gmail.com"]
  spec.summary       = %q{Aerospike native client}
  spec.description   = %q{Unofficial Aerospike Client for ruby with c extension (official aerospike c client)}
  spec.homepage      = "https://github.com/rainlabs/aerospike_native"
  spec.license       = "MIT"
  #  spec.required_ruby_version = '>= 1.9.3'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib", "ext"]
  spec.extensions    = %w[ext/aerospike_native/extconf.rb]

  spec.add_dependency "msgpack", "~> 0.6"

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rake-compiler", "~> 0.9"
  spec.add_development_dependency "rspec", "~> 3.3"
end

# The package task builds the gem in pkg/my_malloc-1.0.gem so you can test
# installing it.

Gem::PackageTask.new s do end

# This isn't a good test, but does provide a sanity check

task test: %w[compile] do
  ruby '-Ilib', '-raerospike_native', '-e', "p AerospikeNative::Key.new('namespace', 'set', 'value')"
end

task default: :test

