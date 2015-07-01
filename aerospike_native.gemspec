# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'aerospike_native/version'

Gem::Specification.new do |spec|
  spec.name          = "aerospike_native"
  spec.version       = AerospikeNative::VERSION
  spec.authors       = ["Vladimir Ziablitskii"]
  spec.email         = ["zyablitskiy@gmail.com"]
  spec.summary       = %q{Aerospike native client}
  spec.description   = %q{Aerospike ruby client with c extension}
  spec.homepage      = "https://github.com/rainlabs/aerospike_native"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib", "ext"]
  spec.extensions    = %w[ext/aerospike_native/extconf.rb]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rake-compiler"
  spec.add_development_dependency "rspec"
end
