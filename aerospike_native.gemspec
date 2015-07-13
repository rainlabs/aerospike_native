# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'aerospike_native/version'

Gem::Specification.new do |spec|
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

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rake-compiler", "~> 0.9"
  spec.add_development_dependency "rspec", "~> 3.3"
end
