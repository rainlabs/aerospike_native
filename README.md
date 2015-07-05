# AerospikeNative

Ruby [aerospike](http://www.aerospike.com/) client with c extension (unstable)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'aerospike_native'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install aerospike_native
    
## Current status

* `operate` command with _write_, _increment_, _append_, _prepend_, _touch_
* `put` command
* `get` command
* :disappointed: Raw (bytes) type is not supported
* Supported timeout for policies
* Supported digest keys
* Supported exceptions (`AerospikeNative::Exception`) with several error codes constants like `AerospikeNative::Exception::ERR_CLIENT`

## Usage

```ruby
require 'aerospike_native'

client = AerospikeNative::Client.new([{'host' => '127.0.0.1', 'port' => 3000}])
key = AerospikeNative::Key.new('test', 'test', 'sample')
client.put(key, {'bin1' => 1, 'bin2' => 'test'}, {'timeout' => 1})
client.operate(key, [AerospikeNative::Operation.write('bin3', 25), AerospikeNative::Operation.increment('bin1', 2), AerospikeNative::Operation.append('bin1', '_aerospike')], {'timeout' => 1})

record = client.get(key)
```

## Contributing

1. Fork it ( https://github.com/rainlabs/aerospike_native/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
