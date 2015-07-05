# AerospikeNative

Ruby [aerospike](http://www.aerospike.com/) client with c extension

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

* `operate` command with all operation types
* `put` command
* `get` command
* `remove` command
* `select` command
* `exixts?` command
* :disappointed: Raw (bytes) type is not supported
* Supported timeout for policies
* Supported digest keys
* Supported exceptions (`AerospikeNative::Exception`) with several error codes constants like `AerospikeNative::Exception::ERR_CLIENT`
* Index management (`create_index` and `drop_index`)

## Usage

```ruby
require 'aerospike_native'

client = AerospikeNative::Client.new([{'host' => '127.0.0.1', 'port' => 3000}])
key = AerospikeNative::Key.new('test', 'test', 'sample')
client.put(key, {'bin1' => 1, 'bin2' => 'test'}, {'timeout' => 1})
client.operate(key, [AerospikeNative::Operation.write('bin3', 25), AerospikeNative::Operation.increment('bin1', 2), AerospikeNative::Operation.append('bin1', '_aerospike')], {'timeout' => 1})

client.exists?(key)
record = client.get(key)
record = client.select(key, ['bin2', 'bin1'])
client.remove(key)
```

## Contributing

1. Fork it ( https://github.com/rainlabs/aerospike_native/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
