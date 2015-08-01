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
* `query` command (where, select and udf support)
* `scan` command (select and udf support)
* `batch` command (get and exists support)
* `udf` command (udf management: put, remove, list, get)
* Supported bytes type for non-native object types(string or fixnum) via [msgpack](https://github.com/msgpack/msgpack-ruby)
* lists and maps for bin value not supported yet (stored as bytes at the moment)
* Supported policies with all parameters for described commands
* Supported digest keys
* Supported exceptions (`AerospikeNative::Exception`) with several error codes constants `AerospikeNative::Exception.constants`
* Index management (`create_index` and `drop_index`)

## Examples

Located in path `examples`

Execute in gem root path command `ruby -Ilib:ext -r aerospike_native ./examples/batch.rb` or another example

Here is a list of examples:

* _batch.rb_ - batch command example
* _operate.rb_ - operate command example
* _put_get_remove.rb_ - key-value operatations example
* _query_and_index.rb_ - create/drop index and execute query
* _query_udf.rb_ - apply udf function to query operation
* _scan.rb_ - scan records
* _scan_udf.rb_ - apply udf function to scan operation

## Usage

### Basic

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

10.times do |i|
  client.put(AerospikeNative::Key.new('test', 'test', "key#{i}"), {'number' => i, 'name' => 'key'})
end
client.create_index('test', 'test', 'number', 'number_idx');

records = client.query('test', 'test').where(number: [1, 7])
puts records.inspect
records.each { |record| client.remove(record.key) }
```

### Logger

You can specify logger and log level

```ruby
AerospikeNative::Client.set_logger Rails.logger    # stdout by default
AerospikeNative::Client.set_log_level :debug       # :debug by default
```

## Contributing

1. Fork it ( https://github.com/rainlabs/aerospike_native/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
