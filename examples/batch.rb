require_relative './common/common'

def keys(namespace, set, *values)
  values.map{ |value| AerospikeNative::Key.new(namespace, set, value) }
end

def main
  Common::Common.run_example do |client, namespace, set, logger|
    20.times do |i|
      client.put(AerospikeNative::Key.new(namespace, set, i), {'number' => i, 'key' => 'number', 'testbin' => i.to_s})
      client.put(AerospikeNative::Key.new(namespace, set, "key#{i}"), {'number' => i, 'key' => 'string', 'testbin' => i.to_s})
    end

    bins = [:number, :key]

    records = client.batch.exists(keys(namespace, set, 1, 10, "key5", "key26"))
    logger.info "fetched records metadata: #{records.inspect}"

    records = client.batch.get(keys(namespace, set, 1, 10, "key5", "key26"))
    logger.info "fetched records with all bins: #{records.inspect}"

    records = client.batch.get(keys(namespace, set, 1, 10, "key5", "key26"), bins)
    logger.info "fetched records with specified bins: #{records.inspect}"
  end
end

main
