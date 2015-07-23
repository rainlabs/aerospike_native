require './common/common'

def main
  Common::Common.run_example do |client, namespace, set, logger|
    20.times do |i|
      client.put(AerospikeNative::Key.new(namespace, set, i), {'number' => i, 'key' => 'number', 'testbin' => i.to_s})
      client.put(AerospikeNative::Key.new(namespace, set, "key#{i}"), {'number' => i, 'key' => 'string', 'testbin' => i.to_s})
    end

    keys = []
    keys << AerospikeNative::Key.new(namespace, set, 1)
    keys << AerospikeNative::Key.new(namespace, set, 10)
    keys << AerospikeNative::Key.new(namespace, set, "key5")
    keys << AerospikeNative::Key.new(namespace, set, "key26")

    records = client.batch.get_exists(keys)
    logger.info "fetched records metadata: #{records.inspect}"

    records = client.batch.get(keys)
    logger.info "fetched records with specified bins: #{records.inspect}"

    records = client.batch.get(keys, bins)
    logger.info "fetched records with all bins: #{records.inspect}"
  end
end

main
