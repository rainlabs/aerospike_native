require './common/common'

def main
  Common::Common.run_example do |client, namespace, set, logger|
    index_name = "number_#{set}_idx"
    client.create_index(namespace, set, 'number', index_name)

    20.times do |i|
      client.put(AerospikeNative::Key.new(namespace, set, i), {'number' => i, 'key' => 'number', 'testbin' => i.to_s})
    end

    records = client.query(namespace, set).select(:number, :testbin).where(number: [10, 15]).exec
    logger.info records.inspect

    client.drop_index(namespace, index_name)
  end
end

main