require_relative './common/common'

def main
  Common::Common.run_example do |client, namespace, set, logger|
    20.times do |i|
      client.put(AerospikeNative::Key.new(namespace, set, i), {'number' => i, 'key' => 'number', 'testbin' => i.to_s})
    end

    records = client.scan(namespace, set).exec
    logger.info "scan records with all bins: #{records.inspect}"

    records = client.scan(namespace, set).select(:number, :testbin).exec
    logger.info "scan records with specified bins: #{records.inspect}"
  end
end

main
