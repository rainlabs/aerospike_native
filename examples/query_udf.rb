require_relative './common/common'

def main
  Common::Common.run_example do |client, namespace, set, logger|
    logger.info "Found user scripts: #{client.udf.list}"

    ruby_sum = 0
    20.times do |i|
      ruby_sum += i
      client.put(AerospikeNative::Key.new(namespace, set, i), {'number' => i, 'key' => 'number', 'testbin' => i.to_s})
    end

    client.udf.put("./examples/lua/test_udf.lua")
    logger.info "adding user script..."
    client.udf.wait("test_udf.lua", 1000)
    logger.info "Found user scripts: #{client.udf.list}"
    logger.info "user script already added"

    logger.info "perform a aggregate query"
    sum = client.query(namespace, set).apply("test_udf", "sum_number").exec.first
    logger.info "ruby sum #{ruby_sum}"
    logger.info "aerospike sum #{sum}"

    logger.info "removing user script..."
    client.udf.remove("test_udf.lua")
    logger.info "Found user scripts: #{client.udf.list}"
  end
end

main

