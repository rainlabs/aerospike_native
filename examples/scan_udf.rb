require_relative './common/common'

def main
  Common::Common.run_example do |client, namespace, set, logger|
    logger.info "Found user scripts: #{client.udf.list}"

    3.times do |i|
      client.put(AerospikeNative::Key.new(namespace, set, i), {'number' => i, 'key' => 'number', 'testbin' => i})
    end

    client.udf.put("./examples/lua/test_udf.lua")
    logger.info "adding user script..."
    client.udf.wait("test_udf.lua", 1000)
    logger.info "Found user scripts: #{client.udf.list}"
    logger.info "user script already added"

    logger.info "performing update scan..."
    scan_id = client.scan(namespace, set).apply("test_udf", "add_testbin_to_number").set_background(true).exec
    loop do
      info = client.scan_info(scan_id)
      logger.info "scan info: #{info}"
      break if info['status'] == AerospikeNative::Scan::STATUS_COMPLETED
      sleep 1
    end
    records = client.query(namespace, set).exec
    logger.info records.map(&:bins).inspect

    logger.info "removing user script..."
    client.udf.remove("test_udf.lua")
    logger.info "Found user scripts: #{client.udf.list}"
  end
end

main


