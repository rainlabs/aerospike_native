require_relative './common/common'

def main
  Common::Common.run_example do |client, namespace, set, logger|
    keys = []
    keys << AerospikeNative::Key.new(namespace, set, 1)
    keys << AerospikeNative::Key.new(namespace, set, "hello")
    keys << AerospikeNative::Key.new(namespace, set, 2.5)

    keys.each do |key|
      next if client.exists?(key)

      client.put(key, {'testbin' => 1, 'testbin2' => 2.5, 'testbin3' => "string", 'testbin4' => [1, 2, 3]})
      logger.info "GET #{key.inspect} - #{client.get(key).inspect}"
      client.remove key
    end
  end
end

main

