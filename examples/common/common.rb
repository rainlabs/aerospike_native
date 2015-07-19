require 'aerospike_native'
require 'logger'

module Common
  class Common
    class << self
      def namespace; 'test' end
      def set; 'examples' end
      def client; @@client ||= AerospikeNative::Client.new([{'host' => '127.0.0.1', 'port' => 3010}]) end

      def cleanup
        client.query(namespace, set).exec{ |record| client.remove(record.key) }
      end

      def custom_logger
        @@logger = Logger.new(STDOUT, Logger::DEBUG)
      end

      def set_custom_logger
        AerospikeNative::Client.set_logger custom_logger
        AerospikeNative::Client.set_log_level :debug
      end

      def init
        set_custom_logger
        client
      end

      def run_example
        init
        custom_logger.info "Example started"
        yield(client, namespace, set, custom_logger)
        cleanup
        custom_logger.info "Example finished"
      end
    end
  end
end