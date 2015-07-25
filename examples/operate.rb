require_relative './common/common'

def main
  Common::Common.run_example do |client, namespace, set, logger|
    key = AerospikeNative::Key.new(namespace, set, "operate_test")
    bins = []
    bins << AerospikeNative::Operation.write('value', 'beep')
    bins << AerospikeNative::Operation.write('incr', 5)

    record = client.operate(key, bins)
    logger.info "Without read or touch: #{record.inspect}"

    bins = []
    bins << AerospikeNative::Operation.touch
    bins << AerospikeNative::Operation.append('value', '_once')
    bins << AerospikeNative::Operation.increment('incr', 10)

    record = client.operate(key, bins)
    logger.info "With touch: #{record.inspect}"

    bins = []
    bins << AerospikeNative::Operation.read('value')
    bins << AerospikeNative::Operation.read('incr')
    bins << AerospikeNative::Operation.prepend('value', 'twice_')

    record = client.operate(key, bins)
    logger.info "With read: #{record.inspect}"
  end
end

main
