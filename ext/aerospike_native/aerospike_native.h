#ifndef AEROSPIKE_NATIVE_H
#define AEROSPIKE_NATIVE_H

#include "common.h"
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>

RUBY_EXTERN VALUE AerospikeNativeClass;

enum IndexType {
    INDEX_STRING = AS_INDEX_STRING,
    INDEX_NUMERIC = AS_INDEX_NUMERIC
};

#endif // AEROSPIKE_NATIVE_H

