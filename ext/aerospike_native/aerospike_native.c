//#include <iostream>
#include "aerospike_native.h"
#include "client.h"
#include "key.h"
#include "operation.h"

VALUE AerospikeNativeClass;

void Init_aerospike_native()
{
    AerospikeNativeClass = rb_define_module("AerospikeNative");
    define_client();
    define_native_key();
    define_operation();
}
