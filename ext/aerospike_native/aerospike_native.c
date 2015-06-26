//#include <iostream>
#include "aerospike_native.h"
#include "client.h"

VALUE AerospikeNative;

void Init_aerospike_native()
{
    AerospikeNative = rb_define_module("AerospikeNative");
    define_client();
}
