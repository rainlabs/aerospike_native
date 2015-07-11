#include "aerospike_native.h"
#include "client.h"
#include "key.h"
#include "operation.h"
#include "record.h"
#include "condition.h"

VALUE AerospikeNativeClass;

void Init_aerospike_native()
{
    AerospikeNativeClass = rb_define_module("AerospikeNative");
    define_exception();
    define_client();
    define_native_key();
    define_record();
    define_operation();
    define_condition();

    rb_define_const(AerospikeNativeClass, "INDEX_NUMERIC", INT2FIX(INDEX_NUMERIC));
    rb_define_const(AerospikeNativeClass, "INDEX_STRING", INT2FIX(INDEX_STRING));
}
