#include "aerospike_native.h"
#include "client.h"
#include "key.h"
#include "operation.h"
#include "record.h"
#include "policy.h"
#include "query.h"

VALUE AerospikeNativeClass;

void Init_aerospike_native()
{
    AerospikeNativeClass = rb_define_module("AerospikeNative");
    define_exception();
    define_logger();
    define_query();
    define_native_key();
    define_record();
    define_operation();
    define_policy();
    define_client();

    rb_define_const(AerospikeNativeClass, "INDEX_NUMERIC", INT2FIX(INDEX_NUMERIC));
    rb_define_const(AerospikeNativeClass, "INDEX_STRING", INT2FIX(INDEX_STRING));
}
