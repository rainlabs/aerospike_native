#include "aerospike_native.h"
#include "client.h"
#include "key.h"
#include "operation.h"
#include "record.h"
#include "policy.h"
#include "query.h"
#include "batch.h"
#include "scan.h"

VALUE AerospikeNativeClass;
VALUE MsgPackClass;

void Init_aerospike_native()
{
    MsgPackClass = rb_const_get(rb_cObject, rb_intern("MessagePack"));
    AerospikeNativeClass = rb_define_module("AerospikeNative");
    define_exception();
    define_logger();
    define_query();
    define_scan();
    define_batch();
    define_native_key();
    define_record();
    define_operation();
    define_policy();
    define_client();

    rb_define_const(AerospikeNativeClass, "INDEX_NUMERIC", INT2FIX(INDEX_NUMERIC));
    rb_define_const(AerospikeNativeClass, "INDEX_STRING", INT2FIX(INDEX_STRING));
}
