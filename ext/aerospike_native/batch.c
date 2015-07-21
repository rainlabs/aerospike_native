#include "batch.h"
#include "client.h"
#include "record.h"
#include <aerospike/aerospike_batch.h>

VALUE BatchClass;

VALUE batch_initialize(VALUE vSelf, VALUE vClient)
{
    check_aerospike_client(vClient);
    rb_iv_set(vSelf, "@client", vClient);
    return vSelf;
}

bool batch_read_callback(const as_batch_read* results, uint32_t n, void* udata)
{
    uint32_t i = 0;
    VALUE vLogger;
    char sMsg[1000];

    vLogger = rb_cv_get(ClientClass, "@@logger");

    for(i = 0; i < n; i++) {
        switch(results[i].result) {
        case AEROSPIKE_OK: {
            VALUE vRecord = rb_record_from_c(&results[i].record, results[i].key);

            if ( rb_block_given_p() ) {
                rb_yield(vRecord);
            } else {
                VALUE *vArray = (VALUE*) udata;
                rb_ary_push(*vArray, vRecord);
            }
            break;
        }
        case AEROSPIKE_ERR_RECORD_NOT_FOUND:
            sprintf(sMsg, "Aerospike batch read record not found %d", i);
            rb_funcall(vLogger, rb_intern("warn"), 1, rb_str_new2(sMsg));
            break;
        default:
            sprintf(sMsg, "Aerospike batch read error %d", results[i].result);
            rb_funcall(vLogger, rb_intern("error"), 1, rb_str_new2(sMsg));
        }
    }

    return true;
}

VALUE batch_get(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKeys, vClient, vArray;

    as_batch batch;
    as_policy_batch policy;
    as_key* key;
    aerospike* ptr;
    as_error err;

    uint32_t n = 0, idx = 0;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    vKeys = vArgs[0];
    Check_Type(vKeys, T_ARRAY);

    as_policy_batch_init(&policy);
    if (argc == 2 && TYPE(vArgs[1]) != T_NIL) {
        SET_POLICY(policy, vArgs[1]);
    }

    idx = RARRAY_LEN(vKeys);
    as_batch_inita(&batch, idx);

    for(n = 0; n < idx; n++) {
        VALUE vKey = rb_ary_entry(vKeys, n);
        Data_Get_Struct(vKey, as_key, key);
        as_key_init_value(as_batch_keyat(&batch, n), key->ns, key->set, key->valuep);
    }

    vClient = rb_iv_get(vSelf, "@client");
    Data_Get_Struct(vClient, aerospike, ptr);

    vArray = rb_ary_new();
    if (aerospike_batch_get(ptr, &err, &policy, &batch, batch_read_callback, &vArray) != AEROSPIKE_OK) {
        as_batch_destroy(&batch);
        raise_aerospike_exception(err.code, err.message);
    }
    as_batch_destroy(&batch);

    if ( rb_block_given_p() ) {
        return Qnil;
    }

    return vArray;
}

VALUE batch_exists(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKeys, vClient, vArray;

    as_batch batch;
    as_policy_batch policy;
    as_key* key;
    aerospike* ptr;
    as_error err;

    uint32_t n = 0, idx = 0;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    vKeys = vArgs[0];
    Check_Type(vKeys, T_ARRAY);

    as_policy_batch_init(&policy);
    if (argc == 2 && TYPE(vArgs[1]) != T_NIL) {
        SET_POLICY(policy, vArgs[1]);
    }

    idx = RARRAY_LEN(vKeys);
    as_batch_inita(&batch, idx);

    for(n = 0; n < idx; n++) {
        VALUE vKey = rb_ary_entry(vKeys, n);
        Data_Get_Struct(vKey, as_key, key);
        as_key_init_value(as_batch_keyat(&batch, n), key->ns, key->set, key->valuep);
    }

    vClient = rb_iv_get(vSelf, "@client");
    Data_Get_Struct(vClient, aerospike, ptr);

    vArray = rb_ary_new();
    if (aerospike_batch_exists(ptr, &err, &policy, &batch, batch_read_callback, &vArray) != AEROSPIKE_OK) {
        as_batch_destroy(&batch);
        raise_aerospike_exception(err.code, err.message);
    }
    as_batch_destroy(&batch);

    if ( rb_block_given_p() ) {
        return Qnil;
    }

    return vArray;
}

void define_batch()
{
    BatchClass = rb_define_class_under(AerospikeNativeClass, "Batch", rb_cObject);
    rb_define_method(BatchClass, "initialize", batch_initialize, 1);
    rb_define_method(BatchClass, "get", batch_get, -1);
    rb_define_method(BatchClass, "exists", batch_exists, -1);

    rb_define_attr(BatchClass, "client", 1, 0);
}
