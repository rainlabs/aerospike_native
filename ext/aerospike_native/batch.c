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
    char sMsg[1000];

    for(i = 0; i < n; i++) {
        VALUE vRecord = Qnil;

        switch(results[i].result) {
        case AEROSPIKE_OK: {
            vRecord = rb_record_from_c(&results[i].record, results[i].key);
            break;
        }
        case AEROSPIKE_ERR_RECORD_NOT_FOUND:
            sprintf(sMsg, "Aerospike batch read record not found %d", i);
            rb_funcall(LoggerInstance, rb_intern("warn"), 1, rb_str_new2(sMsg));
            break;
        default:
            sprintf(sMsg, "Aerospike batch read error %d", results[i].result);
            rb_funcall(LoggerInstance, rb_intern("error"), 1, rb_str_new2(sMsg));
        }

        if ( rb_block_given_p() ) {
            rb_yield(vRecord);
        } else {
            VALUE *vArray = (VALUE*) udata;
            rb_ary_push(*vArray, vRecord);
        }
    }

    return true;
}

/*
 * call-seq:
 *   get(keys) -> Array
 *   get(keys, bins) -> Array
 *   get(keys, bins, policy_settings) -> Array
 *   get(keys, policy_settings) -> Array
 *   get(keys, ...) { |record| ... } -> Nil
 *
 * batch get records by keys with specified bins
 */
VALUE batch_get(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKeys, vClient, vArray, vBins;

    as_batch batch;
    as_policy_batch policy;
    as_key* key;
    aerospike* ptr;
    as_error err;

    uint32_t n = 0, idx = 0, bins_idx = 0;

    if (argc > 3 || argc < 1) {  // there should only be 1, 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..3)", argc);
    }

    vKeys = vArgs[0];
    Check_Type(vKeys, T_ARRAY);
    as_policy_batch_init(&policy);

    if (argc == 3) {
        vBins = vArgs[1];
        Check_Type(vBins, T_ARRAY);
        bins_idx = RARRAY_LEN(vBins);

        if (TYPE(vArgs[2]) != T_NIL) {
            SET_BATCH_POLICY(policy, vArgs[2]);
        }
    } else {
        switch(TYPE(vArgs[1])) {
        case T_NIL:
            break;
        case T_ARRAY:
            vBins = vArgs[1];
            bins_idx = RARRAY_LEN(vBins);
            break;
        case T_HASH: {
            SET_POLICY(policy, vArgs[1]);
            break;
        }
        default:
            rb_raise(rb_eTypeError, "wrong argument type (expected Array or Hash)");
        }
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
    if (bins_idx > 0) {
        char* sBins[bins_idx];
        for(n = 0; n < bins_idx; n++) {
            VALUE vEl = rb_ary_entry(vBins, n);
            GET_STRING(vEl);
            sBins[n] = StringValueCStr(vEl);
        }
        if (aerospike_batch_get_bins(ptr, &err, &policy, &batch, sBins, bins_idx, batch_read_callback, &vArray) != AEROSPIKE_OK) {
            as_batch_destroy(&batch);
            raise_aerospike_exception(err.code, err.message);
        }
    } else {
        if (aerospike_batch_get(ptr, &err, &policy, &batch, batch_read_callback, &vArray) != AEROSPIKE_OK) {
            as_batch_destroy(&batch);
            raise_aerospike_exception(err.code, err.message);
        }
    }

    as_batch_destroy(&batch);
    if ( rb_block_given_p() ) {
        return Qnil;
    }

    return vArray;
}

// TODO: implement batch read to customize bins for each key
// VALUE batch_read(int argc, VALUE* vArgs, VALUE vSelf);

/*
 * call-seq:
 *   exists(keys) -> Array
 *   exists(keys, policy_settings) -> Array
 *   exists(keys, ...) { |record| ... } -> Nil
 *   exists(keys, policy_settings) { |record| ... } -> Nil
 *
 * batch get metatdata of records (ttl, gen)
 */
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
        SET_BATCH_POLICY(policy, vArgs[1]);
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
