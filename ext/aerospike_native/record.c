#include "record.h"
#include "key.h"
#include "client.h"

VALUE RecordClass;

/*
 * call-seq:
 *   new(key, bins, gen, ttl) -> AerospikeNative::Record
 *
 * initialize new record
 */
VALUE record_initialize(VALUE vSelf, VALUE vKey, VALUE vBins, VALUE vGen, VALUE vTtl)
{
    check_aerospike_key(vKey);
    Check_Type(vBins, T_HASH);
    Check_Type(vGen, T_FIXNUM);
    Check_Type(vTtl, T_FIXNUM);

    rb_iv_set(vSelf, "@key", vKey);
    rb_iv_set(vSelf, "@bins", vBins);
    rb_iv_set(vSelf, "@gen", vGen);
    rb_iv_set(vSelf, "@ttl", vTtl);

    return vSelf;
}

VALUE rb_record_from_c(as_record* record, as_key* key)
{
    VALUE vKeyParams[5], vParams[4];
    VALUE vLogger;
    as_key current_key;
    as_bin bin;
    int n;
    char msg[200];

    if (key == NULL) {
        current_key = record->key;
    } else {
        current_key = *key;
    }

    vKeyParams[0] = rb_str_new2(current_key.ns);
    vKeyParams[1] = rb_str_new2(current_key.set);
    vKeyParams[2] = Qnil;

    if (current_key.valuep != NULL) {
        switch( as_val_type(current_key.valuep) ) {
        case AS_INTEGER:
            vKeyParams[2] = INT2NUM(as_integer_get(current_key.valuep));
            break;
        case AS_STRING:
            if (current_key.value.string.len > 0) {
                vKeyParams[2] = rb_str_new2(as_string_get(current_key.valuep));
            }
            break;
        case AS_BYTES: {
            VALUE vString = rb_str_new(as_bytes_get(current_key.valuep), as_bytes_size(current_key.valuep));
            vKeyParams[2] = rb_funcall(MsgPackClass, rb_intern("unpack"), 1, vString);
            break;
        }
        }
    }
    vKeyParams[3] = rb_str_new(current_key.digest.value, AS_DIGEST_VALUE_SIZE);

    vParams[0] = rb_class_new_instance(4, vKeyParams, KeyClass);
    vParams[1] = rb_hash_new();
    vParams[2] = UINT2NUM(record->gen);
    vParams[3] = UINT2NUM(record->ttl);

    MsgPackClass = rb_const_get(rb_cObject, rb_intern("MessagePack"));
    for(n = 0; n < record->bins.size; n++) {
        bin = record->bins.entries[n];
        switch( as_val_type(bin.valuep) ) {
        case AS_NIL:
            rb_hash_aset(vParams[1], rb_str_new2(bin.name), Qnil);
            break;
        case AS_INTEGER:
            rb_hash_aset(vParams[1], rb_str_new2(bin.name), LONG2NUM(as_integer_get(bin.valuep)));
            break;
        case AS_STRING:
            rb_hash_aset(vParams[1], rb_str_new2(bin.name), rb_str_new2(as_string_get(bin.valuep)));
            break;
        case AS_BYTES: {
            VALUE vString = rb_str_new(as_bytes_get(bin.valuep), as_bytes_size(bin.valuep));
            rb_hash_aset(vParams[1], rb_str_new2(bin.name), rb_funcall(MsgPackClass, rb_intern("unpack"), 1, vString));
            break;
        }
        case AS_UNDEF:
        default:
            vLogger = rb_cv_get(ClientClass, "@@logger");
            sprintf(msg, "unhandled val type: %d\n", as_val_type(bin.valuep));
            rb_funcall(vLogger, rb_intern("warn"), 1, rb_str_new2(msg));
            break;
        }
    }

    as_record_destroy(record);
    return rb_class_new_instance(4, vParams, RecordClass);
}

void define_record()
{
    RecordClass = rb_define_class_under(AerospikeNativeClass, "Record", rb_cObject);
    rb_define_method(RecordClass, "initialize", record_initialize, 4);
    rb_define_attr(RecordClass, "key", 1, 0);
    rb_define_attr(RecordClass, "bins", 1, 0);
    rb_define_attr(RecordClass, "gen", 1, 0);
    rb_define_attr(RecordClass, "ttl", 1, 0);
}



