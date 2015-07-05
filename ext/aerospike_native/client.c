#include "client.h"
#include "operation.h"
#include "key.h"
#include "record.h"
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/aerospike_key.h>

VALUE ClientClass;

static void client_deallocate(void *p)
{
    aerospike* ptr = p;
    as_error err;

    aerospike_close(ptr, &err);
    aerospike_destroy(ptr);
}

static VALUE client_allocate(VALUE klass)
{
    VALUE obj;
    aerospike *ptr;

    obj = Data_Make_Struct(klass, aerospike, NULL, client_deallocate, ptr);

    return obj;
}

VALUE client_initialize(int argc, VALUE* argv, VALUE self)
{
    VALUE ary = Qnil;
    aerospike *ptr;
    as_config config;
    as_error err;
    long idx = 0, n = 0;

    if (argc > 1) {  // there should only be 0 or 1 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 0..1)", argc);
    }

    if (argc == 1) {
        ary = argv[0];
    }

    switch (TYPE(ary)) {
    case T_NIL:
    case T_ARRAY:
        break;
    default:
        /* raise exception */
        Check_Type(ary, T_ARRAY);
        break;
    }
    Data_Get_Struct(self, aerospike, ptr);

    as_config_init(&config);

    if (TYPE(ary) == T_ARRAY) {
        idx = RARRAY_LEN(ary);
        for(n = 0; n < idx; n++) {
            VALUE hash = rb_ary_entry(ary, n);
            VALUE host = rb_hash_aref(hash, rb_str_new2("host"));
            VALUE port = rb_hash_aref(hash, rb_str_new2("port"));
            printf("host: %s:%d\n", StringValueCStr(host), NUM2UINT(port));
            as_config_add_host(&config, StringValueCStr(host), NUM2UINT(port));
        }
    }

    if (idx == 0) {
        as_config_add_host(&config, "127.0.0.1", 3000);
    }

    aerospike_init(ptr, &config);

    if ( aerospike_connect(ptr, &err) != AEROSPIKE_OK ) {
        raise_aerospike_exception(err.code, err.message);
    }
    return self;
}

VALUE client_put(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;
    VALUE vBins;
    VALUE vSettings;
    VALUE vHashKeys;

    aerospike *ptr;
    as_key* key;
    as_error err;
    as_record record;
    as_policy_write policy;

    int idx = 0, n = 0;

    if (argc > 3 || argc < 2) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..3)", argc);
    }

    vKey = vArgs[0];
    check_aerospike_key(vKey);

    vBins = vArgs[1];
    Check_Type(vBins, T_HASH);

    as_policy_write_init(&policy);

    if (argc == 3) {
        vSettings = vArgs[2];

        switch (TYPE(vSettings)) {
        case T_NIL:
            break;
        case T_HASH: {
            VALUE vTimeout = rb_hash_aref(vSettings, rb_str_new2("timeout"));
            if (TYPE(vTimeout) == T_FIXNUM) {
                policy.timeout = NUM2UINT( vTimeout );
            }
            break;
        }
        default:
            /* raise exception */
            Check_Type(vSettings, T_HASH);
            break;
        }
    }

    idx = RHASH_SIZE(vBins);
    if (idx == 0) {
        return Qfalse;
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    as_record_inita(&record, idx);
    vHashKeys = rb_hash_keys(vBins);

    for(n = 0; n < idx; n++) {
        VALUE bin_name = rb_ary_entry(vHashKeys, n);
        VALUE bin_value = rb_hash_aref(vBins, bin_name);

        Check_Type(bin_name, T_STRING);

        switch( TYPE(bin_value) ) {
        case T_STRING:
            as_record_set_str(&record, StringValueCStr(bin_name), StringValueCStr(bin_value));
            break;
        case T_FIXNUM:
            as_record_set_int64(&record, StringValueCStr(bin_name), NUM2LONG(bin_value));
            break;
        default:
            rb_raise(rb_eArgError, "Incorrect input type (expected string or fixnum)");
            break;
        }
     }

    Data_Get_Struct(vKey, as_key, key);

    if (aerospike_key_put(ptr, &err, &policy, key, &record) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    return Qtrue;
}

VALUE client_get(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;
    VALUE vSettings;
    VALUE vParams[4];

    aerospike *ptr;
    as_key* key;
    as_error err;
    as_record* record = NULL;
    as_policy_read policy;
    as_bin bin;

    int n = 0;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    vKey = vArgs[0];
    check_aerospike_key(vKey);

    as_policy_read_init(&policy);

    if (argc == 2) {
        vSettings = vArgs[1];

        switch (TYPE(vSettings)) {
        case T_NIL:
            break;
        case T_HASH: {
            VALUE vTimeout = rb_hash_aref(vSettings, rb_str_new2("timeout"));
            if (TYPE(vTimeout) == T_FIXNUM) {
                policy.timeout = NUM2UINT( vTimeout );
            }
            break;
        }
        default:
            /* raise exception */
            Check_Type(vSettings, T_HASH);
            break;
        }
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    Data_Get_Struct(vKey, as_key, key);

    if (aerospike_key_get(ptr, &err, &policy, key, &record) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    vParams[0] = vKey;
    vParams[1] = rb_hash_new();
    vParams[2] = UINT2NUM(record->gen);
    vParams[3] = UINT2NUM(record->ttl);

    for(n = 0; n < record->bins.size; n++) {
        bin = record->bins.entries[n];
        switch( as_val_type(bin.valuep) ) {
        case AS_INTEGER:
            rb_hash_aset(vParams[1], rb_str_new2(bin.name), LONG2NUM(bin.valuep->integer.value));
            break;
        case AS_STRING:
            rb_hash_aset(vParams[1], rb_str_new2(bin.name), rb_str_new2(bin.valuep->string.value));
            break;
        case AS_UNDEF:
        default:
            break;
        }
    }

    return rb_class_new_instance(4, vParams, RecordClass);
}

VALUE client_operate(int argc, VALUE* argv, VALUE vSelf)
{
    VALUE vKey;
    VALUE vOperations;
    VALUE vSettings = Qnil;
    long idx = 0, n = 0;

    aerospike *ptr;
    as_operations ops;
    as_key* key;
    as_error err;
    as_policy_operate policy;

    if (argc > 3 || argc < 2) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..3)", argc);
    }

    vKey = argv[0];
    check_aerospike_key(vKey);

    vOperations = argv[1];
    Check_Type(vOperations, T_ARRAY);
    as_policy_operate_init(&policy);

    if (argc == 3) {
        vSettings = argv[2];

        switch (TYPE(vSettings)) {
        case T_NIL:
            break;
        case T_HASH: {
            VALUE vTimeout = rb_hash_aref(vSettings, rb_str_new2("timeout"));
            if (TYPE(vTimeout) == T_FIXNUM) {
                policy.timeout = NUM2UINT( vTimeout );
            }
            break;
        }
        default:
            /* raise exception */
            Check_Type(vSettings, T_HASH);
            break;
        }
    }

    idx = RARRAY_LEN(vOperations);
    if (idx == 0) {
        return Qfalse;
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    as_operations_inita(&ops, idx);

   for(n = 0; n < idx; n++) {
        VALUE operation = rb_ary_entry(vOperations, n);
        int op_type = NUM2INT( rb_iv_get(operation, "@op_type") );
        VALUE bin_name = rb_iv_get(operation, "@bin_name");
        VALUE bin_value = rb_iv_get(operation, "@bin_value");

        switch( op_type ) {
        case OPERATION_WRITE:
            switch( TYPE(bin_value) ) {
            case T_STRING:
                as_operations_add_write_str(&ops, StringValueCStr( bin_name ), StringValueCStr( bin_value ));
                break;
            case T_FIXNUM:
                as_operations_add_write_int64(&ops, StringValueCStr( bin_name ), NUM2LONG( bin_value ));
                break;
            }

            break;
        case OPERATION_READ:
            break;
        case OPERATION_INCREMENT:
            as_operations_add_incr(&ops, StringValueCStr( bin_name ), NUM2INT( bin_value ));
            break;
        case OPERATION_APPEND:
            Check_Type(bin_value, T_STRING);
            as_operations_add_append_str(&ops, StringValueCStr( bin_name ), StringValueCStr( bin_value ));
            break;
        case OPERATION_PREPEND:
            Check_Type(bin_value, T_STRING);
            as_operations_add_prepend_str(&ops, StringValueCStr( bin_name ), StringValueCStr( bin_value ));
            break;
        case OPERATION_TOUCH:
            as_operations_add_touch(&ops);
            break;
        default:
            rb_raise(rb_eArgError, "Incorrect operation type");
            break;
        }
    }

    Data_Get_Struct(vKey, as_key, key);

    if (aerospike_key_operate(ptr, &err, &policy, key, &ops, NULL) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    as_operations_destroy(&ops);

    return Qtrue;
}

void define_client()
{
    ClientClass = rb_define_class_under(AerospikeNativeClass, "Client", rb_cObject);
    rb_define_alloc_func(ClientClass, client_allocate);
    rb_define_method(ClientClass, "initialize", client_initialize, -1);
    rb_define_method(ClientClass, "operate", client_operate, -1);
    rb_define_method(ClientClass, "put", client_put, -1);
    rb_define_method(ClientClass, "get", client_get, -1);
}
