#include "client.h"
#include "operation.h"
#include "key.h"
#include "record.h"
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_index.h>

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
        SET_POLICY(policy, vArgs[2]);
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
        as_record_destroy(&record);
        raise_aerospike_exception(err.code, err.message);
    }

    as_record_destroy(&record);
    return Qtrue;
}

VALUE client_get(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;
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
        SET_POLICY(policy, vArgs[1]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    Data_Get_Struct(vKey, as_key, key);

    if (aerospike_key_get(ptr, &err, &policy, key, &record) != AEROSPIKE_OK) {
        as_record_destroy(record);
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

    as_record_destroy(record);
    return rb_class_new_instance(4, vParams, RecordClass);
}

VALUE client_operate(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;
    VALUE vOperations;
    VALUE vParams[4];
    long idx = 0, n = 0;
    bool isset_read = false;

    aerospike *ptr;
    as_operations ops;
    as_key* key;
    as_error err;
    as_policy_operate policy;
    as_record* record = NULL;
    as_bin bin;

    if (argc > 3 || argc < 2) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..3)", argc);
    }

    vKey = vArgs[0];
    check_aerospike_key(vKey);

    vOperations = vArgs[1];
    Check_Type(vOperations, T_ARRAY);
    as_policy_operate_init(&policy);

    if (argc == 3) {
        SET_POLICY(policy, vArgs[2]);
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
            isset_read = true;
            as_operations_add_read(&ops, StringValueCStr( bin_name ));
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
            isset_read = true;
            as_operations_add_touch(&ops);
            break;
        default:
            rb_raise(rb_eArgError, "Incorrect operation type");
            break;
        }
    }

    Data_Get_Struct(vKey, as_key, key);

    if (isset_read) {
        if (aerospike_key_operate(ptr, &err, &policy, key, &ops, &record) != AEROSPIKE_OK) {
            as_operations_destroy(&ops);
            as_record_destroy(record);
            raise_aerospike_exception(err.code, err.message);
        }

        as_operations_destroy(&ops);

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

        as_record_destroy(record);
        return rb_class_new_instance(4, vParams, RecordClass);
    } else {
        if (aerospike_key_operate(ptr, &err, &policy, key, &ops, NULL) != AEROSPIKE_OK) {
            as_operations_destroy(&ops);
            raise_aerospike_exception(err.code, err.message);
        }

        as_operations_destroy(&ops);
        return Qtrue;
    }
}

VALUE client_remove(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;

    aerospike *ptr;
    as_key* key;
    as_error err;
    as_policy_remove policy;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    vKey = vArgs[0];
    check_aerospike_key(vKey);

    if (argc == 2) {
        SET_POLICY(policy, vArgs[2]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    Data_Get_Struct(vKey, as_key, key);

    if (aerospike_key_remove(ptr, &err, &policy, key) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    return Qtrue;
}

VALUE client_exists(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;

    aerospike *ptr;
    as_key* key;
    as_error err;
    as_policy_read policy;
    as_record* record = NULL;
    as_status status;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    vKey = vArgs[0];
    check_aerospike_key(vKey);

    if (argc == 2) {
        SET_POLICY(policy, vArgs[1]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    Data_Get_Struct(vKey, as_key, key);

    status = aerospike_key_exists(ptr, &err, &policy, key, &record);
    if (status != AEROSPIKE_OK && status != AEROSPIKE_ERR_RECORD_NOT_FOUND) {
        as_record_destroy(record);
        raise_aerospike_exception(err.code, err.message);
    }
    as_record_destroy(record);

    if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
        return Qfalse;
    }

    return Qtrue;
}

VALUE client_select(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;
    VALUE vArray;
    VALUE vParams[4];

    aerospike *ptr;
    as_key* key;
    as_error err;
    as_policy_read policy;
    as_record* record = NULL;
    as_bin bin;
    long n = 0, idx = 0;

    if (argc > 3 || argc < 2) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..3)", argc);
    }

    vKey = vArgs[0];
    check_aerospike_key(vKey);
    vArray = vArgs[1];

    Check_Type(vArray, T_ARRAY);
    idx = RARRAY_LEN(vArray);
    if (idx == 0) {
        return Qfalse;
    }

    if (argc == 3) {
        SET_POLICY(policy, vArgs[2]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    Data_Get_Struct(vKey, as_key, key);
    const char* bins[idx];

    for(n = 0; n < idx; n++) {
        VALUE bin_name = rb_ary_entry(vArray, n);

        Check_Type(bin_name, T_STRING);
        char* name = StringValueCStr( bin_name );
        bins[n] = malloc(strlen(name) * sizeof(char) + 1);
        memset(bins[n], '\0', strlen(name) + 1);
        strcpy(bins[n], name);
    }

    if (aerospike_key_select(ptr, &err, &policy, key, bins, &record) != AEROSPIKE_OK) {
        for(n = 0; n < idx; n++) {
            free(bins[n]);
        }
        as_record_destroy(record);
        raise_aerospike_exception(err.code, err.message);
    }
    for(n = 0; n < idx; n++) {
        free(bins[n]);
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

    as_record_destroy(record);
    return rb_class_new_instance(4, vParams, RecordClass);
}

VALUE client_create_index(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vNamespace, vSet, vBinName, vIndexName;

    aerospike *ptr;
    as_error err;
    as_policy_info policy;
    bool is_integer = true;

    if (argc > 5 || argc < 4) {  // there should only be 4 or 5 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 4..5)", argc);
    }

    vNamespace = vArgs[0];
    Check_Type(vNamespace, T_STRING);

    vSet = vArgs[1];
    Check_Type(vSet, T_STRING);

    vBinName = vArgs[2];
    Check_Type(vBinName, T_STRING);

    vIndexName = vArgs[3];
    Check_Type(vIndexName, T_STRING);

    if (argc == 5) {
        VALUE vType = Qnil;
        SET_POLICY(policy, vArgs[2]);
        vType = rb_hash_aref(vArgs[2], rb_str_new2("type"));
        if (TYPE(vType) == T_FIXNUM) {
            if (FIX2INT(vType) == 1) {
                is_integer = false;
            }
        }
    }

    Data_Get_Struct(vSelf, aerospike, ptr);

    if (is_integer) {
        if (aerospike_index_integer_create(ptr, &err, &policy, StringValueCStr(vNamespace), StringValueCStr(vSet), StringValueCStr(vBinName), StringValueCStr(vIndexName)) != AEROSPIKE_OK) {
            raise_aerospike_exception(err.code, err.message);
        }
    } else {
        if (aerospike_index_string_create(ptr, &err, &policy, StringValueCStr(vNamespace), StringValueCStr(vSet), StringValueCStr(vBinName), StringValueCStr(vIndexName)) != AEROSPIKE_OK) {
            raise_aerospike_exception(err.code, err.message);
        }
    }
    // TODO aerospike_index_create_wait

    return Qtrue;
}

VALUE client_drop_index(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vNamespace, vIndexName;

    aerospike *ptr;
    as_error err;
    as_policy_info policy;

    if (argc > 3 || argc < 2) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..3)", argc);
    }

    vNamespace = vArgs[0];
    Check_Type(vNamespace, T_STRING);

    vIndexName = vArgs[1];
    Check_Type(vIndexName, T_STRING);

    if (argc == 3) {
        SET_POLICY(policy, vArgs[2]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);

    if (aerospike_index_remove(ptr, &err, &policy, StringValueCStr(vNamespace), StringValueCStr(vIndexName)) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

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
    rb_define_method(ClientClass, "remove", client_remove, -1);
    rb_define_method(ClientClass, "exists?", client_exists, -1);
    rb_define_method(ClientClass, "select", client_select, -1);
    rb_define_method(ClientClass, "create_index", client_create_index, -1);
    rb_define_method(ClientClass, "drop_index", client_drop_index, -1);

    rb_define_const(ClientClass, "INTEGER", INT2FIX(0));
    rb_define_const(ClientClass, "STRING", INT2FIX(1));
}
