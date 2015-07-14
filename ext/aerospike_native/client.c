#include "client.h"
#include "operation.h"
#include "key.h"
#include "record.h"
#include "condition.h"
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_query.h>

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

/*
 * call-seq:
 *   new() -> AerospikeNative::Client
 *   new(hosts) -> AerospikeNative::Client
 *
 * initialize new client, use {'host' => ..., 'port' => ...} for each hosts element
 */
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

/*
 * call-seq:
 *   put(key, bins) -> true or false
 *   put(key, bins, policy_settings) -> true or false
 *
 * put bins to specified key, bins are hash
 */
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

    if (argc == 3 && TYPE(vArgs[2]) != T_NIL) {
        SET_WRITE_POLICY(policy, vArgs[2]);
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
            rb_raise(rb_eTypeError, "wrong argument type for bin value (expected Fixnum or String)");
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

/*
 * call-seq:
 *   get(key) -> AerospikeNative::Record
 *   get(key, policy_settings) -> AerospikeNative::Record
 *
 * get record
 */
VALUE client_get(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;

    aerospike *ptr;
    as_key* key;
    as_error err;
    as_record* record = NULL;
    as_policy_read policy;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    vKey = vArgs[0];
    check_aerospike_key(vKey);

    as_policy_read_init(&policy);

    if (argc == 2 && TYPE(vArgs[1]) != T_NIL) {
        SET_READ_POLICY(policy, vArgs[1]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    Data_Get_Struct(vKey, as_key, key);

    if (aerospike_key_get(ptr, &err, &policy, key, &record) != AEROSPIKE_OK) {
        as_record_destroy(record);
        raise_aerospike_exception(err.code, err.message);
    }

    return rb_record_from_c(record, key);
}

/*
 * call-seq:
 *   operate(key, operations) -> true, false or AerospikeNative::Record
 *   operate(key, operations, policy_settings) -> true, false or AerospikeNative::Record
 *
 * perform multiple operations in one transaction, operations are array of AerospikeNative::Operation
 */
VALUE client_operate(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;
    VALUE vOperations;
    long idx = 0, n = 0;
    bool isset_read = false;

    aerospike *ptr;
    as_operations ops;
    as_key* key;
    as_error err;
    as_policy_operate policy;
    as_record* record = NULL;

    if (argc > 3 || argc < 2) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..3)", argc);
    }

    vKey = vArgs[0];
    check_aerospike_key(vKey);

    vOperations = vArgs[1];
    Check_Type(vOperations, T_ARRAY);
    as_policy_operate_init(&policy);

    if (argc == 3 && TYPE(vArgs[2]) != T_NIL) {
        SET_OPERATE_POLICY(policy, vArgs[2]);
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

        return rb_record_from_c(record, key);
    } else {
        if (aerospike_key_operate(ptr, &err, &policy, key, &ops, NULL) != AEROSPIKE_OK) {
            as_operations_destroy(&ops);
            raise_aerospike_exception(err.code, err.message);
        }

        as_operations_destroy(&ops);
        return Qtrue;
    }
}

/*
 * call-seq:
 *   remove(key) -> true or false
 *   remove(key, policy_settings) -> true or false
 *
 * remove record
 */
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

    if (argc == 2 && TYPE(vArgs[2]) != T_NIL) {
        SET_REMOVE_POLICY(policy, vArgs[2]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);
    Data_Get_Struct(vKey, as_key, key);

    if (aerospike_key_remove(ptr, &err, &policy, key) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    return Qtrue;
}

/*
 * call-seq:
 *   exists?(key) -> true or false
 *   exists?(key, policy_settings) -> true or false
 *
 * check existing record by key
 */
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

    if (argc == 2 && TYPE(vArgs[1]) != T_NIL) {
        SET_READ_POLICY(policy, vArgs[1]);
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

/*
 * call-seq:
 *   select(key, bins) -> AerospikeNative::Record
 *   select(key, bins, policy_settings) -> AerospikeNative::Record
 *
 * select specified bins by key
 */
VALUE client_select(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vKey;
    VALUE vArray;

    aerospike *ptr;
    as_key* key;
    as_error err;
    as_policy_read policy;
    as_record* record = NULL;
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

    if (argc == 3 && TYPE(vArgs[2]) != T_NIL) {
        SET_READ_POLICY(policy, vArgs[2]);
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

    return rb_record_from_c(record, key);
}

/*
 * call-seq:
 *   create_index(namespace, set, bin_name, index_name) -> true or false
 *   create_index(namespace, set, bin_name, index_name, policy_settings) -> true or false
 *
 * Create new index, use {'type' => AerospikeNative::INDEX_NUMERIC or AerospikeNative::INDEX_STRING} as policy_settings to define index type
 */
VALUE client_create_index(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vNamespace, vSet, vBinName, vIndexName;

    aerospike *ptr;
    as_error err;
    as_policy_info policy;
    as_index_task task;
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

    if (argc == 5 && TYPE(vArgs[4]) != T_NIL) {
        VALUE vType = Qnil;
        SET_INFO_POLICY(policy, vArgs[4]);
        vType = rb_hash_aref(vArgs[4], rb_str_new2("type"));
        if (TYPE(vType) == T_FIXNUM) {
            switch(FIX2INT(vType)) {
            case INDEX_NUMERIC:
                is_integer = true;
                break;
            case INDEX_STRING:
                is_integer = false;
                break;
            default:
                rb_raise(rb_eArgError, "Incorrect index type");
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

    task.as = ptr;
    strcpy(task.ns, StringValueCStr(vNamespace));
    strcpy(task.name, StringValueCStr(vIndexName));
    task.done = false;
    if (aerospike_index_create_wait(&err, &task, 1000) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    return (task.done ? Qtrue : Qfalse);
}

/*
 * call-seq:
 *   drop_index(namespace, index_name) -> true or false
 *   drop_index(namespace, index_name, policy_settings) -> true or false
 *
 * Remove specified index from node
 */
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

    if (argc == 3 && TYPE(vArgs[2]) != T_NIL) {
        SET_INFO_POLICY(policy, vArgs[2]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);

    if (aerospike_index_remove(ptr, &err, &policy, StringValueCStr(vNamespace), StringValueCStr(vIndexName)) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    return Qtrue;
}

bool query_callback(const as_val *value, void *udata) {
    VALUE vRecord;
    as_record *record;

    if (value == NULL) {
        // query is complete
        return true;
    }

    record = as_record_fromval(value);

    if (record != NULL) {
        vRecord = rb_record_from_c(record, NULL);

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
 *   where(namespace, set) -> array
 *   where(namespace, set, conditions) -> array
 *   where(namespace, set, conditions, policy_settings) -> array
 *   where(namespace, set) { |record| ... } -> nil
 *   where(namespace, set, conditions) { |record| ... } -> nil
 *   where(namespace, set, conditions, policy_settings) { |record| ... } -> nil
 *
 * Perform a query with where clause
 */
VALUE client_exec_query(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vNamespace;
    VALUE vSet;
    VALUE vConditions;
    VALUE vArray;

    aerospike *ptr;
    as_error err;
    as_policy_query policy;
    as_query query;

    int idx = 0, n = 0;

    if (argc > 4 || argc < 2) {  // there should only be 2, 3 or 4 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..4)", argc);
    }

    vNamespace = vArgs[0];
    Check_Type(vNamespace, T_STRING);

    vSet = vArgs[1];
    Check_Type(vSet, T_STRING);

    vConditions = vArgs[2];
    switch(TYPE(vConditions)) {
    case T_NIL:
        break;
    case T_ARRAY:
        idx = RARRAY_LEN(vConditions);
        break;
    default:
        rb_raise(rb_eTypeError, "wrong argument type for condition (expected Array or Nil)");
    }

    as_policy_query_init(&policy);
    if (argc == 4 && TYPE(vArgs[3]) != T_NIL) {
        SET_POLICY(policy, vArgs[3]);
    }

    Data_Get_Struct(vSelf, aerospike, ptr);

    as_query_init(&query, StringValueCStr(vNamespace), StringValueCStr(vSet));
    as_query_where_inita(&query, idx);
    for(n = 0; n < idx; n++) {
        VALUE vMin, vMax, vBinName;
        VALUE vCondition = rb_ary_entry(vConditions, n);
        check_aerospike_condition(vCondition);
        vMin = rb_iv_get(vCondition, "@min");
        vMax = rb_iv_get(vCondition, "@max");
        vBinName = rb_iv_get(vCondition, "@bin_name");

        switch(TYPE(vMin)) {
        case T_FIXNUM:
            switch(TYPE(vMax)) {
            case T_NIL:
                as_query_where(&query, StringValueCStr(vBinName), as_integer_equals(FIX2LONG(vMin)));
                break;
            case T_FIXNUM:
                as_query_where(&query, StringValueCStr(vBinName), as_integer_range(FIX2LONG(vMin), FIX2LONG(vMax)));
                break;
            default:
                rb_raise(rb_eArgError, "Incorrect condition");
            }

            break;
        case T_STRING:
            Check_Type(vMax, T_NIL);
            as_query_where(&query, StringValueCStr(vBinName), as_string_equals(StringValueCStr(vMin)));
            break;
        default:
            rb_raise(rb_eArgError, "Incorrect condition");
        }
    }

    vArray = rb_ary_new();
    if (aerospike_query_foreach(ptr, &err, &policy, &query, query_callback, &vArray) != AEROSPIKE_OK) {
        as_query_destroy(&query);
        raise_aerospike_exception(err.code, err.message);
    }
    as_query_destroy(&query);

    if ( rb_block_given_p() ) {
        return Qnil;
    }

    return vArray;
}

VALUE client_set_logger(VALUE vSelf, VALUE vNewLogger)
{
    VALUE vLogger = rb_cv_get(vSelf, "@@logger");
    rb_iv_set(vLogger, "@internal", vNewLogger);

    return vLogger;
}

VALUE client_set_log_level(VALUE vSelf, VALUE vLevel)
{
    VALUE vLogger = rb_cv_get(vSelf, "@@logger");
    Check_Type(vLevel, T_SYMBOL);

    return rb_funcall(vLogger, rb_intern("set_level"), 1, vLevel);
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
    rb_define_method(ClientClass, "where", client_exec_query, -1);

    rb_cv_set(ClientClass, "@@logger", rb_class_new_instance(0, NULL, LoggerClass));
    rb_define_singleton_method(ClientClass, "set_logger", client_set_logger, 1);
    rb_define_singleton_method(ClientClass, "set_log_level", client_set_log_level, 1);
}
