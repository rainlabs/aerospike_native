#include "client.h"
#include "operation.h"
#include "key.h"
#include "record.h"
#include "query.h"
#include "batch.h"
#include "scan.h"
#include "udf.h"
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_query.h>

VALUE ClientClass;
VALUE LoggerInstance;

void check_aerospike_client(VALUE vClient)
{
    char sName[] = "AerospikeNative::Client";

    if (strcmp(sName, rb_obj_classname(vClient)) != 0) {
        rb_raise(rb_eArgError, "Incorrect type (expected %s)", sName);
    }
}

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
 * initialize new client, use host' => ..., 'port' => ... for each hosts element
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
//    printf("lua dir: %s\n", config.lua.system_path);
//    strcpy(config.lua.system_path, "/home/rain/soft/aerospike-server/share/udf/lua");
//    strcpy(config.lua.user_path, "/home/rain/soft/aerospike-server/var/udf/lua");

    if (TYPE(ary) == T_ARRAY) {
        idx = RARRAY_LEN(ary);
        for(n = 0; n < idx; n++) {
            VALUE vHost, vPort;
            VALUE hash = rb_ary_entry(ary, n);
            vHost = rb_hash_aref(hash, rb_str_new2("host"));
            if (TYPE(vHost) == T_NIL) {
                vHost = rb_hash_aref(hash, ID2SYM( rb_intern("host") ));
            }
            vPort = rb_hash_aref(hash, rb_str_new2("port"));
            if (TYPE(vPort) == T_NIL) {
                vPort = rb_hash_aref(hash, ID2SYM( rb_intern("port") ));
            }
            as_config_add_host(&config, StringValueCStr(vHost), NUM2UINT(vPort));
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
        case T_NIL:
            as_record_set_nil(&record, StringValueCStr(bin_name));
            break;
        case T_STRING:
            as_record_set_str(&record, StringValueCStr(bin_name), StringValueCStr(bin_value));
            break;
        case T_FIXNUM:
            as_record_set_int64(&record, StringValueCStr(bin_name), NUM2LONG(bin_value));
            break;
//        case T_ARRAY:
//        case T_HASH:
//            rb_raise(rb_eTypeError, "wrong argument type for bin value (hashes and arrays not supported yet)");
//            break;
        default: {
            VALUE vBytes = rb_funcall(bin_value, rb_intern("to_msgpack"), 0);
            int strSize = RSTRING_LEN(vBytes);
            as_record_set_raw(&record, StringValueCStr(bin_name), StringValuePtr(vBytes), strSize);
            break;
        }
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
            case T_NIL: {
                as_record rec;
                as_record_inita(&rec, 1);
                as_record_set_nil(&rec, StringValueCStr( bin_name ));
                ops.binops.entries[ops.binops.size].op = AS_OPERATOR_WRITE;
                ops.binops.entries[ops.binops.size].bin = rec.bins.entries[0];
                ops.binops.size++;
                break;
            }
            case T_STRING:
                as_operations_add_write_str(&ops, StringValueCStr( bin_name ), StringValueCStr( bin_value ));
                break;
            case T_FIXNUM:
                as_operations_add_write_int64(&ops, StringValueCStr( bin_name ), NUM2LONG( bin_value ));
                break;
//            case T_ARRAY:
//            case T_HASH:
//                rb_raise(rb_eTypeError, "wrong argument type for bin value (hashes and arrays not supported yet)");
//                break;
            default: {
                VALUE vBytes = rb_funcall(bin_value, rb_intern("to_msgpack"), 0);
                int strSize = RSTRING_LEN(vBytes);
                as_operations_add_write_raw(&ops, StringValueCStr(bin_name), StringValuePtr(vBytes), strSize);
                break;
            }
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
 * Create new index, use \{'type' => AerospikeNative::INDEX_NUMERIC or AerospikeNative::INDEX_STRING\} as policy_settings to define index type
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


/*
 * call-seq:
 *   query(namespace, set) -> AerospikeNative::Query
 *
 * Instantiate new query
 */
VALUE client_query(VALUE vSelf, VALUE vNamespace, VALUE vSet)
{
    VALUE vParams[3];

    vParams[0] = vSelf;
    vParams[1] = vNamespace;
    vParams[2] = vSet;

    return rb_class_new_instance(3, vParams, QueryClass);
}

VALUE client_set_logger(VALUE vSelf, VALUE vNewLogger)
{
    rb_iv_set(LoggerInstance, "@internal", vNewLogger);
    return LoggerInstance;
}

VALUE client_set_log_level(VALUE vSelf, VALUE vLevel)
{
    Check_Type(vLevel, T_SYMBOL);
    return rb_funcall(LoggerInstance, rb_intern("set_level"), 1, vLevel);
}

/*
 * call-seq:
 *   batch -> AerospikeNative::Batch
 *
 * Instantiate new batch
 */
VALUE client_batch(VALUE vSelf)
{
    VALUE vParams[1];

    vParams[0] = vSelf;
    return rb_class_new_instance(1, vParams, BatchClass);
}

/*
 * call-seq:
 *   scan(namespace, set) -> AerospikeNative::Scan
 *
 * Instantiate new scan
 */
VALUE client_scan(VALUE vSelf, VALUE vNamespace, VALUE vSet)
{
    VALUE vParams[3];

    vParams[0] = vSelf;
    vParams[1] = vNamespace;
    vParams[2] = vSet;

    return rb_class_new_instance(3, vParams, ScanClass);
}

/*
 * call-seq:
 *   scan_info(scan_id) -> Hash
 *   scan_info(scan_id, scan_policy) -> Hash
 *
 * return scan info
 */
VALUE client_scan_info(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vParams[3];
    if (argc > 1) {  // there should only be 1 or 2 argument
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    vParams[0] = vSelf;
    vParams[1] = vArgs[0];
    vParams[2] = Qnil;
    if (argc == 2) {
        vParams[2] = vArgs[1];
    }

    return rb_funcall2(ScanClass, rb_intern("info"), 3, vParams);
}

VALUE client_udf(VALUE vSelf)
{
    VALUE vParams[1];
    vParams[0] = vSelf;

    return rb_class_new_instance(1, vParams, UdfClass);
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
    rb_define_method(ClientClass, "query", client_query, 2);
    rb_define_method(ClientClass, "batch", client_batch, 0);
    rb_define_method(ClientClass, "scan", client_scan, 2);
    rb_define_method(ClientClass, "scan_info", client_scan_info, -1);
    rb_define_method(ClientClass, "udf", client_udf, 0);

    LoggerInstance = rb_class_new_instance(0, NULL, LoggerClass);
    rb_cv_set(ClientClass, "@@logger", LoggerInstance);
    rb_define_singleton_method(ClientClass, "set_logger", client_set_logger, 1);
    rb_define_singleton_method(ClientClass, "set_log_level", client_set_log_level, 1);
}
