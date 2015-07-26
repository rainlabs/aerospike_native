#include "udf.h"
#include "client.h"
#include <aerospike/aerospike_udf.h>

VALUE UdfClass;

VALUE udf_initialize(VALUE vSelf, VALUE vClient)
{
    check_aerospike_client(vClient);
    rb_iv_set(vSelf, "@client", vClient);

    return vSelf;
}

VALUE udf_put(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vClient;
    FILE* file;
    aerospike* ptr;
//    uint8_t* content; //, p_write;
    int read, size;
    as_error err;
    as_string base_string;
    as_bytes udf_content;
    as_policy_info policy;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 argument
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    Check_Type(vArgs[0], T_STRING);
    file = fopen(StringValueCStr(vArgs[0]), "r");

    if (!file) {
        rb_funcall(LoggerInstance, rb_intern("warn"), 1, rb_str_new2("register UDF: File Not Found"));
        return Qfalse;
    }

    as_policy_info_init(&policy);
    if(argc == 2 && TYPE(vArgs[1]) != T_NIL) {
        SET_INFO_POLICY(policy, vArgs[1]);
    }

    // Read the file's content into a local buffer.

    uint8_t* content = (uint8_t*)malloc(1024 * 1024);

    if (! content) {
        rb_funcall(LoggerInstance, rb_intern("warn"), 1, rb_str_new2("script content allocation failed"));
        return Qfalse;
    }

    uint8_t* p_write = content;
    read = (int)fread(p_write, 1, 512, file);
    size = 0;

    while (read) {
        size += read;
        p_write += read;
        read = (int)fread(p_write, 1, 512, file);
    }

    fclose(file);

    // Wrap the local buffer as an as_bytes object.
    as_bytes_init_wrap(&udf_content, content, size, true);

    vClient = rb_iv_get(vSelf, "@client");
    Data_Get_Struct(vClient, aerospike, ptr);

    // Register the UDF file in the database cluster.
    if (aerospike_udf_put(ptr, &err, &policy, as_basename(&base_string, StringValueCStr(vArgs[0])), AS_UDF_TYPE_LUA,
            &udf_content) != AEROSPIKE_OK) {
        as_bytes_destroy(&udf_content);
        raise_aerospike_exception(err.code, err.message);
    }

    // This frees the local buffer.
    as_bytes_destroy(&udf_content);

    return Qtrue;
}

VALUE udf_remove(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vClient;
    aerospike* ptr;
    as_error err;
    as_policy_info policy;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 argument
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    Check_Type(vArgs[0], T_STRING);

    as_policy_info_init(&policy);
    if(argc == 2 && TYPE(vArgs[1]) != T_NIL) {
        SET_INFO_POLICY(policy, vArgs[1]);
    }

    vClient = rb_iv_get(vSelf, "@client");
    Data_Get_Struct(vClient, aerospike, ptr);

    // Remove the UDF file in the database cluster.
    if (aerospike_udf_remove(ptr, &err, &policy, StringValueCStr(vArgs[0])) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    return Qtrue;
}

VALUE udf_list(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vClient, vHash;
    aerospike* ptr;
    as_error err;
    as_policy_info policy;
    as_udf_files files;

    int n;

    if (argc > 1) {  // there should only be 0 or 1 argument
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 0..1)", argc);
    }

    as_policy_info_init(&policy);
    if(argc == 1 && TYPE(vArgs[0]) != T_NIL) {
        SET_INFO_POLICY(policy, vArgs[0]);
    }

    vClient = rb_iv_get(vSelf, "@client");
    Data_Get_Struct(vClient, aerospike, ptr);

    vHash = rb_hash_new();
    as_udf_files_init(&files, 0);
    if (aerospike_udf_list(ptr, &err, &policy, &files) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    for(n = 0; n < files.size; n++) {
        as_udf_file file = files.entries[n];
        VALUE vParamHash = rb_hash_new();
        rb_hash_aset(vParamHash, rb_str_new2("type"), INT2FIX(file.type));
        rb_hash_aset(vParamHash, rb_str_new2("hash"), rb_str_new(file.hash, AS_UDF_FILE_HASH_SIZE));
        rb_hash_aset(vHash, rb_str_new2(file.name), vParamHash);
    }

    return vHash;
}

VALUE udf_get(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vClient, vHash, vParamHash;
    aerospike* ptr;
    as_error err;
    as_policy_info policy;
    as_udf_file file;

    if (argc > 2 || argc < 1) {  // there should only be 1 or 2 argument
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..2)", argc);
    }

    as_policy_info_init(&policy);
    if(argc == 2 && TYPE(vArgs[1]) != T_NIL) {
        SET_INFO_POLICY(policy, vArgs[1]);
    }

    vClient = rb_iv_get(vSelf, "@client");
    Data_Get_Struct(vClient, aerospike, ptr);

    vHash = rb_hash_new();
    as_udf_file_init(&file);
    if (aerospike_udf_get(ptr, &err, &policy, StringValueCStr(vArgs[0]), AS_UDF_TYPE_LUA, &file) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    vParamHash = rb_hash_new();
    rb_hash_aset(vParamHash, rb_str_new2("type"), INT2FIX(file.type));
    rb_hash_aset(vParamHash, rb_str_new2("hash"), rb_str_new(file.hash, AS_UDF_FILE_HASH_SIZE));
    rb_hash_aset(vHash, rb_str_new2(file.name), vParamHash);

    return vHash;
}

VALUE udf_wait(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vClient, vSettings = Qnil;
    aerospike* ptr;
    as_error err;
    as_policy_info policy;

    uint32_t timeout = 1000;

    if (argc > 3 || argc < 1) {  // there should only be 1, 2 or 3 argument
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..3)", argc);
    }

    if (argc == 3) {
        Check_Type(vArgs[1], T_FIXNUM);
        timeout = FIX2ULONG(vArgs[1]);
        vSettings = vArgs[2];
    } else if (argc == 2) {
        switch(TYPE(vArgs[1])) {
        case T_NIL:
            break;
        case T_FIXNUM:
            timeout = FIX2ULONG(vArgs[1]);
            break;
        case T_HASH:
            vSettings = vArgs[1];
            break;
        default:
            rb_raise(rb_eTypeError, "wrong argument type (expected Hash or Fixnum)");
        }
    }

    as_policy_info_init(&policy);
    if(TYPE(vSettings) != T_NIL) {
        SET_INFO_POLICY(policy, vSettings);
    }

    vClient = rb_iv_get(vSelf, "@client");
    Data_Get_Struct(vClient, aerospike, ptr);

    if (aerospike_udf_put_wait(ptr, &err, &policy, StringValueCStr(vArgs[0]), timeout) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    return Qtrue;
}

void define_udf()
{
    UdfClass = rb_define_class_under(AerospikeNativeClass, "Udf", rb_cObject);
    rb_define_method(UdfClass, "initialize", udf_initialize, 1);
    rb_define_method(UdfClass, "put", udf_put, -1);
    rb_define_method(UdfClass, "remove", udf_remove, -1);
    rb_define_method(UdfClass, "list", udf_list, -1);
    rb_define_method(UdfClass, "get", udf_get, -1);
    rb_define_method(UdfClass, "wait", udf_wait, -1);

    rb_define_attr(UdfClass, "client", 1, 0);
    rb_define_const(UdfClass, "LUA", INT2FIX(AS_UDF_TYPE_LUA));
}
