#include "client.h"
#include "operation.h"
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
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1)", argc);
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
        printf( "Aerospike error (%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line );
        rb_raise(rb_eRuntimeError, "Aerospike error (%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line);
    }
    return self;
}

VALUE client_operate(int argc, VALUE* argv, VALUE vSelf)
{
    VALUE vKey;
    VALUE vOperations;
    VALUE vSettings = Qnil;
    long idx = 0, n = 0;

    aerospike *ptr;
    as_operations ops;
    as_key key;
    as_error err;

    VALUE vNamespace, vSet, vKeyValue;

    if (argc > 3 || argc < 2) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 3)", argc);
    }

    vKey = argv[0];
//    char* name = rb_obj_classname(vKey);
    printf("class name: %s\n", rb_obj_classname(vKey));

    vOperations = argv[1];
    Check_Type(vOperations, T_ARRAY);

    if (argc == 3) {
        vSettings = argv[2];

        switch (TYPE(vSettings)) {
        case T_NIL:
        case T_HASH:
            break;
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

    printf("prepare operations\n");

    for(n = 0; n < idx; n++) {
        VALUE operation = rb_ary_entry(vOperations, n);
        int op_type = NUM2INT( rb_iv_get(operation, "@op_type") );
        VALUE bin_name = rb_iv_get(operation, "@bin_name");
        VALUE bin_value = rb_iv_get(operation, "@bin_value");

        switch( op_type ) {
        case WRITE:
            break;
        case INCREMENT:
            as_operations_add_incr(&ops, StringValueCStr( bin_name ), NUM2INT( bin_value ));
            break;
        }
    }

    printf("prepare key\n");

    vNamespace = rb_iv_get(vKey, "@namespace");
    vSet = rb_iv_get(vKey, "@set");
    vKeyValue = rb_iv_get(vKey, "@value");
    as_key_init_str(&key, StringValueCStr( vNamespace ), StringValueCStr( vSet ), StringValueCStr( vKeyValue ));

    printf("execute\n");
    if (aerospike_key_operate(ptr, &err, NULL, &key, &ops, NULL) != AEROSPIKE_OK) {
        printf("error\n");
        fprintf(stderr, "err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line);
    }
    printf("finish\n");

    as_operations_destroy(&ops);

    return Qtrue;
}

// GET
// PUT
// OPERATE

// Record
// Key
// Bin
// Policy

void define_client()
{
    ClientClass = rb_define_class_under(AerospikeNativeClass, "Client", rb_cObject);
    rb_define_alloc_func(ClientClass, client_allocate);
    rb_define_method(ClientClass, "initialize", client_initialize, -1);
    rb_define_method(ClientClass, "operate", client_operate, -1);
}
