#include "client.h"

VALUE Client;

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

VALUE initialize(int argc, VALUE* argv, VALUE self)
{
    VALUE ary = Qnil;
    aerospike *ptr;
    as_config config;
    as_error err;
    long idx = 0, n = 0;

    if (argc > 1) {  // there should only be 1 or 2 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1)", argc);
    }

    if (argc == 1) {
        rb_scan_args(argc, argv, "1", &ary);
    }

    switch (TYPE(ary)) {
    case T_NIL:
        printf("nil\n");
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

// GET
// PUT
// OPERATE

// Record
// Key
// Bin
// Policy

void define_client()
{
    Client = rb_define_class_under(AerospikeNative, "Client", rb_cObject);
    rb_define_alloc_func(Client, client_allocate);
    rb_define_method(Client, "initialize", initialize, -1);
}
