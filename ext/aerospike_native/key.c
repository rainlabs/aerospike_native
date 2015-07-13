#include "key.h"
#include <aerospike/as_key.h>

VALUE KeyClass;

static void key_deallocate(void *p)
{
    as_key* ptr = p;
    as_key_destroy(ptr);
}

static VALUE key_allocate(VALUE klass)
{
    VALUE obj;
    as_key *ptr;

    obj = Data_Make_Struct(klass, as_key, NULL, key_deallocate, ptr);

    return obj;
}

/*
 * call-seq:
 *   new(namespace, set, value) -> AerospikeNative::Key
 *   new(namespace, set, value, digest) -> AerospikeNative::Key
 *
 * initialize new key
 */
VALUE key_initialize(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vNamespace, vSet, vValue, vDigest = Qnil;
    as_key *ptr;
    as_digest* digest = NULL;

    if (argc > 4 || argc < 3) {  // there should only be 3 or 4 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 3..4)", argc);
    }

    vNamespace = vArgs[0];
    Check_Type(vNamespace, T_STRING);

    vSet = vArgs[1];
    Check_Type(vSet, T_STRING);

    vValue = vArgs[2];

    if (argc == 4) {
        vDigest = vArgs[3];
    }

    Data_Get_Struct(vSelf, as_key, ptr);

    if(TYPE(vValue) == T_STRING || TYPE(vValue) == T_FIXNUM) {
        switch(TYPE(vValue)) {
        case T_FIXNUM:
            as_key_init_int64(ptr, StringValueCStr( vNamespace ), StringValueCStr( vSet ), FIX2LONG( vValue ));
            break;
        case T_STRING:
            as_key_init_str(ptr, StringValueCStr( vNamespace ), StringValueCStr( vSet ), StringValueCStr( vValue ));
            break;
        default:
            rb_raise(rb_eArgError, "Expected FIXNUM or STRING as value");
        }
    } else {
        Check_Type(vValue, T_NIL);
        Check_Type(vDigest, T_STRING);
        as_key_init_digest(ptr, StringValueCStr( vNamespace ), StringValueCStr( vSet ), StringValuePtr( vDigest ));
    }

    if (digest == NULL) {
        digest = as_key_digest(ptr);
    }

    rb_iv_set(vSelf, "@namespace", vNamespace);
    rb_iv_set(vSelf, "@set", vSet);
    rb_iv_set(vSelf, "@value", vValue);
    rb_iv_set(vSelf, "@digest", rb_str_new( digest->value, AS_DIGEST_VALUE_SIZE));

    return vSelf;
}

void check_aerospike_key(VALUE vKey)
{
    char sKeyName[] = "AerospikeNative::Key";

    if (strcmp(sKeyName, rb_obj_classname(vKey)) != 0) {
        rb_raise(rb_eArgError, "Incorrect type (expected %s)", sKeyName);
    }
}

void define_native_key()
{
    KeyClass = rb_define_class_under(AerospikeNativeClass, "Key", rb_cObject);
    rb_define_alloc_func(KeyClass, key_allocate);
    rb_define_method(KeyClass, "initialize", key_initialize, -1);
    rb_define_attr(KeyClass, "namespace", 1, 0);
    rb_define_attr(KeyClass, "set", 1, 0);
    rb_define_attr(KeyClass, "value", 1, 0);
    rb_define_attr(KeyClass, "digest", 1, 0);
}

