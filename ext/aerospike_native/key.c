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

VALUE key_initialize(VALUE vSelf, VALUE vNamespace, VALUE vSet, VALUE vValue)
{
    as_key *ptr;
    as_digest* digest;

    Check_Type(vNamespace, T_STRING);
    Check_Type(vSet, T_STRING);
    Check_Type(vValue, T_STRING);

    Data_Get_Struct(vSelf, as_key, ptr);
    as_key_init_str(ptr, StringValueCStr( vNamespace ), StringValueCStr( vSet ), StringValueCStr( vValue ));
    digest = as_key_digest(ptr);

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
    rb_define_method(KeyClass, "initialize", key_initialize, 3);
    rb_define_attr(KeyClass, "namespace", 1, 0);
    rb_define_attr(KeyClass, "set", 1, 0);
    rb_define_attr(KeyClass, "value", 1, 0);
    rb_define_attr(KeyClass, "digest", 1, 0);
}

