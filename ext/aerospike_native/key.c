#include "key.h"

VALUE KeyClass;

VALUE key_initialize(VALUE vSelf, VALUE vNamespace, VALUE vSet, VALUE vValue)
{
    Check_Type(vNamespace, T_STRING);
    Check_Type(vSet, T_STRING);
    Check_Type(vValue, T_STRING);

    rb_iv_set(vSelf, "@namespace", vNamespace);
    rb_iv_set(vSelf, "@set", vSet);
    rb_iv_set(vSelf, "@value", vValue);

    return vSelf;
}

void define_native_key()
{
    KeyClass = rb_define_class_under(AerospikeNativeClass, "Key", rb_cObject);
    rb_define_method(KeyClass, "initialize", key_initialize, 3);
    rb_define_attr(KeyClass, "namespace", 1, 0);
    rb_define_attr(KeyClass, "set", 1, 0);
    rb_define_attr(KeyClass, "value", 1, 0);
    rb_define_attr(KeyClass, "digest", 1, 0);
}

