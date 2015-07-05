#include "exception.h"

VALUE ExceptionClass;

VALUE exception_initialize(VALUE vSelf, VALUE vCode, VALUE vMessage)
{
    Check_Type(vCode, T_FIXNUM);
    Check_Type(vMessage, T_STRING);

    rb_iv_set(vSelf, "@code", vCode);
    rb_iv_set(vSelf, "@message", vMessage);

    return vSelf;
}

void define_exception()
{
    ExceptionClass = rb_define_class_under(AerospikeNativeClass, "Excpetion", rb_eStandardError);
    rb_define_method(ExceptionClass, "initialize", exception_initialize, 2);
    rb_define_attr(ExceptionClass, "code", 1, 0);
    rb_define_attr(ExceptionClass, "message", 1, 0);
}
