#include "operation.h"

VALUE OperationClass;

VALUE operation_initialize(VALUE vSelf, VALUE vOpType, VALUE vBinName, VALUE vBinValue)
{
    Check_Type(vOpType, T_FIXNUM);
    Check_Type(vBinName, T_STRING);

    rb_iv_set(vSelf, "@op_type", vOpType);
    rb_iv_set(vSelf, "@bin_name", vBinName);
    rb_iv_set(vSelf, "@bin_value", vBinValue);

    return vSelf;
}

VALUE operation_write(VALUE vSelf, VALUE vBinName, VALUE vBinValue)
{
    VALUE vArgs[3];
    vArgs[0] = INT2NUM(WRITE);
    vArgs[1] = vBinName;
    vArgs[2] = vBinValue;
    return rb_class_new_instance(3, vArgs, vSelf);
}

VALUE operation_increment(VALUE vSelf, VALUE vBinName, VALUE vBinValue)
{
    VALUE vArgs[3];
    vArgs[0] = INT2NUM(INCREMENT);
    vArgs[1] = vBinName;
    vArgs[2] = vBinValue;
    return rb_class_new_instance(3, vArgs, vSelf);
}

void define_operation()
{
    OperationClass = rb_define_class_under(AerospikeNativeClass, "Operation", rb_cObject);
    rb_define_method(OperationClass, "initialize", operation_initialize, 3);
    rb_define_singleton_method(OperationClass, "write", operation_write, 2);
    rb_define_singleton_method(OperationClass, "increment", operation_increment, 2);
    rb_define_attr(OperationClass, "op_type", 1, 0);
    rb_define_attr(OperationClass, "bin_name", 1, 0);
    rb_define_attr(OperationClass, "bin_value", 1, 0);
}


