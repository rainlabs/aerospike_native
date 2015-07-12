#include "operation.h"

VALUE OperationClass;

/*
 * call-seq:
 *   new(operation_type, bin_name, bin_value) -> AerospikeNative::Operation
 *
 * initialize new operation for client.operate command
 */
VALUE operation_initialize(VALUE vSelf, VALUE vOpType, VALUE vBinName, VALUE vBinValue)
{
    int op_type = 0;

    Check_Type(vOpType, T_FIXNUM);
    op_type = NUM2INT(vOpType);

    if (op_type != OPERATION_TOUCH) {
        Check_Type(vBinName, T_STRING);
    } else {
        Check_Type(vBinName, T_NIL);
    }

    rb_iv_set(vSelf, "@op_type", vOpType);
    rb_iv_set(vSelf, "@bin_name", vBinName);
    rb_iv_set(vSelf, "@bin_value", vBinValue);

    return vSelf;
}

/*
 * call-seq:
 *   write(bin_name, bin_value) -> AerospikeNative::Operation
 *
 * initialize new write operation
 */
VALUE operation_write(VALUE vSelf, VALUE vBinName, VALUE vBinValue)
{
    VALUE vArgs[3];
    vArgs[0] = INT2NUM(OPERATION_WRITE);
    vArgs[1] = vBinName;
    vArgs[2] = vBinValue;
    return rb_class_new_instance(3, vArgs, vSelf);
}

/*
 * call-seq:
 *   append(bin_name, bin_value) -> AerospikeNative::Operation
 *
 * initialize new append operation
 */
VALUE operation_append(VALUE vSelf, VALUE vBinName, VALUE vBinValue)
{
    VALUE vArgs[3];

    Check_Type(vBinValue, T_STRING);

    vArgs[0] = INT2NUM(OPERATION_APPEND);
    vArgs[1] = vBinName;
    vArgs[2] = vBinValue;
    return rb_class_new_instance(3, vArgs, vSelf);
}

/*
 * call-seq:
 *   prepend(bin_name, bin_value) -> AerospikeNative::Operation
 *
 * initialize new prepend operation
 */
VALUE operation_prepend(VALUE vSelf, VALUE vBinName, VALUE vBinValue)
{
    VALUE vArgs[3];

    Check_Type(vBinValue, T_STRING);

    vArgs[0] = INT2NUM(OPERATION_PREPEND);
    vArgs[1] = vBinName;
    vArgs[2] = vBinValue;
    return rb_class_new_instance(3, vArgs, vSelf);
}

/*
 * call-seq:
 *   increment(bin_name, bin_value) -> AerospikeNative::Operation
 *
 * initialize new increment operation
 */
VALUE operation_increment(VALUE vSelf, VALUE vBinName, VALUE vBinValue)
{
    VALUE vArgs[3];

    Check_Type(vBinValue, T_FIXNUM);

    vArgs[0] = INT2NUM(OPERATION_INCREMENT);
    vArgs[1] = vBinName;
    vArgs[2] = vBinValue;
    return rb_class_new_instance(3, vArgs, vSelf);
}

/*
 * call-seq:
 *   touch() -> AerospikeNative::Operation
 *
 * initialize new touch operation
 */
VALUE operation_touch(VALUE vSelf)
{
    VALUE vArgs[3];

    vArgs[0] = INT2NUM(OPERATION_TOUCH);
    vArgs[1] = Qnil;
    vArgs[2] = Qnil;
    return rb_class_new_instance(3, vArgs, vSelf);
}

/*
 * call-seq:
 *   read(bin_name) -> AerospikeNative::Operation
 *
 * initialize new read operation
 */
VALUE operation_read(VALUE vSelf, VALUE vBinName)
{
    VALUE vArgs[3];

    vArgs[0] = INT2NUM(OPERATION_READ);
    vArgs[1] = vBinName;
    vArgs[2] = Qnil;
    return rb_class_new_instance(3, vArgs, vSelf);
}

void define_operation()
{
    OperationClass = rb_define_class_under(AerospikeNativeClass, "Operation", rb_cObject);
    rb_define_method(OperationClass, "initialize", operation_initialize, 3);
    rb_define_singleton_method(OperationClass, "write", operation_write, 2);
    rb_define_singleton_method(OperationClass, "append", operation_append, 2);
    rb_define_singleton_method(OperationClass, "prepend", operation_prepend, 2);
    rb_define_singleton_method(OperationClass, "increment", operation_increment, 2);
    rb_define_singleton_method(OperationClass, "touch", operation_touch, 0);
    rb_define_singleton_method(OperationClass, "read", operation_read, 1);
    rb_define_attr(OperationClass, "op_type", 1, 0);
    rb_define_attr(OperationClass, "bin_name", 1, 0);
    rb_define_attr(OperationClass, "bin_value", 1, 0);
}


