#include "exception.h"

VALUE ExceptionClass;

VALUE exception_initialize(VALUE vSelf, VALUE vMessage)
{
    Check_Type(vMessage, T_STRING);
    rb_iv_set(vSelf, "@message", vMessage);

    return vSelf;
}

void define_exception()
{
    ExceptionClass = rb_define_class_under(AerospikeNativeClass, "Exception", rb_eStandardError);
    rb_define_method(ExceptionClass, "initialize", exception_initialize, 1);
    rb_define_attr(ExceptionClass, "code", 1, 0);
    rb_define_attr(ExceptionClass, "message", 1, 0);

    rb_define_const(ExceptionClass, "ERR_CLIENT_ABORT", INT2FIX(AEROSPIKE_ERR_CLIENT_ABORT));
    rb_define_const(ExceptionClass, "ERR_INVALID_HOST", INT2FIX(AEROSPIKE_ERR_INVALID_HOST));
    rb_define_const(ExceptionClass, "NO_MORE_RECORDS", INT2FIX(AEROSPIKE_NO_MORE_RECORDS));
    rb_define_const(ExceptionClass, "ERR_PARAM", INT2FIX(AEROSPIKE_ERR_PARAM));
    rb_define_const(ExceptionClass, "ERR_CLIENT", INT2FIX(AEROSPIKE_ERR_CLIENT));
    rb_define_const(ExceptionClass, "OK", INT2FIX(AEROSPIKE_OK));
    rb_define_const(ExceptionClass, "ERR_SERVER", INT2FIX(AEROSPIKE_ERR_SERVER));
    rb_define_const(ExceptionClass, "ERR_RECORD_NOT_FOUND", INT2FIX(AEROSPIKE_ERR_RECORD_NOT_FOUND)); // 2

    rb_define_const(ExceptionClass, "ERR_RECORD_EXISTS", INT2FIX(AEROSPIKE_ERR_RECORD_EXISTS)); // 5
    rb_define_const(ExceptionClass, "ERR_BIN_EXISTS", INT2FIX(AEROSPIKE_ERR_BIN_EXISTS)); // 6

    rb_define_const(ExceptionClass, "ERR_TIMEOUT", INT2FIX(AEROSPIKE_ERR_TIMEOUT)); // 9

    rb_define_const(ExceptionClass, "ERR_RECORD_TOO_BIG", INT2FIX(AEROSPIKE_ERR_RECORD_TOO_BIG)); // 13
    rb_define_const(ExceptionClass, "ERR_RECORD_BUSY", INT2FIX(AEROSPIKE_ERR_RECORD_BUSY));
}

void raise_aerospike_exception(int iCode, char* sMessage)
{
    VALUE vException;

    vException = rb_exc_new2(ExceptionClass, sMessage);
    rb_iv_set(vException, "@code", iCode);
    rb_exc_raise(vException);
}
