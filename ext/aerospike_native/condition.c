#include "condition.h"

VALUE ConditionClass;

void check_aerospike_condition(VALUE vKey)
{
    char sName[] = "AerospikeNative::Condition";

    if (strcmp(sName, rb_obj_classname(vKey)) != 0) {
        rb_raise(rb_eArgError, "Incorrect type (expected %s)", sName);
    }
}

VALUE condition_initialize(VALUE vSelf, VALUE vBinName, VALUE vIndexType, VALUE vDataType, VALUE vMin, VALUE vMax)
{
    int index_type = 0, data_type = 0;
    Check_Type(vBinName, T_STRING);
    Check_Type(vIndexType, T_FIXNUM);
    Check_Type(vDataType, T_FIXNUM);

    index_type = NUM2INT(vIndexType);
    data_type = NUM2INT(vDataType);

    switch(index_type) {
    case INDEX_NUMERIC:
        Check_Type(vMin, T_FIXNUM);
        switch(TYPE(vMax)) {
        case T_NIL:
        case T_FIXNUM:
            break;
        default:
            rb_raise(rb_eArgError, "Expected FIXNUM or NIL value");
        }
        break;
    case INDEX_STRING:
        Check_Type(vMin, T_STRING);
        switch(TYPE(vMax)) {
        case T_NIL:
        case T_STRING:
            break;
        default:
            rb_raise(rb_eArgError, "Expected STRING or NIL value");
        }
        break;
    default:
        rb_raise(rb_eArgError, "Incorrect index type");
    }

    switch(data_type) {
    case CONDITION_TYPE_DEFAULT:
    case CONDITION_TYPE_LIST:
    case CONDITION_TYPE_MAPKEYS:
    case CONDITION_TYPE_MAPVALUES:
        break;
    default:
        rb_raise(rb_eArgError, "Incorrect data type");
    }

    rb_iv_set(vSelf, "@bin_name", vBinName);
    rb_iv_set(vSelf, "@index_type", vIndexType);
    rb_iv_set(vSelf, "@data_type", vDataType);
    rb_iv_set(vSelf, "@min", vMin);
    rb_iv_set(vSelf, "@max", vMax);

    return vSelf;
}

VALUE condition_equal(VALUE vSelf, VALUE vBinName, VALUE vVal)
{
    VALUE vArgs[5];
    int index_type;

    Check_Type(vBinName, T_STRING);

    switch(TYPE(vVal)) {
    case T_FIXNUM:
        index_type = INDEX_NUMERIC;
        break;
    case T_STRING:
        index_type = INDEX_STRING;
        break;
    default:
        rb_raise(rb_eArgError, "Expected only FIXNUM or STRING types");
    }

    vArgs[0] = vBinName;
    vArgs[1] = INT2FIX(index_type);
    vArgs[2] = INT2FIX(CONDITION_TYPE_DEFAULT);
    vArgs[3] = vVal;
    vArgs[4] = Qnil;
    return rb_class_new_instance(5, vArgs, vSelf);
}

VALUE condition_range(VALUE vSelf, VALUE vBinName, VALUE vMin, VALUE vMax)
{
    VALUE vArgs[5];

    Check_Type(vBinName, T_STRING);
    Check_Type(vMin, T_FIXNUM);
    Check_Type(vMax, T_FIXNUM);

    vArgs[0] = vBinName;
    vArgs[1] = INT2FIX(INDEX_NUMERIC);
    vArgs[2] = INT2FIX(CONDITION_TYPE_DEFAULT);
    vArgs[3] = vMin;
    vArgs[4] = vMax;
    return rb_class_new_instance(5, vArgs, vSelf);
}

void define_condition()
{
    ConditionClass = rb_define_class_under(AerospikeNativeClass, "Condition", rb_cObject);
    rb_define_method(ConditionClass, "initialize", condition_initialize, 5);
    rb_define_singleton_method(ConditionClass, "equal", condition_equal, 2);
    rb_define_singleton_method(ConditionClass, "range", condition_range, 3);

    rb_define_attr(ConditionClass, "bin_name", 1, 0);
    rb_define_attr(ConditionClass, "index_type", 1, 0);
    rb_define_attr(ConditionClass, "data_type", 1, 0);
    rb_define_attr(ConditionClass, "min", 1, 0);
    rb_define_attr(ConditionClass, "max", 1, 0);
}



