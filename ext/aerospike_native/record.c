#include "record.h"
#include "key.h"

VALUE RecordClass;

VALUE record_initialize(VALUE vSelf, VALUE vKey, VALUE vBins)
{
    check_aerospike_key(vKey);
    Check_Type(vBins, T_HASH);

    rb_iv_set(vSelf, "@key", vKey);
    rb_iv_set(vSelf, "@bins", vBins);

    return vSelf;
}


void define_record()
{
    RecordClass = rb_define_class_under(AerospikeNativeClass, "Record", rb_cObject);
    rb_define_method(RecordClass, "initialize", record_initialize, 2);
    rb_define_attr(RecordClass, "key", 1, 0);
    rb_define_attr(RecordClass, "bins", 1, 0);
}



