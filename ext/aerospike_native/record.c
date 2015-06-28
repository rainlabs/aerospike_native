#include "record.h"
#include "key.h"

VALUE RecordClass;

VALUE record_initialize(VALUE vSelf, VALUE vKey, VALUE vBins, VALUE vGen, VALUE vTtl)
{
    check_aerospike_key(vKey);
    Check_Type(vBins, T_HASH);
    Check_Type(vGen, T_FIXNUM);
    Check_Type(vTtl, T_FIXNUM);

    rb_iv_set(vSelf, "@key", vKey);
    rb_iv_set(vSelf, "@bins", vBins);
    rb_iv_set(vSelf, "@gen", vGen);
    rb_iv_set(vSelf, "@ttl", vTtl);

    return vSelf;
}


void define_record()
{
    RecordClass = rb_define_class_under(AerospikeNativeClass, "Record", rb_cObject);
    rb_define_method(RecordClass, "initialize", record_initialize, 4);
    rb_define_attr(RecordClass, "key", 1, 0);
    rb_define_attr(RecordClass, "bins", 1, 0);
    rb_define_attr(RecordClass, "gen", 1, 0);
    rb_define_attr(RecordClass, "ttl", 1, 0);
}



