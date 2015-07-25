#include "scan.h"
#include "query.h"
#include "client.h"
#include <aerospike/aerospike_scan.h>

VALUE ScanClass;

static void scan_deallocate(void *p)
{
    as_scan* ptr = p;
    as_scan_destroy(ptr);
}

VALUE scan_initialize(VALUE vSelf, VALUE vClient, VALUE vNamespace, VALUE vSet)
{
    Check_Type(vNamespace, T_STRING);
    Check_Type(vSet, T_STRING);
    check_aerospike_client(vClient);

    rb_iv_set(vSelf, "@client", vClient);
    rb_iv_set(vSelf, "@namespace", vNamespace);
    rb_iv_set(vSelf, "@set", vSet);

    return vSelf;
}

VALUE scan_select(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vBins;
    int i;

    if (argc == 0) {  // there should be greater than 0
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 1..n)", argc);
    }

    vBins = rb_iv_get(vSelf, "@select_bins");
    if(TYPE(vBins) == T_NIL) {
        vBins = rb_ary_new();
    }
    if (argc == 1) {
        int idx;
        VALUE vArray = vArgs[0];
        Check_Type(vArray, T_ARRAY);
        idx = RARRAY_LEN(vArray);
        for(i = 0; i < idx; i++) {
            VALUE vString = rb_ary_entry(vArray, i);
            GET_STRING(vString);
            rb_ary_push(vBins, vString);
        }
    } else {
        for(i = 0; i < argc; i++) {
            VALUE vString = vArgs[i];
            GET_STRING(vString);
            rb_ary_push(vBins, vString);
        }
    }

    rb_iv_set(vSelf, "@select_bins", vBins);
    return vSelf;
}

VALUE scan_concurrent(VALUE vSelf, VALUE vValue)
{
    rb_iv_set(vSelf, "@concurrent", vValue);
    return vSelf;
}

VALUE scan_percent(VALUE vSelf, VALUE vValue)
{
    rb_iv_set(vSelf, "@percent", vValue);
    return vSelf;
}

VALUE scan_priority(VALUE vSelf, VALUE vValue)
{
    rb_iv_set(vSelf, "@priority", vValue);
    return vSelf;
}

VALUE scan_no_bins(VALUE vSelf, VALUE vValue)
{
    rb_iv_set(vSelf, "@no_bins", vValue);
    return vSelf;
}

VALUE scan_background(VALUE vSelf, VALUE vValue)
{
    rb_iv_set(vSelf, "@background", vValue);
    return vSelf;
}

VALUE scan_exec(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vClient, vNamespace, vSet;
    VALUE vArray;
    VALUE vConcurrent, vPercent, vPriority, vBins, vNoBins, vBackground;
    as_scan scan;
    as_policy_scan policy;
    as_error err;
    aerospike* ptr;

    int n, idx = 0;
    bool is_background = false;

    if (argc > 1) {  // there should only be 0 or 1 argument
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 0..1)", argc);
    }

    as_policy_scan_init(&policy);
    if(argc == 1) {
        SET_SCAN_POLICY(policy, vArgs[0]);
    }

    vClient = rb_iv_get(vSelf, "@client");
    vNamespace = rb_iv_get(vSelf, "@namespace");
    vSet = rb_iv_get(vSelf, "@set");
    vConcurrent = rb_iv_get(vSelf, "@concurrent");
    vPercent = rb_iv_get(vSelf, "@percent");
    vPriority = rb_iv_get(vSelf, "@priority");
    vNoBins = rb_iv_get(vSelf, "@no_bins");
    vBins = rb_iv_get(vSelf, "@select_bins");
    vBackground = rb_iv_get(vSelf, "@background");
    as_scan_init(&scan, StringValueCStr(vNamespace), StringValueCStr(vSet));

    if (TYPE(vPercent) == T_FIXNUM) {
        as_scan_set_percent(&scan, FIX2INT(vPercent));
    }

    if (TYPE(vPriority) == T_FIXNUM) {
        as_scan_set_priority(&scan, FIX2INT(vPriority));
    }

    if (TYPE(vConcurrent) != T_NIL) {
        as_scan_set_priority(&scan, RTEST(vConcurrent));
    }

    if (TYPE(vNoBins) != T_NIL) {
        as_scan_set_nobins(&scan, RTEST(vNoBins));
    }

    if (TYPE(vBackground) != T_NIL) {
        is_background = RTEST(vBackground);
    }

    if (TYPE(vBins) == T_ARRAY && (idx = RARRAY_LEN(vBins)) > 0) {
        as_scan_select_inita(&scan, idx);
        for(n = 0; n < idx; n++) {
            VALUE vEntry = rb_ary_entry(vBins, n);
            as_scan_select(&scan, StringValueCStr(vEntry));
        }
    }

    Data_Get_Struct(vClient, aerospike, ptr);

    vArray = rb_ary_new();
    if(is_background) {
        uint64_t scan_id = 0;
        if (aerospike_scan_background(ptr, &err, &policy, &scan, &scan_id) != AEROSPIKE_OK) {
            as_scan_destroy(&scan);
            raise_aerospike_exception(err.code, err.message);
        }
        as_scan_destroy(&scan);
        return ULONG2NUM(scan_id);
    }

    if (aerospike_scan_foreach(ptr, &err, &policy, &scan, query_callback, &vArray) != AEROSPIKE_OK) {
        as_scan_destroy(&scan);
        raise_aerospike_exception(err.code, err.message);
    }

    as_scan_destroy(&scan);
    if ( rb_block_given_p() ) {
        return Qnil;
    }

    return vArray;
}

VALUE scan_info(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vClient, vHash;
    as_scan_info info;
    as_policy_scan policy;
    as_error err;
    aerospike* ptr;
    uint64_t scan_id;

    if (argc > 3 || argc < 2) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..3)", argc);
    }

    vClient = vArgs[0];
    check_aerospike_client(vClient);

    Check_Type(vArgs[1], T_FIXNUM);
    scan_id = FIX2ULONG(vArgs[1]);

    as_policy_scan_init(&policy);
    if(argc == 3 && TYPE(vArgs[2]) != T_NIL) {
        SET_SCAN_POLICY(policy, vArgs[2]);
    }

    Data_Get_Struct(vClient, aerospike, ptr);

    if (aerospike_scan_info(ptr, &err, &policy, scan_id, &info) != AEROSPIKE_OK) {
        raise_aerospike_exception(err.code, err.message);
    }

    vHash = rb_hash_new();
    rb_hash_aset(vHash, rb_str_new2("progress_percent"), ULONG2NUM(info.progress_pct));
    rb_hash_aset(vHash, rb_str_new2("records_scanned"), ULONG2NUM(info.records_scanned));
    rb_hash_aset(vHash, rb_str_new2("status"), INT2NUM(info.status));

    return vHash;
}

void define_scan()
{
    ScanClass = rb_define_class_under(AerospikeNativeClass, "Scan", rb_cObject);
    rb_define_method(ScanClass, "initialize", scan_initialize, 3);
    rb_define_method(ScanClass, "exec", scan_exec, -1);
    rb_define_method(ScanClass, "select", scan_select, -1);
    rb_define_method(ScanClass, "set_concurrent", scan_concurrent, 1);
    rb_define_method(ScanClass, "set_percent", scan_percent, 1);
    rb_define_method(ScanClass, "set_priority", scan_priority, 1);
    rb_define_method(ScanClass, "set_no_bins", scan_no_bins, 1);
//    rb_define_method(ScanClass, "set_background", scan_background, 1);
    rb_define_singleton_method(ScanClass, "info", scan_info, -1);

    rb_define_attr(ScanClass, "client", 1, 0);
    rb_define_attr(ScanClass, "select_bins", 1, 0);
    rb_define_attr(ScanClass, "concurrent", 1, 0);
    rb_define_attr(ScanClass, "percent", 1, 0);
    rb_define_attr(ScanClass, "priority", 1, 0);
    rb_define_attr(ScanClass, "no_bins", 1, 0);
    rb_define_attr(ScanClass, "background", 1, 0);

    rb_define_const(ScanClass, "STATUS_UNDEFINED", INT2FIX(AS_SCAN_STATUS_UNDEF));
    rb_define_const(ScanClass, "STATUS_INPROGRESS", INT2FIX(AS_SCAN_STATUS_INPROGRESS));
    rb_define_const(ScanClass, "STATUS_ABORTED", INT2FIX(AS_SCAN_STATUS_ABORTED));
    rb_define_const(ScanClass, "STATUS_COMPLETED", INT2FIX(AS_SCAN_STATUS_COMPLETED));

    rb_define_const(ScanClass, "PRIORITY_AUTO", INT2FIX(AS_SCAN_PRIORITY_AUTO));
    rb_define_const(ScanClass, "PRIORITY_HIGH", INT2FIX(AS_SCAN_PRIORITY_HIGH));
    rb_define_const(ScanClass, "PRIORITY_MEDIUM", INT2FIX(AS_SCAN_PRIORITY_MEDIUM));
    rb_define_const(ScanClass, "PRIORITY_LOW", INT2FIX(AS_SCAN_PRIORITY_LOW));
}
