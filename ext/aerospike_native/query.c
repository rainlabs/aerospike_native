#include "query.h"
#include "client.h"
#include "record.h"
#include <aerospike/aerospike_query.h>

VALUE QueryClass;

VALUE query_initialize(VALUE vSelf, VALUE vClient, VALUE vNamespace, VALUE vSet)
{
    Check_Type(vNamespace, T_STRING);
    Check_Type(vSet, T_STRING);
    check_aerospike_client(vClient);

    rb_iv_set(vSelf, "@client", vClient);
    rb_iv_set(vSelf, "@namespace", vNamespace);
    rb_iv_set(vSelf, "@set", vSet);

    return vSelf;
}

/*
 * call-seq:
 *   select(bins) -> AerospikeNative::Query
 *   select(bin1, bin2, bin3, ...) -> AerospikeNative::Query
 *
 * set specified bins
 */
VALUE query_select(int argc, VALUE* vArgs, VALUE vSelf)
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

int query_order_hash_foreach(VALUE vKey, VALUE vValue, VALUE vHash)
{
    VALUE vHashKey, vHashValue;
    VALUE vAsc, vDesc;

    vAsc = ID2SYM( rb_intern("asc") );
    vDesc = ID2SYM( rb_intern("desc") );

    Check_Type(vValue, T_SYMBOL);
    vHashKey = vKey;
    GET_STRING(vHashKey);
    if(vValue != vAsc && vValue != vDesc) {
        rb_raise(rb_eArgError, "expected :asc or :desc for value");
    }

    if(vValue == vAsc) {
        vHashValue = INT2NUM(AS_ORDER_ASCENDING);
    } else {
        vHashValue = INT2NUM(AS_ORDER_DESCENDING);
    }

    rb_hash_aset(vHash, vHashKey, vHashValue);

    return ST_CONTINUE;
}

VALUE query_order(VALUE vSelf, VALUE vHash)
{
    VALUE vBins;
    Check_Type(vHash, T_HASH);

    vBins = rb_iv_get(vSelf, "@order_bins");
    if(TYPE(vBins) == T_NIL) {
        vBins = rb_hash_new();
    }
    rb_hash_foreach(vHash, query_order_hash_foreach, vBins);
    rb_iv_set(vSelf, "@order_bins", vBins);
    return vSelf;
}

int query_where_hash_foreach(VALUE vKey, VALUE vValue, VALUE vHash)
{
    VALUE vHashKey, vHashValue;
    int idx;

    vHashKey = vKey;
    GET_STRING(vHashKey);

    switch(TYPE(vValue)) {
    case T_ARRAY:
        idx = RARRAY_LEN(vValue);
        if (idx != 2) {
            rb_raise(rb_eArgError, "wrong array length (expected 2 elements)");
        }
        vHashValue = vValue;
        break;
    case T_FIXNUM:
    case T_STRING:
        vHashValue = vValue;
        break;
    default:
        rb_raise(rb_eTypeError, "wrong argument type for (expected Array, String or Fixnum)");
    }

    rb_hash_aset(vHash, vHashKey, vHashValue);

    return ST_CONTINUE;
}

VALUE query_where(VALUE vSelf, VALUE vHash)
{
    VALUE vBins;
    Check_Type(vHash, T_HASH);

    vBins = rb_iv_get(vSelf, "@where_bins");
    if(TYPE(vBins) == T_NIL) {
        vBins = rb_hash_new();
    }
    rb_hash_foreach(vHash, query_where_hash_foreach, vBins);
    rb_iv_set(vSelf, "@where_bins", vBins);
    return vSelf;
}

VALUE query_apply(int argc, VALUE* vArgs, VALUE vSelf)
{
    if (argc < 2 || argc > 3) {  // there should only be 2 or 3 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 2..3)", argc);
    }

    Check_Type(vArgs[0], T_STRING);
    Check_Type(vArgs[1], T_STRING);
    rb_iv_set(vSelf, "@udf_module", vArgs[0]);
    rb_iv_set(vSelf, "@udf_function", vArgs[1]);

    if (argc == 3 && TYPE(vArgs[2]) != T_NIL) {
        Check_Type(vArgs[2], T_ARRAY);
    }

    return vSelf;
}

bool query_callback(const as_val *value, void *udata) {
    VALUE vRecord;

    if (value == NULL) {
        // query is complete
        return true;
    }

    switch(as_val_type(value)) {
    case AS_REC: {
        as_record* record = as_record_fromval(value);
        if (record != NULL) {
            vRecord = rb_record_from_c(record, NULL);
        }
        break;
    }
    case AS_INTEGER: {
        as_integer* integer = as_integer_fromval(value);
        if (integer != NULL) {
            vRecord = LONG2NUM( as_integer_get(integer) );
        }
        break;
    }
    case AS_STRING: {
        as_string* string = as_string_fromval(value);
        if (string != NULL) {
            vRecord = LONG2NUM( as_string_get(string) );
        }
        break;
    }
    }

    if ( rb_block_given_p() ) {
        rb_yield(vRecord);
    } else {
        VALUE *vArray = (VALUE*) udata;
        rb_ary_push(*vArray, vRecord);
    }

    return true;
}

VALUE query_exec(int argc, VALUE* vArgs, VALUE vSelf)
{
    VALUE vNamespace;
    VALUE vSet;
    VALUE vArray;
    VALUE vClient;
    VALUE vWhere, vSelect, vOrder;
    VALUE vUdfModule;
    VALUE vWhereKeys, vOrderKeys;

    aerospike *ptr;
    as_error err;
    as_policy_query policy;
    as_query query;

    int n = 0;
    int where_idx = 0, select_idx = 0, order_idx = 0;

    if (argc > 1) {  // there should only be 0 or 1 arguments
        rb_raise(rb_eArgError, "wrong number of arguments (%d for 0..1)", argc);
    }

    vNamespace = rb_iv_get(vSelf, "@namespace");
    vSet = rb_iv_get(vSelf, "@set");

    vWhere = rb_iv_get(vSelf, "@where_bins");
    switch(TYPE(vWhere)) {
    case T_NIL:
        break;
    case T_HASH:
        vWhereKeys = rb_hash_keys(vWhere);
        where_idx = RARRAY_LEN(vWhereKeys);
        break;
    default:
        rb_raise(rb_eTypeError, "wrong argument type for where (expected Hash or Nil)");
    }

    vSelect = rb_iv_get(vSelf, "@select_bins");
    switch(TYPE(vSelect)) {
    case T_NIL:
        break;
    case T_ARRAY:
        select_idx = RARRAY_LEN(vSelect);
        break;
    default:
        rb_raise(rb_eTypeError, "wrong argument type for select (expected Array or Nil)");
    }

    vOrder = rb_iv_get(vSelf, "@order_bins");
    switch(TYPE(vOrder)) {
    case T_NIL:
        break;
    case T_HASH:
        vOrderKeys = rb_hash_keys(vOrder);
        order_idx = RARRAY_LEN(vOrderKeys);
        break;
    default:
        rb_raise(rb_eTypeError, "wrong argument type for order (expected Hash or Nil)");
    }

    as_policy_query_init(&policy);
    if (argc == 1 && TYPE(vArgs[0]) != T_NIL) {
        SET_POLICY(policy, vArgs[0]);
    }

    vClient = rb_iv_get(vSelf, "@client");
    Data_Get_Struct(vClient, aerospike, ptr);

    as_query_init(&query, StringValueCStr(vNamespace), StringValueCStr(vSet));

    as_query_select_inita(&query, select_idx);
    for(n = 0; n < select_idx; n++) {
        VALUE vBinName;
        vBinName = rb_ary_entry(vSelect, n);

        as_query_select(&query, StringValueCStr(vBinName));
    }

    as_query_orderby_inita(&query, order_idx);
    for(n = 0; n < order_idx; n++) {
        VALUE vBinName;
        VALUE vCondition;
        vBinName = rb_ary_entry(vOrderKeys, n);
        vCondition = rb_hash_aref(vOrder, vBinName);

        as_query_orderby(&query, StringValueCStr(vBinName), NUM2INT(vCondition));
    }

    as_query_where_inita(&query, where_idx);
    for(n = 0; n < where_idx; n++) {
        VALUE vMin = Qnil, vMax = Qnil, vBinName;
        VALUE vCondition;
        vBinName = rb_ary_entry(vWhereKeys, n);
        vCondition = rb_hash_aref(vWhere, vBinName);
        switch(TYPE(vCondition)) {
        case T_ARRAY:
            vMin = rb_ary_entry(vCondition, 0);
            vMax = rb_ary_entry(vCondition, 1);
            break;
        default:
            vMin = vCondition;
        }

        switch(TYPE(vMin)) {
        case T_FIXNUM:
            switch(TYPE(vMax)) {
            case T_NIL:
                as_query_where(&query, StringValueCStr(vBinName), as_integer_equals(FIX2LONG(vMin)));
                break;
            case T_FIXNUM:
                as_query_where(&query, StringValueCStr(vBinName), as_integer_range(FIX2LONG(vMin), FIX2LONG(vMax)));
                break;
            default:
                rb_raise(rb_eArgError, "Incorrect condition");
            }

            break;
        case T_STRING:
            Check_Type(vMax, T_NIL);
            as_query_where(&query, StringValueCStr(vBinName), as_string_equals(StringValueCStr(vMin)));
            break;
        default:
            rb_raise(rb_eArgError, "Incorrect condition");
        }
    }

    vUdfModule = rb_iv_get(vSelf, "@udf_module");
    switch(TYPE(vUdfModule)) {
    case T_NIL:
        break;
    case T_STRING: {
        VALUE vUdfFunction = rb_iv_get(vSelf, "@udf_function");
        as_query_apply(&query, StringValueCStr(vUdfModule), StringValueCStr(vUdfFunction), NULL);
        break;
    }
    default:
        rb_raise(rb_eTypeError, "wrong argument type for udf module (expected String or Nil)");
    }

    vArray = rb_ary_new();
    if (aerospike_query_foreach(ptr, &err, &policy, &query, query_callback, &vArray) != AEROSPIKE_OK) {
        as_query_destroy(&query);
        raise_aerospike_exception(err.code, err.message);
    }
    as_query_destroy(&query);

    if ( rb_block_given_p() ) {
        return Qnil;
    }

    return vArray;
}

void define_query()
{
    QueryClass = rb_define_class_under(AerospikeNativeClass, "Query", rb_cObject);
    rb_define_method(QueryClass, "initialize", query_initialize, 3);
    rb_define_method(QueryClass, "select", query_select, -1);
    rb_define_method(QueryClass, "order", query_order, 1);
    rb_define_method(QueryClass, "where", query_where, 1);
    rb_define_method(QueryClass, "apply", query_apply, -1);
    rb_define_method(QueryClass, "exec", query_exec, -1);

    rb_define_attr(QueryClass, "client", 1, 0);
    rb_define_attr(QueryClass, "namespace", 1, 0);
    rb_define_attr(QueryClass, "set", 1, 0);
    rb_define_attr(QueryClass, "select_bins", 1, 0);
    rb_define_attr(QueryClass, "where_bins", 1, 0);
    rb_define_attr(QueryClass, "order_bins", 1, 0);

    rb_define_attr(QueryClass, "udf_module", 1, 0);
    rb_define_attr(QueryClass, "udf_function", 1, 0);
    rb_define_attr(QueryClass, "udf_arglist", 1, 0);
}
