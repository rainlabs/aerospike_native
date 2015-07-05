#include "common.h"

int keys_i(VALUE key, VALUE value, VALUE ary)
{
    rb_ary_push(ary, key);
    return ST_CONTINUE;
}

VALUE rb_hash_keys(VALUE hash)
{
    VALUE ary;

    ary = rb_ary_new_capa(RHASH_SIZE(hash));
    rb_hash_foreach(hash, keys_i, ary);

    return ary;
}
