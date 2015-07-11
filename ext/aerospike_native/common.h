#ifndef COMMON_H
#define COMMON_H

#include <ruby.h>
#include "exception.h"

VALUE rb_hash_keys(VALUE hash);

#define SET_POLICY(policy, vSettings)                                      \
    switch (TYPE(vSettings)) {                                             \
    case T_NIL:                                                            \
        break;                                                             \
    case T_HASH: {                                                         \
        VALUE vTimeout = rb_hash_aref(vSettings, rb_str_new2("timeout"));  \
        if (TYPE(vTimeout) == T_FIXNUM) {                                  \
            policy.timeout = NUM2UINT( vTimeout );                         \
        }                                                                  \
        break;                                                             \
    }                                                                      \
    default:                                                               \
        /* raise exception */                                              \
        Check_Type(vSettings, T_HASH);                                     \
        break;                                                             \
    }

#endif // COMMON_H

