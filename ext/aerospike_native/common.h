#ifndef COMMON_H
#define COMMON_H

#include <ruby.h>
#include "exception.h"
#include "logger.h"

VALUE rb_hash_keys(VALUE hash);

#define GET_STRING(vString)                                                \
    switch(TYPE(vString)) {                                                \
    case T_SYMBOL:                                                         \
        vString = rb_sym_to_s(vString);                                    \
    case T_STRING:                                                         \
        break;                                                             \
    default:                                                               \
        rb_raise(rb_eTypeError, "wrong argument type (expected String or Symbol)"); \
    }

#define SET_POLICY(policy, vSettings)                                      \
    VALUE vTimeout;                                                        \
    Check_Type(vSettings, T_HASH);                                         \
    vTimeout = rb_hash_aref(vSettings, rb_str_new2("timeout"));            \
    if (TYPE(vTimeout) == T_NIL) {                                         \
        vTimeout = rb_hash_aref(vSettings, ID2SYM(rb_intern("timeout")));  \
    }                                                                      \
    if (TYPE(vTimeout) == T_FIXNUM) {                                      \
        policy.timeout = NUM2UINT( vTimeout );                             \
    }

#define SET_RETRY_POLICY(policy, vSettings)                                \
    vRetry = rb_hash_aref(vSettings, rb_str_new2("retry"));                \
    if (TYPE(vRetry) == T_FIXNUM) {                                        \
        int policy_retry = FIX2INT(vRetry);                                \
        switch(policy_retry) {                                             \
        case AS_POLICY_RETRY_NONE:                                         \
        case AS_POLICY_RETRY_ONCE:                                         \
            break;                                                         \
        default:                                                           \
            rb_raise(rb_eArgError, "Incorrect \"retry\" policy value");    \
        }                                                                  \
        policy.retry = policy_retry;                                       \
    }

#define SET_KEY_POLICY(policy, vSettings)                                  \
    vKey = rb_hash_aref(vSettings, rb_str_new2("key"));                    \
    if (TYPE(vKey) == T_FIXNUM) {                                          \
        int policy_key = FIX2INT(vKey);                                    \
        switch(policy_key) {                                               \
        case AS_POLICY_KEY_DIGEST:                                         \
        case AS_POLICY_KEY_SEND:                                           \
            break;                                                         \
        default:                                                           \
            rb_raise(rb_eArgError, "Incorrect \"key\" policy value");      \
        }                                                                  \
        policy.key = policy_key;                                           \
    }

#define SET_GEN_POLICY(policy, vSettings)                                  \
    vGen = rb_hash_aref(vSettings, rb_str_new2("gen"));                    \
    if (TYPE(vGen) == T_FIXNUM) {                                          \
        int policy_gen = FIX2INT(vGen);                                    \
        switch(policy_gen) {                                               \
        case AS_POLICY_GEN_IGNORE:                                         \
        case AS_POLICY_GEN_EQ:                                             \
        case AS_POLICY_GEN_GT:                                             \
            break;                                                         \
        default:                                                           \
            rb_raise(rb_eArgError, "Incorrect \"gen\" policy value");      \
        }                                                                  \
        policy.gen = policy_gen;                                           \
    }

#define SET_EXISTS_POLICY(policy, vSettings)                               \
    vExists = rb_hash_aref(vSettings, rb_str_new2("exists"));              \
    if (TYPE(vExists) == T_FIXNUM) {                                       \
        int policy_exists = FIX2INT(vExists);                              \
        switch(policy_exists) {                                            \
        case AS_POLICY_EXISTS_CREATE:                                      \
        case AS_POLICY_EXISTS_CREATE_OR_REPLACE:                           \
        case AS_POLICY_EXISTS_IGNORE:                                      \
        case AS_POLICY_EXISTS_REPLACE:                                     \
        case AS_POLICY_EXISTS_UPDATE:                                      \
            break;                                                         \
        default:                                                           \
            rb_raise(rb_eArgError, "Incorrect \"exists\" policy value");   \
        }                                                                  \
        policy.exists = policy_exists;                                     \
    }

#define SET_COMMIT_LEVEL_POLICY(policy, vSettings)                         \
    vCommitLevel = rb_hash_aref(vSettings, rb_str_new2("commit_level"));   \
    if (TYPE(vCommitLevel) == T_FIXNUM) {                                  \
        int policy_commit = FIX2INT(vCommitLevel);                         \
        switch(policy_commit) {                                            \
        case AS_POLICY_COMMIT_LEVEL_ALL:                                   \
        case AS_POLICY_COMMIT_LEVEL_MASTER:                                \
            break;                                                         \
        default:                                                           \
         rb_raise(rb_eArgError, "Incorrect \"commit_level\" policy value");\
        }                                                                  \
        policy.commit_level = policy_commit;                               \
    }

#define SET_REPLICA_POLICY(policy, vSettings)                              \
    vReplica = rb_hash_aref(vSettings, rb_str_new2("replica"));            \
    if (TYPE(vReplica) == T_FIXNUM) {                                      \
        int policy_replica = FIX2INT(vReplica);                            \
        switch(policy_replica) {                                           \
        case AS_POLICY_REPLICA_ANY:                                        \
        case AS_POLICY_REPLICA_MASTER:                                     \
            break;                                                         \
        default:                                                           \
            rb_raise(rb_eArgError, "Incorrect \"replica\" policy value");  \
        }                                                                  \
        policy.replica = policy_replica;                                   \
    }

#define SET_CONSISTENCY_LEVEL_POLICY(policy, vSettings)                    \
    vConsistencyLevel = rb_hash_aref(vSettings, rb_str_new2("consistency_level")); \
    if (TYPE(vConsistencyLevel) == T_FIXNUM) {                             \
        int policy_consistency_level = FIX2INT(vConsistencyLevel);         \
        switch(policy_consistency_level) {                                 \
        case AS_POLICY_CONSISTENCY_LEVEL_ALL:                              \
        case AS_POLICY_CONSISTENCY_LEVEL_ONE:                              \
            break;                                                         \
        default:                                                           \
            rb_raise(rb_eArgError, "Incorrect \"consistency_level\" policy value"); \
        }                                                                  \
        policy.consistency_level = policy_consistency_level;               \
    }

#define SET_WRITE_POLICY(policy, vSettings)                                \
    VALUE vRetry, vKey, vGen, vExists, vCommitLevel;                       \
    SET_POLICY(policy, vSettings);                                         \
    SET_RETRY_POLICY(policy, vSettings);                                   \
    SET_KEY_POLICY(policy, vSettings);                                     \
    SET_GEN_POLICY(policy, vSettings);                                     \
    SET_EXISTS_POLICY(policy, vSettings);                                  \
    SET_COMMIT_LEVEL_POLICY(policy, vSettings);

#define SET_READ_POLICY(policy, vSettings)                                 \
    VALUE vKey, vReplica, vConsistencyLevel;                               \
    SET_POLICY(policy, vSettings);                                         \
    SET_KEY_POLICY(policy, vSettings);                                     \
    SET_REPLICA_POLICY(policy, vSettings);                                 \
    SET_CONSISTENCY_LEVEL_POLICY(policy, vSettings);

#define SET_OPERATE_POLICY(policy, vSettings)                              \
    VALUE vRetry, vKey, vGen, vReplica, vConsistencyLevel, vCommitLevel;   \
    SET_POLICY(policy, vSettings);                                         \
    SET_RETRY_POLICY(policy, vSettings);                                   \
    SET_KEY_POLICY(policy, vSettings);                                     \
    SET_GEN_POLICY(policy, vSettings);                                     \
    SET_REPLICA_POLICY(policy, vSettings);                                 \
    SET_CONSISTENCY_LEVEL_POLICY(policy, vSettings);                       \
    SET_COMMIT_LEVEL_POLICY(policy, vSettings);

#define SET_REMOVE_POLICY(policy, vSettings)                               \
    VALUE vRetry, vKey, vGen, vCommitLevel, vGeneration;                   \
    SET_POLICY(policy, vSettings);                                         \
    SET_RETRY_POLICY(policy, vSettings);                                   \
    SET_KEY_POLICY(policy, vSettings);                                     \
    SET_GEN_POLICY(policy, vSettings);                                     \
    SET_COMMIT_LEVEL_POLICY(policy, vSettings);                            \
    vGeneration = rb_hash_aref(vSettings, rb_str_new2("generation"));      \
    if (TYPE(vGeneration) == T_FIXNUM) {                                   \
        policy.generation = FIX2UINT(vGeneration);                         \
    }

#define SET_INFO_POLICY(policy, vSettings)                                 \
    VALUE vSendAsIs, vCheckBounds;                                         \
    SET_POLICY(policy, vSettings);                                         \
    vSendAsIs = rb_hash_aref(vSettings, rb_str_new2("send_as_is"));        \
    if (TYPE(vSendAsIs) != T_NIL) {                                        \
        policy.send_as_is = RTEST(vSendAsIs);                              \
    }                                                                      \
    vCheckBounds = rb_hash_aref(vSettings, rb_str_new2("check_bounds"));   \
    if (TYPE(vCheckBounds) != T_NIL) {                                     \
        policy.check_bounds = RTEST(vCheckBounds);                         \
    }

#endif // COMMON_H

