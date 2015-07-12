#include "policy.h"
#include <aerospike/as_policy.h>

VALUE PolicyClass;

void define_policy()
{
    PolicyClass = rb_define_class_under(AerospikeNativeClass, "Policy", rb_cObject);

    rb_define_const(PolicyClass, "RETRY_NONE", INT2FIX(AS_POLICY_RETRY_NONE));
    rb_define_const(PolicyClass, "RETRY_ONCE", INT2FIX(AS_POLICY_RETRY_ONCE));

    rb_define_const(PolicyClass, "KEY_DIGEST", INT2FIX(AS_POLICY_KEY_DIGEST));
    rb_define_const(PolicyClass, "KEY_SEND", INT2FIX(AS_POLICY_KEY_SEND));

    rb_define_const(PolicyClass, "GEN_IGNORE", INT2FIX(AS_POLICY_GEN_IGNORE));
    rb_define_const(PolicyClass, "GEN_EQ", INT2FIX(AS_POLICY_GEN_EQ));
    rb_define_const(PolicyClass, "GEN_GT", INT2FIX(AS_POLICY_GEN_GT));

    rb_define_const(PolicyClass, "EXISTS_CREATE", INT2FIX(AS_POLICY_EXISTS_CREATE));
    rb_define_const(PolicyClass, "EXISTS_CREATE_OR_REPLACE", INT2FIX(AS_POLICY_EXISTS_CREATE_OR_REPLACE));
    rb_define_const(PolicyClass, "EXISTS_IGNORE", INT2FIX(AS_POLICY_EXISTS_IGNORE));
    rb_define_const(PolicyClass, "EXISTS_REPLACE", INT2FIX(AS_POLICY_EXISTS_REPLACE));
    rb_define_const(PolicyClass, "EXISTS_UPDATE", INT2FIX(AS_POLICY_EXISTS_UPDATE));

    rb_define_const(PolicyClass, "COMMIT_LEVEL_ALL", INT2FIX(AS_POLICY_COMMIT_LEVEL_ALL));
    rb_define_const(PolicyClass, "COMMIT_LEVEL_MASTER", INT2FIX(AS_POLICY_COMMIT_LEVEL_MASTER));

    rb_define_const(PolicyClass, "REPLICA_ANY", INT2FIX(AS_POLICY_REPLICA_ANY));
    rb_define_const(PolicyClass, "REPLICA_MASTER", INT2FIX(AS_POLICY_REPLICA_MASTER));

    rb_define_const(PolicyClass, "CONSISTENCY_LEVEL_ALL", INT2FIX(AS_POLICY_CONSISTENCY_LEVEL_ALL));
    rb_define_const(PolicyClass, "CONSISTENCY_LEVEL_ONE", INT2FIX(AS_POLICY_CONSISTENCY_LEVEL_ONE));
}
