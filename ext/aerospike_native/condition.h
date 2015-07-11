#ifndef CONDITION_H
#define CONDITION_H

#include "aerospike_native.h"

RUBY_EXTERN VALUE ConditionClass;
void define_condition();
void check_aerospike_condition(VALUE vKey);

enum ConditionType {
    CONDITION_TYPE_DEFAULT = AS_INDEX_TYPE_DEFAULT,
    CONDITION_TYPE_LIST = AS_INDEX_TYPE_LIST,
    CONDITION_TYPE_MAPKEYS = AS_INDEX_TYPE_MAPKEYS,
    CONDITION_TYPE_MAPVALUES = AS_INDEX_TYPE_MAPVALUES
};

#endif // CONDITION_H

