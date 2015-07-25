#ifndef QUERY_H
#define QUERY_H

#include "aerospike_native.h"

RUBY_EXTERN VALUE QueryClass;
void define_query();
bool query_callback(const as_val *value, void *udata);

#endif // QUERY_H

