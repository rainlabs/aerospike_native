#ifndef KEY_H
#define KEY_H

#include "aerospike_native.h"

RUBY_EXTERN VALUE KeyClass;
void define_native_key();
void check_aerospike_key(VALUE vKey);

#endif // KEY_H

