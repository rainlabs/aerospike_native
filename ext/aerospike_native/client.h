#ifndef CLIENT_H
#define CLIENT_H

#include "aerospike_native.h"

RUBY_EXTERN VALUE ClientClass;
void define_client();
void check_aerospike_client(VALUE vKey);

#endif // CLIENT_H
