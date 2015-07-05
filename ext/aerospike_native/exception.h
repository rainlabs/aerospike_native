#ifndef EXCEPTION_H
#define EXCEPTION_H

#include "aerospike_native.h"

RUBY_EXTERN VALUE ExceptionClass;
void define_exception();
void raise_aerospike_exception(int iCode, char* sMessage);

#endif // EXCEPTION_H

