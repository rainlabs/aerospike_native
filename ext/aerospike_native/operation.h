#ifndef OPERATION_H
#define OPERATION_H

#include "aerospike_native.h"

extern VALUE OperationClass;
void define_operation();

enum OperationType {
    READ,
    WRITE,
    INCREMENT,
    APPEND,
    PREPEND,
    TOUCH
};

#endif // OPERATION_H

