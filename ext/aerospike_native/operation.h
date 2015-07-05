#ifndef OPERATION_H
#define OPERATION_H

#include "aerospike_native.h"

RUBY_EXTERN VALUE OperationClass;
void define_operation();

enum OperationType {
    OPERATION_READ,
    OPERATION_WRITE,
    OPERATION_INCREMENT,
    OPERATION_APPEND,
    OPERATION_PREPEND,
    OPERATION_TOUCH
};

#endif // OPERATION_H

