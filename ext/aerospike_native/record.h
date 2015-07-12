#ifndef RECORD_H
#define RECORD_H

#include "aerospike_native.h"
#include <aerospike/as_record.h>

RUBY_EXTERN VALUE RecordClass;
void define_record();

VALUE rb_record_from_c(as_record* record, as_key* key);

#endif // RECORD_H

