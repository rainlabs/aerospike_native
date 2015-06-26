require 'mkmf'

extension_name = 'aerospike_native'
#$CXXFLAGS = ' -Wextra -pedantic -c -Wall -std=c++0x'
$LDFLAGS  = ' -laerospike'
# have_library('aerospike')

#dir_config(extension_name)       # The destination
#create_makefile(extension_name)  # Create Makefile
create_makefile "aerospike_native/aerospike_native"
