require 'mkmf'

extension_name = 'aerospike_native'

LIBDIR     = RbConfig::CONFIG['libdir']
INCLUDEDIR = RbConfig::CONFIG['includedir']

HEADER_DIRS = [INCLUDEDIR, File.expand_path(File.join(File.dirname(__FILE__), "include"))]
LIB_DIRS = [LIBDIR, File.expand_path(File.join(File.dirname(__FILE__), "lib"))]

libs = ['-laerospike', '-lcrypto']
dir_config(extension_name, HEADER_DIRS, LIB_DIRS)

libs.each do |lib|
  $LOCAL_LIBS << "#{lib} "
end

create_makefile(extension_name)
