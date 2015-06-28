require 'mkmf'

extension_name = 'aerospike_native'

find_executable('make')
find_executable('git')
have_library('crypto')
#have_library('libc')
#have_library('openssl')

headers_path = File.expand_path(File.join(File.dirname(__FILE__), "include"))
lib_path = File.expand_path(File.join(File.dirname(__FILE__), "lib"))
aerospike_client_c_dir = File.expand_path(File.join(File.dirname(__FILE__), "aerospike-client-c"))
`git clone https://github.com/aerospike/aerospike-client-c.git`
Dir.chdir(aerospike_client_c_dir) do
  `git reset --hard f4aa41fc237fca3e25110d15e72b7735262e6653`
  `git submodule update --init`
  `make`
  target_dir = Dir.glob("target/*").first
  `cp -p #{target_dir}/lib/libaerospike.so #{lib_path}`
  `cp -rp #{target_dir}/include/* #{headers_path}`
end

LIBDIR     = RbConfig::CONFIG['libdir']
INCLUDEDIR = RbConfig::CONFIG['includedir']

HEADER_DIRS = [INCLUDEDIR, headers_path]
LIB_DIRS = [LIBDIR, lib_path]

libs = ['-laerospike', '-lcrypto']
dir_config(extension_name, HEADER_DIRS, LIB_DIRS)

libs.each do |lib|
  $LOCAL_LIBS << "#{lib} "
end

create_makefile(extension_name)
