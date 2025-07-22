# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

compiler = meson.get_compiler('cpp')
has_pread = compiler.compiles('''
    #include<fcntl.h>
    #include<unistd.h>
    int main(int,char*[]){
      int f = open("/x/y", O_RDONLY);
      char buf[100];
      return pread(f, buf, 100, 1000) == 0;
    }
''')

has_strptime = compiler.compiles('''
    #include<time.h>
    int main(int,char*[]){
      struct tm time2020;
      return !strptime("2020-02-02 12:34:56", "%Y-%m-%d %H:%M:%S", &time2020);
    }
''')

has_builtin_overflow_check = compiler.compiles('''
    int main(){
      int a;
      return __builtin_add_overflow(1, 2, &a);
    }
''')

has_diagnostic_push = compiler.compiles('''
    #ifdef __clang__
      #pragma clang diagnostic push
      #pragma clang diagnostic ignored "-Wdeprecated"
      #pragma clang diagnostic pop
   #elif defined(__GNUC__)
      #pragma GCC diagnostic push
      #pragma GCC diagnostic ignored "-Wdeprecated"
      #pragma GCC diagnostic pop
   #elif defined(_MSC_VER)
      #pragma warning( push )
      #pragma warning( disable : 4996 )
      #pragma warning( pop )
   #else
     unknownCompiler!
   #endif
   int main(int, char *[]) {}
''')

has_std_isnan = compiler.compiles('''
    #include<cmath>
    int main(int, char *[]) {
      return std::isnan(1.0f);
    }
''')

has_double_to_string = compiler.compiles('''
    #include<string>
    int main(int, char *[]) {
      double d = 5;
      std::to_string(d);
    }
''')

has_int64_to_string = compiler.compiles('''
    #include<cstdint>
    #include<string>
    int main(int, char *[]) {
      int64_t d = 5;
      std::to_string(d);
    }
''')

has_pre_1970 = compiler.run('''
    #include<time.h>
    int main(int, char *[]) {
      time_t t = -14210715; // 1969-07-20 12:34:45
      struct tm *ptm = gmtime(&t);
      return !(ptm && ptm->tm_year == 69);
    }
''')

has_post_2038 = compiler.run('''
    #include<stdlib.h>
    #include<time.h>
    int main(int, char *[]) {
      setenv("TZ", "America/Los_Angeles", 1);
      tzset();
      struct tm time2037;
      struct tm time2038;
      strptime("2037-05-05 12:34:56", "%Y-%m-%d %H:%M:%S", &time2037);
      strptime("2038-05-05 12:34:56", "%Y-%m-%d %H:%M:%S", &time2038);
      return (mktime(&time2038) - mktime(&time2037)) <= 31500000;
    }
''')

cdata = configuration_data()
cdata.set10('HAS_PREAD', has_pread)
cdata.set10('HAS_STRPTIME', has_strptime)
cdata.set10('HAS_DIAGNOSTIC_PUSH', has_diagnostic_push)
cdata.set10('HAS_DOUBLE_TO_STRING', has_double_to_string)
cdata.set10('HAS_INT64_TO_STRING', has_int64_to_string)
cdata.set('HAS_PRE_1970', has_pre_1970.returncode() == 0)
cdata.set('HAS_POST_2038', has_post_2038.returncode() == 0)
cdata.set10('HAS_STD_ISNAN', has_std_isnan)
cdata.set10('HAS_BUILTIN_OVERFLOW_CHECK', has_builtin_overflow_check)
cdata.set10('NEEDS_Z_PREFIX', false)  # Meson zlib subproject does not need this

adaptor_header = configure_file(
    input: 'Adaptor.hh.in',
    output: 'Adaptor.hh',
    configuration: cdata,
    format: 'cmake',
)

source_files = [adaptor_header]
source_files += files(
    'io/InputStream.cc',
    'io/OutputStream.cc',
    'io/Cache.cc',
    'sargs/ExpressionTree.cc',
    'sargs/Literal.cc',
    'sargs/PredicateLeaf.cc',
    'sargs/SargsApplier.cc',
    'sargs/SearchArgument.cc',
    'sargs/TruthValue.cc',
    'wrap/orc-proto-wrapper.cc',
    'Adaptor.cc',
    'BlockBuffer.cc',
    'BloomFilter.cc',
    'BpackingDefault.cc',
    'ByteRLE.cc',
    'ColumnPrinter.cc',
    'ColumnReader.cc',
    'ColumnWriter.cc',
    'Common.cc',
    'Compression.cc',
    'ConvertColumnReader.cc',
    'CpuInfoUtil.cc',
    'Exceptions.cc',
    'Geospatial.cc',
    'Int128.cc',
    'LzoDecompressor.cc',
    'MemoryPool.cc',
    'Murmur3.cc',
    'OrcFile.cc',
    'Reader.cc',
    'RLEv1.cc',
    'RLEV2Util.cc',
    'RleDecoderV2.cc',
    'RleEncoderV2.cc',
    'RLE.cc',
    'SchemaEvolution.cc',
    'Statistics.cc',
    'StripeStream.cc',
    'Timezone.cc',
    'TypeImpl.cc',
    'Vector.cc',
    'Writer.cc',
    'Dictionary.cc',
)

incdir = include_directories('../include')
orc_format_proto_dep = dependency('orc_format_proto')
# zstd requires us to add the threads
threads_dep = dependency('threads')

orc_lib = library(
    'orc',
    sources: source_files,
    cpp_args: [
        '-DBUILD_SPARSEHASH'
    ],
    dependencies: [
        orc_format_proto_dep,
        protobuf_dep,
        zlib_dep,
        snappy_dep,
        lz4_dep,
        zstd_dep,
        threads_dep,
        sparsehash_c11_dep,
    ],
    include_directories: incdir,
    install: true,
)

orc_dep = declare_dependency(
    link_with: orc_lib,
    include_directories: incdir,
    dependencies: orc_format_proto_dep,
)
