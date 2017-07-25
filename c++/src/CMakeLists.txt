# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX11_FLAGS} ${WARN_FLAGS}")

INCLUDE(CheckCXXSourceCompiles)

CHECK_CXX_SOURCE_COMPILES("
    #include<fcntl.h>
    #include<unistd.h>
    int main(int,char*[]){
      int f = open(\"/x/y\", O_RDONLY);
      char buf[100];
      return pread(f, buf, 100, 1000) == 0;
    }"
  HAS_PREAD
)

CHECK_CXX_SOURCE_COMPILES("
    #include<string>
    int main(int,char* argv[]){
      return static_cast<int>(std::stoll(argv[0]));
    }"
  HAS_STOLL
)

CHECK_CXX_SOURCE_COMPILES("
    #include<stdint.h>
    #include<stdio.h>
    int main(int,char*[]){
      int64_t x = 1; printf(\"%lld\",x);
    }"
  INT64_IS_LL
)

CHECK_CXX_SOURCE_COMPILES("
    #ifdef __clang__
      #pragma clang diagnostic push
      #pragma clang diagnostic ignored \"-Wdeprecated\"
      #pragma clang diagnostic pop
   #elif defined(__GNUC__)
      #pragma GCC diagnostic push
      #pragma GCC diagnostic ignored \"-Wdeprecated\"
      #pragma GCC diagnostic pop
   #else
     unknownCompiler!
   #endif
   int main(int, char *[]) {}"
  HAS_DIAGNOSTIC_PUSH
)

CHECK_CXX_SOURCE_COMPILES("
    #include<cmath>
    int main(int, char *[]) {
      return std::isnan(1.0f);
    }"
  HAS_STD_ISNAN
)

CHECK_CXX_SOURCE_COMPILES("
    #include<mutex>
    int main(int, char *[]) {
       std::mutex test_mutex;
       std::lock_guard<std::mutex> lock_mutex(test_mutex);
    }"
  HAS_STD_MUTEX
)

CHECK_CXX_SOURCE_COMPILES("
    #include<string>
    std::string func() {
      std::string var = \"test\";
      return std::move(var);
    }
    int main(int, char *[]) {}"
  NEEDS_REDUNDANT_MOVE
)

INCLUDE(CheckCXXSourceRuns)

CHECK_CXX_SOURCE_RUNS("
    #include<time.h>
    int main(int, char *[]) {
      time_t t = -1000;
      return gmtime(&t) == 0;
    }"
  HAS_PRE_1970
)

CHECK_CXX_SOURCE_RUNS("
    #include<stdlib.h>
    #include<time.h>
    int main(int, char *[]) {
      setenv(\"TZ\", \"America/Los_Angeles\", 1);
      tzset();
      struct tm time2037;
      struct tm time2038;
      strptime(\"2037-05-05 12:34:56\", \"%Y-%m-%d %H:%M:%S\", &time2037);
      strptime(\"2038-05-05 12:34:56\", \"%Y-%m-%d %H:%M:%S\", &time2038);
      return mktime(&time2038) - mktime(&time2037) != 31536000;
    }"
  HAS_POST_2038
)

configure_file (
  "Adaptor.hh.in"
  "${CMAKE_CURRENT_BINARY_DIR}/Adaptor.hh"
  )

include_directories (
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${ZLIB_INCLUDE_DIRS}
  ${SNAPPY_INCLUDE_DIRS}
  ${LZ4_INCLUDE_DIRS}
  )

# To avoid reading the Timezone database from disk, we load the file during the
# build and inject them as binary into the library. This can increase the size
# of library but avoids doing system calls at runtime. The behavior can be
# disabled by using the CMake flag NO_EMBEDDED_TZ_DB
set(TZ_DEPS "")
if (EMBEDDED_TZ_DB)
  message(STATUS "Caching of timezone data enabled.")
  set(DEFAULT_TIMEZONE_DIR "/usr/share/zoneinfo")

  # Check if zone1970.tab is present, otherwise select zone.tab
  # Parse zone tab to extract all availble zones
  execute_process(COMMAND find ${DEFAULT_TIMEZONE_DIR} -type f
    COMMAND grep -v "posix"
    COMMAND grep -v "right"
    COMMAND grep -v "tab"
    COMMAND grep -v "leap" # ignore leap seconds file
    RESULT_VARIABLE TZ_LIST_RC
    OUTPUT_VARIABLE TZ_LIST_OUTPUT
    ERROR_VARIABLE TZ_ERROR)

  if (TZ_LIST_RC)
    message(FATAL_ERROR "Could not parse Timezone database.")
  endif()

  set(FILE2ARRAY "${CMAKE_CURRENT_SOURCE_DIR}/../file2array.sh")

  # Add each target to dependency on Timezone.cc
  string(REPLACE "\n" ";" TZ_LIST_OUT ${TZ_LIST_OUTPUT})
  set(TZ_CLOBBERED)
  set(TZ_CLOBBERED "${TZ_CLOBBERED}#include <map>\n")
  set(TZ_CLOBBERED "${TZ_CLOBBERED}#include <string>\n")
  set(TZ_CLOBBERED "${TZ_CLOBBERED}#include <vector>\n")
  set(TZ_CLOBBERED "${TZ_CLOBBERED}\n")

  # First, generate all header files.
  foreach(TZ_PATH ${TZ_LIST_OUT})
    string(REPLACE "${DEFAULT_TIMEZONE_DIR}/" "" OF_NAME ${TZ_PATH})
    string(REPLACE "/" "_" OF_NAME ${OF_NAME})
    string(REPLACE "-" "_" OF_NAME ${OF_NAME})
    string(REPLACE "+" "__" OF_NAME ${OF_NAME})

    add_custom_command(OUTPUT "${OF_NAME}.h"
      COMMAND ${FILE2ARRAY} -v ${OF_NAME} ${TZ_PATH} >"${OF_NAME}.h"
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    list(APPEND TZ_DEPS "${OF_NAME}.h")
    set(TZ_CLOBBERED "${TZ_CLOBBERED}#include \"${OF_NAME}.h\"\n")
  endforeach()

  # Second, generate the initializer list for the map of timezone buffers.
  set(TZ_CLOBBERED "${TZ_CLOBBERED}extern std::map<std::string, std::vector<unsigned char>> TZ_DATABASE\;\n")
  set(TZ_CLOBBERED "${TZ_CLOBBERED}std::map<std::string, std::vector<unsigned char>> TZ_DATABASE = {\n")
  foreach(TZ_PATH ${TZ_LIST_OUT})
    string(REPLACE "${DEFAULT_TIMEZONE_DIR}/" "" REL_PATH ${TZ_PATH})
    string(REPLACE "/" "_" OF_NAME ${REL_PATH})
    string(REPLACE "-" "_" OF_NAME ${OF_NAME})
    string(REPLACE "+" "__" OF_NAME ${OF_NAME})
    set(TZ_CLOBBERED "${TZ_CLOBBERED}{ \"${REL_PATH}\", ${OF_NAME} }, \n")
  endforeach()
  set(TZ_CLOBBERED "${TZ_CLOBBERED}}\;\n")

  # To make it easier to include the TZ files, generated a clobbered file.
  file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/tz_db_clobbered.h ${TZ_CLOBBERED})

  add_definitions(-DUSE_TZ_DB)

  if (STATIC_TZ)
    add_definitions(-DTZ_DB_STATIC_TZ=${STATIC_TZ})
  endif()
  list(APPEND TZ_DEPS "tz_db_clobbered")
else()
  message(STATUS "Caching of timezone information disabled.")
endif()

add_custom_command(OUTPUT orc_proto.pb.h orc_proto.pb.cc
   COMMAND ${PROTOBUF_EXECUTABLE}
        -I ${CMAKE_SOURCE_DIR}/proto
        --cpp_out="${CMAKE_CURRENT_BINARY_DIR}"
        "${CMAKE_SOURCE_DIR}/proto/orc_proto.proto"
)

add_library (orc STATIC
  "${CMAKE_CURRENT_BINARY_DIR}/Adaptor.hh"
  ${TZ_DEPS}
  orc_proto.pb.h
  io/InputStream.cc
  io/OutputStream.cc
  wrap/orc-proto-wrapper.cc
  ByteRLE.cc
  ColumnPrinter.cc
  ColumnReader.cc
  Common.cc
  Compression.cc
  Exceptions.cc
  Int128.cc
  LzoDecompressor.cc
  MemoryPool.cc
  OrcFile.cc
  Reader.cc
  RLEv1.cc
  RLEv2.cc
  RLE.cc
  Statistics.cc
  StripeStream.cc
  Timezone.cc
  TypeImpl.cc
  Vector.cc
  )

install(TARGETS orc DESTINATION lib)

target_link_libraries (orc
  ${PROTOBUF_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${SNAPPY_LIBRARIES}
  ${LZ4_LIBRARIES}
  )

add_dependencies(orc protoc)
