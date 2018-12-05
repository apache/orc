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

include_directories(
  ${PROJECT_SOURCE_DIR}/c++/src
  ${PROJECT_BINARY_DIR}/c++/include
  ${PROJECT_BINARY_DIR}/c++/src
  ${PROTOBUF_INCLUDE_DIR}
  ${ZLIB_INCLUDE_DIR}
  ${SNAPPY_INCLUDE_DIR}
  ${GTEST_INCLUDE_DIR}
)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX11_FLAGS} ${WARN_FLAGS}")

add_executable (orc-test
  MemoryInputStream.cc
  MemoryOutputStream.cc
  TestBufferedOutputStream.cc
  TestByteRle.cc
  TestByteRLEEncoder.cc
  TestColumnPrinter.cc
  TestColumnReader.cc
  TestColumnStatistics.cc
  TestCompression.cc
  TestDecompression.cc
  TestDecimal.cc
  TestDictionaryEncoding.cc
  TestDriver.cc
  TestInt128.cc
  TestReader.cc
  TestRleDecoder.cc
  TestRleEncoder.cc
  TestRLEV2Util.cc
  TestStripeIndexStatistics.cc
  TestTimestampStatistics.cc
  TestTimezone.cc
  TestType.cc
  TestWriter.cc
)

target_link_libraries (orc-test
  orc
  lz4
  protobuf
  snappy
  zlib
  ${GTEST_LIBRARIES}
)

add_executable (create-test-files
  CreateTestFiles.cc
)

target_link_libraries (create-test-files
  orc
  protobuf
)

if (TEST_VALGRIND_MEMCHECK)
  add_test (orc-test
          valgrind --tool=memcheck --leak-check=full --error-exitcode=1 ./orc-test)
else ()
  add_test (NAME orc-test COMMAND orc-test)
endif ()

if (WIN32)
  set_property(
    TEST orc-test
    PROPERTY
      ENVIRONMENT "TZDIR=${TZDATA_DIR}"
    )
endif ()
