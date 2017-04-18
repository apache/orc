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
  ${PROTOBUF_INCLUDE_DIRS}
  ${ZLIB_INCLUDE_DIRS}
  ${SNAPPY_INCLUDE_DIRS}
  ${GMOCK_INCLUDE_DIRS}
)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX11_FLAGS} ${WARN_FLAGS}")

add_executable (orc-test
  TestByteRle.cc
  TestColumnPrinter.cc
  TestColumnReader.cc
  TestCompression.cc
  TestDriver.cc
  TestInt128.cc
  TestReader.cc
  TestRle.cc
  TestTimezone.cc
  TestTimestampStatistics.cc
  TestType.cc
)

target_link_libraries (orc-test
  orc
  ${GMOCK_LIBRARIES}
  ${LZ4_LIBRARIES}
  ${PROTOBUF_LIBRARIES}
  ${SNAPPY_LIBRARIES}
  ${ZLIB_LIBRARIES}
)

add_executable (create-test-files
  CreateTestFiles.cc
)

target_link_libraries (create-test-files
  orc
  ${PROTOBUF_LIBRARIES}
)

add_test (NAME orc-test COMMAND orc-test)

