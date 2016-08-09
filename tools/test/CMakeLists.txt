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
  ${PROJECT_SOURCE_DIR}/c++/include
  ${PROJECT_SOURCE_DIR}/c++/src
  ${PROJECT_SOURCE_DIR}/tools-c++/src
  ${PROJECT_BINARY_DIR}/c++/include
  ${PROJECT_BINARY_DIR}/c++/src
  ${PROTOBUF_INCLUDE_DIRS}
  ${ZLIB_INCLUDE_DIRS}
  ${GMOCK_INCLUDE_DIRS}
)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX11_FLAGS} ${WARN_FLAGS}")

add_executable (tool-test
  gzip.cc
  TestFileMetadata.cc
  TestFileScan.cc
  TestMatch.cc
  ToolTest.cc
)

target_link_libraries (tool-test
  orc
  ${PROTOBUF_LIBRARIES}
  ${GMOCK_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${SNAPPY_LIBRARIES}
)

add_test (tool-test tool-test ${EXAMPLE_DIRECTORY} ${PROJECT_BINARY_DIR})
