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

include_directories (
  ${PROJECT_SOURCE_DIR}/c++/include
  ${PROJECT_SOURCE_DIR}/c++/src
  ${PROJECT_BINARY_DIR}/c++/include
  ${PROJECT_BINARY_DIR}/c++/src
  ${PROTOBUF_INCLUDE_DIRS}
  )

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g ${CXX11_FLAGS} ${WARN_FLAGS}")

add_executable (orc-contents
  FileContents.cc
  )

target_link_libraries (orc-contents
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (orc-scan
  FileScan.cc
  )

target_link_libraries (orc-scan
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (orc-metadata
  FileMetadata.cc
  )

target_link_libraries (orc-metadata
  orc
  ${PROTOBUF_LIBRARIES}
  )

 add_executable (orc-statistics
  FileStatistics.cc
  )

target_link_libraries (orc-statistics
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (orc-memory
  FileMemory.cc
  )

target_link_libraries (orc-memory
  orc
  ${PROTOBUF_LIBRARIES}
  )

add_executable (timezone-dump
  TimezoneDump.cc
  )

target_link_libraries (timezone-dump
  orc
  ${PROTOBUF_LIBRARIES}
  )

install(TARGETS
   orc-contents
   orc-metadata
   orc-statistics
   DESTINATION bin)
