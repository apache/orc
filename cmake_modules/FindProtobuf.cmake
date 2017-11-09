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

# PROTOBUF_HOME environmental variable is used to check for Protobuf headers and static library

# PROTOBUF_FOUND is set if Protobuf is found
# PROTOBUF_INCLUDE_DIRS is set to the header directory
# PROTOBUF_LIBS is set to protobuf.a protoc.a static libraries
# PROTOBUF_EXECUTABLE is set to protoc
# PROTOBUF_PREFIX will be PROTOBUF_HOME

if (NOT "${PROTOBUF_HOME}" STREQUAL "")
  message (STATUS "PROTOBUF_HOME set: ${PROTOBUF_HOME}")
endif ()

file (TO_CMAKE_PATH "${PROTOBUF_HOME}" _protobuf_path )

find_path (PROTOBUF_INCLUDE_DIRS google/protobuf/io/zero_copy_stream.h HINTS
  ${_protobuf_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_path (PROTOBUF_INCLUDE_DIRS google/protobuf/io/coded_stream.h HINTS
  ${_protobuf_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_library (PROTOBUF_LIBRARY NAMES protobuf PATHS
  ${_protobuf_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib")

find_library (PROTOC_LIBRARY NAMES protoc PATHS
  ${_protobuf_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib")

find_program(PROTOBUF_EXECUTABLE protoc HINTS
  ${_protobuf_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "bin")

if (PROTOBUF_INCLUDE_DIRS AND PROTOBUF_LIBRARY AND PROTOC_LIBRARY AND PROTOBUF_EXECUTABLE)
  set (PROTOBUF_FOUND TRUE)
  set (PROTOBUF_PREFIX ${PROTOBUF_HOME})
  set (PROTOBUF_HEADER ${PROTOBUF_INCLUDE_DIRS}/${PROTOBUF_HEADER_NAME})
  get_filename_component (PROTOBUF_LIB ${PROTOBUF_LIBRARY} PATH )
  get_filename_component (PROTOC_LIB ${PROTOC_LIBRARY} PATH )
  set (PROTOBUF_HEADER_NAME protobuf.h)
  set (PROTOBUF_LIB_NAME protobuf)
  set (PROTOBUF_STATIC_LIB ${PROTOBUF_LIB}/${CMAKE_STATIC_LIBRARY_PREFIX}${PROTOBUF_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set (PROTOC_LIB_NAME protoc)
  set (PROTOC_STATIC_LIB ${PROTOC_LIB}/${CMAKE_STATIC_LIBRARY_PREFIX}${PROTOC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
else ()
  set (PROTOBUF_FOUND FALSE)
endif ()

if (PROTOBUF_FOUND)
  message (STATUS "Found the Protobuf headers: ${PROTOBUF_INCLUDE_DIRS}")
  message (STATUS "Found the Protobuf library: ${PROTOBUF_LIBRARY}")
  message (STATUS "Found the Protobuf library: ${PROTOC_LIBRARY}")
  message (STATUS "Found the Protobuf executable: ${PROTOBUF_EXECUTABLE}")
elseif (NOT "${PROTOBUF_HOME}" STREQUAL "")
  message (STATUS "Could not find Protobuf headers, library and binary")
endif ()
