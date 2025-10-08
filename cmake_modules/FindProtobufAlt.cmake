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

# PROTOBUF_HOME environmental variable is used to check for Protobuf headers and static library

# ProtobufAlt_FOUND is set if Protobuf is found
# PROTOBUF_INCLUDE_DIR: directory containing headers
# PROTOBUF_LIBRARY: location of libprotobuf
# PROTOBUF_STATIC_LIB: location of protobuf.a
# PROTOC_LIBRARY: location of libprotoc
# PROTOC_STATIC_LIB: location of protoc.a
# PROTOBUF_EXECUTABLE: location of protoc

if (NOT PROTOBUF_HOME)
  if (DEFINED ENV{PROTOBUF_HOME})
    set (PROTOBUF_HOME "$ENV{PROTOBUF_HOME}")
  elseif (Protobuf_ROOT)
    set (PROTOBUF_HOME "${Protobuf_ROOT}")
  elseif (DEFINED ENV{Protobuf_ROOT})
    set (PROTOBUF_HOME "$ENV{Protobuf_ROOT}")
  elseif (PROTOBUF_ROOT)
    set (PROTOBUF_HOME "${PROTOBUF_ROOT}")
  elseif (DEFINED ENV{PROTOBUF_ROOT})
    set (PROTOBUF_HOME "$ENV{PROTOBUF_ROOT}")
  endif ()
endif ()

if( NOT "${PROTOBUF_HOME}" STREQUAL "")
    file (TO_CMAKE_PATH "${PROTOBUF_HOME}" _protobuf_path)
endif()

message (STATUS "PROTOBUF_HOME: ${PROTOBUF_HOME}")

if (NOT DEFINED CMAKE_STATIC_LIBRARY_SUFFIX)
  if (WIN32)
    set (CMAKE_STATIC_LIBRARY_SUFFIX ".lib")
  else ()
    set (CMAKE_STATIC_LIBRARY_SUFFIX ".a")
  endif ()
endif ()

find_package (Protobuf CONFIG)
if (ProtobufAlt_FOUND)
  if (TARGET protobuf::libprotobuf)
    set (PROTOBUF_LIBRARY protobuf::libprotobuf)
    set (PROTOBUF_STATIC_LIB PROTOBUF_STATIC_LIB-NOTFOUND)
    set (PROTOC_LIBRARY protobuf::libprotoc)
    set (PROTOC_STATIC_LIB PROTOC_STATIC_LIB-NOTFOUND)
    set (PROTOBUF_EXECUTABLE protobuf::protoc)

    get_target_property (target_type protobuf::libprotobuf TYPE)
    if (target_type STREQUAL "STATIC_LIBRARY")
      set (PROTOBUF_STATIC_LIB protobuf::libprotobuf)
    endif ()

    get_target_property (target_type protobuf::libprotoc TYPE)
    if (target_type STREQUAL "STATIC_LIBRARY")
      set (PROTOC_STATIC_LIB protobuf::libprotoc)
    endif ()

    get_target_property (PROTOBUF_INCLUDE_DIR protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)
    if (NOT PROTOBUF_INCLUDE_DIR)
      set (PROTOBUF_INCLUDE_DIR ${Protobuf_INCLUDE_DIRS})
      if (NOT PROTOBUF_INCLUDE_DIR)
        message (FATAL_ERROR "Cannot determine Protobuf include directory from protobuf::libprotobuf and Protobuf_INCLUDE_DIRS.")
      endif ()
    endif ()
  else ()
    set (PROTOBUF_LIBRARY ${Protobuf_LIBRARIES})
    set (PROTOBUF_INCLUDE_DIR ${Protobuf_INCLUDE_DIRS})
    if (NOT PROTOBUF_INCLUDE_DIR)
      message (FATAL_ERROR "Cannot determine Protobuf include directory.")
    endif ()

    if (Protobuf_LIBRARIES MATCHES "\\${CMAKE_STATIC_LIBRARY_SUFFIX}$")
      set (PROTOBUF_STATIC_LIB ${Protobuf_LIBRARIES})
    else ()
      set (PROTOBUF_STATIC_LIB PROTOBUF_STATIC_LIB-NOTFOUND)
    endif ()
  endif ()

else()
    find_path (PROTOBUF_INCLUDE_DIR google/protobuf/io/zero_copy_stream.h HINTS
      ${_protobuf_path}
      NO_DEFAULT_PATH
      PATH_SUFFIXES "include")

    find_path (PROTOBUF_INCLUDE_DIR google/protobuf/io/coded_stream.h HINTS
      ${_protobuf_path}
      NO_DEFAULT_PATH
      PATH_SUFFIXES "include")

    find_library (PROTOBUF_LIBRARY NAMES protobuf libprotobuf HINTS
      ${_protobuf_path}
      PATH_SUFFIXES "lib")

    find_library (PROTOBUF_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
      ${_protobuf_path}
      PATH_SUFFIXES "lib")

    find_library (PROTOC_LIBRARY NAMES protoc libprotoc HINTS
      ${_protobuf_path}
      PATH_SUFFIXES "lib")

    find_library (PROTOC_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
      ${_protobuf_path}
      PATH_SUFFIXES "lib")

    find_program(PROTOBUF_EXECUTABLE protoc HINTS
      ${_protobuf_path}
      NO_DEFAULT_PATH
      PATH_SUFFIXES "bin")
endif ()

if (PROTOBUF_INCLUDE_DIR AND PROTOBUF_LIBRARY AND PROTOC_LIBRARY AND PROTOBUF_EXECUTABLE)
  set (ProtobufAlt_FOUND TRUE)
  set (PROTOBUF_LIB_NAME protobuf)
  set (PROTOC_LIB_NAME protoc)
else ()
  set (ProtobufAlt_FOUND FALSE)
endif ()

if (ProtobufAlt_FOUND)
  message (STATUS "Found the Protobuf headers: ${PROTOBUF_INCLUDE_DIR}")
  message (STATUS "Found the Protobuf library: ${PROTOBUF_LIBRARY}")
  message (STATUS "Found the Protoc library: ${PROTOC_LIBRARY}")
  message (STATUS "Found the Protoc executable: ${PROTOBUF_EXECUTABLE}")
  if (PROTOBUF_STATIC_LIB)
     message (STATUS "Found the Protobuf static library: ${PROTOBUF_STATIC_LIB}")
  endif ()
  if (PROTOC_STATIC_LIB)
     message (STATUS "Found the Protoc static library: ${PROTOC_STATIC_LIB}")
  endif ()
else()
  if (_protobuf_path)
    set (PROTOBUF_ERR_MSG "Could not find Protobuf. Looked in ${_protobuf_path}.")
  else ()
    set (PROTOBUF_ERR_MSG "Could not find Protobuf in system search paths.")
  endif()

  if (Protobuf_FIND_REQUIRED)
    message (FATAL_ERROR "${PROTOBUF_ERR_MSG}")
  else ()
    message (STATUS "${PROTOBUF_ERR_MSG}")
  endif ()
endif()

mark_as_advanced (
  PROTOBUF_INCLUDE_DIR
  PROTOBUF_LIBRARY
  PROTOBUF_STATIC_LIB
  PROTOC_STATIC_LIB
  PROTOC_LIBRARY
)

if(ProtobufAlt_FOUND AND NOT TARGET protobuf::libprotobuf)
  add_library(protobuf::libprotobuf UNKNOWN IMPORTED)
  set_target_properties(protobuf::libprotobuf
                        PROPERTIES IMPORTED_LOCATION "${PROTOBUF_LIBRARY}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${PROTOBUF_INCLUDE_DIR}")
endif()
