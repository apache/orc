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

# ZSTD_HOME environmental variable is used to check for Zstd headers and static library

# ZSTD_INCLUDE_DIR: directory containing headers
# ZSTD_LIBRARY: path to libzstd
# ZSTD_STATIC_LIB: path to libzstd.a
# ZSTD_FOUND: whether zstd has been found

if (NOT ZSTD_HOME)
  if (DEFINED ENV{ZSTD_HOME})
    set (ZSTD_HOME "$ENV{ZSTD_HOME}")
  elseif (ZSTD_ROOT)
    set (ZSTD_HOME "${ZSTD_ROOT}")
  elseif (DEFINED ENV{ZSTD_ROOT})
    set (ZSTD_HOME "$ENV{ZSTD_ROOT}")
  endif ()
endif ()

if( NOT "${ZSTD_HOME}" STREQUAL "")
  file (TO_CMAKE_PATH "${ZSTD_HOME}" _zstd_path)
endif()

message (STATUS "ZSTD_HOME: ${ZSTD_HOME}")

find_path (ZSTD_INCLUDE_DIR zstd.h HINTS
        ${_zstd_path}
        NO_DEFAULT_PATH
        PATH_SUFFIXES "include")

find_library (ZSTD_LIBRARY NAMES zstd HINTS
        ${_zstd_path}
        PATH_SUFFIXES "lib")

find_library (ZSTD_STATIC_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}zstd${CMAKE_STATIC_LIBRARY_SUFFIX} HINTS
        ${_zstd_path}
        PATH_SUFFIXES "lib")

if (ZSTD_INCLUDE_DIR AND ZSTD_LIBRARY)
  set (ZSTD_FOUND TRUE)
  set (ZSTD_HEADER_NAME zstd.h)
  set (ZSTD_HEADER ${ZSTD_INCLUDE_DIR}/${ZSTD_HEADER_NAME})
else ()
  set (ZSTD_FOUND FALSE)
endif ()

if (ZSTD_FOUND)
  message (STATUS "Found the zstd header: ${ZSTD_HEADER}")
  message (STATUS "Found the zstd library: ${ZSTD_LIBRARY}")
  if (ZSTD_STATIC_LIB)
    message (STATUS "Found the zstd static library: ${ZSTD_STATIC_LIB}")
  endif ()
else()
  if (_zstd_path)
    set (ZSTD_ERR_MSG "Could not find zstd. Looked in ${_zstd_path}.")
  else ()
    set (ZSTD_ERR_MSG "Could not find zstd in system search paths.")
  endif()

  if (ZSTD_FIND_REQUIRED)
    message (FATAL_ERROR "${ZSTD_ERR_MSG}")
  else ()
    message (STATUS "${ZSTD_ERR_MSG}")
  endif ()
endif()

mark_as_advanced (
        ZSTD_INCLUDE_DIR
        ZSTD_STATIC_LIB
        ZSTD_LIBRARY
)

if(ZSTD_FOUND)
  if(NOT TARGET zstd::libzstd_static AND ZSTD_STATIC_LIB)
    add_library(zstd::libzstd_static STATIC IMPORTED)
    set_target_properties(zstd::libzstd_static
                          PROPERTIES IMPORTED_LOCATION "${ZSTD_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}")
  endif()
  if(NOT TARGET zstd::libzstd_shared AND NOT ZSTD_STATIC_LIB)
    add_library(zstd::libzstd_shared SHARED IMPORTED)
    set_target_properties(zstd::libzstd_shared
                          PROPERTIES IMPORTED_LOCATION "${ZSTD_LIBRARY}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}")
  endif()
endif()
