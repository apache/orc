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

# SNAPPY_HOME environmental variable is used to check for Snappy headers and static library

# SNAPPY_FOUND is set if Snappy is found
# SNAPPY_INCLUDE_DIRS is set to the header directory
# SNAPPY_LIBS is set to snappy.a static library
# SNAPPY_PREFIX will be SNAPPY_HOME

if (NOT "${SNAPPY_HOME}" STREQUAL "")
  message (STATUS "SNAPPY_HOME set: ${SNAPPY_HOME}")
endif ()

file (TO_CMAKE_PATH "${SNAPPY_HOME}" _snappy_path )

find_path (SNAPPY_INCLUDE_DIRS snappy.h HINTS
  ${_snappy_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_library (SNAPPY_LIBRARIES NAMES snappy PATHS
  ${_snappy_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib")

if (SNAPPY_INCLUDE_DIRS AND SNAPPY_LIBRARIES)
  set (SNAPPY_FOUND TRUE)
  set (SNAPPY_PREFIX ${SNAPPY_HOME})
  get_filename_component (SNAPPY_LIBS ${SNAPPY_LIBRARIES} PATH )
  set (SNAPPY_HEADER_NAME snappy.h)
  set (SNAPPY_HEADER ${SNAPPY_INCLUDE_DIRS}/${SNAPPY_HEADER_NAME})
  set (SNAPPY_LIB_NAME snappy)
  set (SNAPPY_STATIC_LIB ${SNAPPY_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
else ()
  set (SNAPPY_FOUND FALSE)
endif ()

if (SNAPPY_FOUND)
  message (STATUS "Found the Snappy header: ${SNAPPY_HEADER}")
  message (STATUS "Found the Snappy library: ${SNAPPY_STATIC_LIB}")
elseif (NOT "${SNAPPY_HOME}" STREQUAL "")
  message (STATUS "Could not find Snappy headers and library")
endif ()
