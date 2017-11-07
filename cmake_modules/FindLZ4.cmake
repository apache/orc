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

# LZ4_HOME environmental variable is used to check for LZ4 headers and static library

# LZ4_FOUND is set if LZ4 is found
# LZ4_INCLUDE_DIRS is set to the header directory
# LZ4_LIBS is set to lz4.a static library
# LZ4_PREFIX will be LZ4_HOME

if (NOT "${LZ4_HOME}" STREQUAL "")
  message (STATUS "LZ4_HOME set: ${LZ4_HOME}")
endif ()

file (TO_CMAKE_PATH "${LZ4_HOME}" _lz4_path )

find_path (LZ4_INCLUDE_DIRS lz4.h HINTS
  ${_lz4_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include")

find_library (LZ4_LIBRARIES NAMES lz4 PATHS
  ${_lz4_path}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib")

if (LZ4_INCLUDE_DIRS AND LZ4_LIBRARIES)
  set (LZ4_FOUND TRUE)
  set (LZ4_PREFIX ${LZ4_HOME})
  get_filename_component (LZ4_LIBS ${LZ4_LIBRARIES} PATH )
  set (LZ4_HEADER_NAME lz4.h)
  set (LZ4_HEADER ${LZ4_INCLUDE_DIRS}/${LZ4_HEADER_NAME})
  set (LZ4_LIB_NAME lz4)
  set (LZ4_STATIC_LIB ${LZ4_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${LZ4_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
else ()
  set (LZ4_FOUND FALSE)
endif ()

if (LZ4_FOUND)
  message (STATUS "Found the LZ4 header: ${LZ4_HEADER}")
  message (STATUS "Found the LZ4 library: ${LZ4_STATIC_LIB}")
elseif (NOT "${LZ4_HOME}" STREQUAL "")
  message (STATUS "Could not find LZ4 headers and library")
endif ()
