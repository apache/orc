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

cmake_minimum_required (VERSION 2.6)

project(ORC)

# Version number of package
SET(CPACK_PACKAGE_VERSION_MAJOR "1")
SET(CPACK_PACKAGE_VERSION_MINOR "4")
SET(CPACK_PACKAGE_VERSION_PATCH "4-SNAPSHOT")
SET(ORC_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")

option (BUILD_JAVA
    "Include ORC Java library in the build process"
     ON)

# Make sure that a build type is selected
if (NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to ReleaseWithDebugInfo")
  set (CMAKE_BUILD_TYPE "RELWITHDEBINFO")
else ()
  message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Set the package format
SET(CPACK_GENERATOR "TGZ")

INCLUDE(CPack)

set (GMOCK_VERSION "1.7.0")
set (GMOCK_INCLUDE_DIRS
     "${CMAKE_SOURCE_DIR}/c++/libs/gmock-${GMOCK_VERSION}/include"
     "${CMAKE_SOURCE_DIR}/c++/libs/gmock-${GMOCK_VERSION}/gtest/include"
    )
set (GMOCK_LIBRARIES gmock)
if(NOT APPLE AND NOT MSVC)
  list (APPEND GMOCK_LIBRARIES pthread)
endif(NOT APPLE AND NOT MSVC)

set (LZ4_VERSION "r131")
set (LZ4_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/c++/libs/lz4-${LZ4_VERSION}/lib")
set (LZ4_LIBRARIES lz4)

set (PROTOBUF_VERSION "2.6.0")
set (PROTOBUF_INCLUDE_DIRS
     "${CMAKE_SOURCE_DIR}/c++/libs/protobuf-${PROTOBUF_VERSION}/src")
set (PROTOBUF_LIBRARIES protobuf)
set (PROTOBUF_EXECUTABLE "${CMAKE_BINARY_DIR}/c++/libs/protobuf-${PROTOBUF_VERSION}/protoc")

set (SNAPPY_VERSION "1.1.2")
set (SNAPPY_INCLUDE_DIRS
     "${CMAKE_SOURCE_DIR}/c++/libs/snappy-${SNAPPY_VERSION}")
set (SNAPPY_LIBRARIES snappy)

set (ZLIB_VERSION "1.2.8")
set (ZLIB_INCLUDE_DIRS
     "${CMAKE_SOURCE_DIR}/c++/libs/zlib-${ZLIB_VERSION}"
     "${CMAKE_BINARY_DIR}/c++/libs/zlib-${ZLIB_VERSION}"
    )
set (ZLIB_LIBRARIES zlib)

#
# Compiler specific flags
#
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
message(STATUS "compiler ${CMAKE_CXX_COMPILER_ID} version ${CMAKE_CXX_COMPILER_VERSION}")
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set (CXX11_FLAGS "-std=c++11")
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-covered-switch-default")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-missing-noreturn -Wno-unknown-pragmas")
  set (WARN_FLAGS "${WARN_FLAGS} -Wconversion -Werror")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set (WARN_FLAGS "-Wall -Wno-unknown-pragmas -Wconversion -Werror")
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR
      CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.7")
    set (CXX11_FLAGS "-std=c++0x")
  else ()
    set (CXX11_FLAGS "-std=c++11")
  endif ()
elseif (MSVC)
  add_definitions (-D_SCL_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_SECURE_NO_WARNINGS)
  # TODO: We assume MSVC debug mode. In the future, set these flags
  # appropriately for all build mode.
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /MTd")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:libc.lib")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:libcmt.lib")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:msvcrt.lib")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:libcd.lib")

  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} /NODEFAULTLIB:msvcrtd.lib")
endif ()

enable_testing()

set (EXAMPLE_DIRECTORY ${CMAKE_SOURCE_DIR}/examples)

add_subdirectory(c++)
add_subdirectory(tools)
if (BUILD_JAVA)
  add_subdirectory(java)
endif()

# Add another target called test-out that prints the results on failure
if (CMAKE_CONFIGURATION_TYPES)
  add_custom_target (test-out
    COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
      --output-on-failure --build-config "$<CONFIGURATION>"
  )
else (CMAKE_CONFIGURATION_TYPES)
  add_custom_target (test-out
    COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
      --output-on-failure
  )
endif (CMAKE_CONFIGURATION_TYPES)

install(
  FILES LICENSE NOTICE
  DESTINATION .)
