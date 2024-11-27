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

cmake_minimum_required (VERSION 3.12.0)
if (POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif ()
if (POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif ()

project(ORC C CXX)

# Version number of package
SET(CPACK_PACKAGE_VERSION_MAJOR "2")
SET(CPACK_PACKAGE_VERSION_MINOR "1")
SET(CPACK_PACKAGE_VERSION_PATCH "0-SNAPSHOT")
SET(ORC_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake_modules")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # For clang-tidy.

option (BUILD_JAVA
    "Include ORC Java library in the build process"
     ON)

option (ANALYZE_JAVA
    "Run static analysis of the Java"
     OFF)

option (BUILD_LIBHDFSPP
    "Include LIBHDFSPP library in the build process"
     OFF)

option(BUILD_CPP_TESTS
    "Build the googletest unit tests"
    ON)

option(BUILD_TOOLS
    "Build the tools"
    ON)

option(TEST_VALGRIND_MEMCHECK
    "Run the test suite using valgrind --tool=memcheck"
    OFF)

option(INSTALL_VENDORED_LIBS
    "Install vendored thirdparty libraries along with liborc"
    ON)

option(STOP_BUILD_ON_WARNING
    "Fail the build on C++ warnings"
    ON)

option(BUILD_POSITION_INDEPENDENT_LIB
    "Compile static libraries with position independent code"
    OFF)

option(BUILD_CPP_ENABLE_METRICS
    "Enable the metrics collection at compile phase"
    OFF)

option(BUILD_ENABLE_AVX512
    "Enable build with AVX512 at compile time"
    OFF)

option(ORC_PACKAGE_KIND
    "Arbitrary string that identifies the kind of package"
    "")

# Make sure that a build type is selected
if (NOT CMAKE_BUILD_TYPE)
  message(STATUS "No build type selected, default to ReleaseWithDebugInfo")
  set (CMAKE_BUILD_TYPE "RELWITHDEBINFO")
else ()
  message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Set the package format
SET(CPACK_GENERATOR "TGZ")
SET(CPACK_PACKAGE_VENDOR "Apache ORC")
SET(CPACK_PACKAGE_CONTACT "Apache ORC <dev@orc.apache.org>")

INCLUDE(CPack)
INCLUDE(ExternalProject)

if (BUILD_POSITION_INDEPENDENT_LIB)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

#
# Compiler specific flags
#
# This ensures that things like c++17 get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
elseif(${CMAKE_CXX_STANDARD} VERSION_LESS 17)
  message(FATAL_ERROR "Cannot set a CMAKE_CXX_STANDARD smaller than 17")
endif()
# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if (NOT MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g -fno-omit-frame-pointer")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG -fno-omit-frame-pointer")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
endif ()
message(STATUS "compiler ${CMAKE_CXX_COMPILER_ID} version ${CMAKE_CXX_COMPILER_VERSION}")
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5.0")
    message(FATAL_ERROR "A c++17-compliant compiler is required, please use at least Clang 5")
  else ()
    set (CXX17_FLAGS "-std=c++17")
  endif ()
  set (WARN_FLAGS "-Wall -Wextra")
  if (STOP_BUILD_ON_WARNING)
    set (WARN_FLAGS "${WARN_FLAGS} -Werror")
  endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5.0")
    message(FATAL_ERROR "A c++17-compliant compiler is required, please use at least GCC 5")
  else ()
    set (CXX17_FLAGS "-std=c++17")
  endif ()
  set (WARN_FLAGS "-Wall -Wextra")
  if (STOP_BUILD_ON_WARNING)
    set (WARN_FLAGS "${WARN_FLAGS} -Werror")
  endif ()
elseif (MSVC)
  include(CheckCXXCompilerFlag)
  CHECK_CXX_COMPILER_FLAG("/std:c++17" CPP17_FLAG_SUPPORTED)
  if (CPP17_FLAG_SUPPORTED)
    add_compile_options("/std:c++17")
  else ()
    message(FATAL_ERROR "A c++17-compliant compiler is required")
  endif ()
  add_definitions (-D_SCL_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_NONSTDC_NO_DEPRECATE) # The POSIX name for this item is deprecated
  set (WARN_FLAGS "${WARN_FLAGS} -wd4521") # multiple copy constructors specified
  set (WARN_FLAGS "${WARN_FLAGS} -wd4146") # unary minus operator applied to unsigned type, result still unsigned
endif ()

enable_testing()

INCLUDE(GNUInstallDirs)  # Put it before ThirdpartyToolchain to make CMAKE_INSTALL_LIBDIR available.
set(ORC_INSTALL_CMAKE_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/orc)
if (ORC_PACKAGE_KIND STREQUAL "vcpkg")
  set(ORC_INSTALL_CMAKE_DIR ${CMAKE_INSTALL_DATAROOTDIR}/orc)
endif ()

INCLUDE(CheckSourceCompiles)
INCLUDE(ThirdpartyToolchain)

if (BUILD_ENABLE_AVX512 AND NOT (CMAKE_SYSTEM_PROCESSOR MATCHES "AMD64|X86|x86|i[3456]86|x64"))
  message(WARNING "Only X86 platform support AVX512")
  set (BUILD_ENABLE_AVX512 "OFF")
endif ()

message(STATUS "BUILD_ENABLE_AVX512: ${BUILD_ENABLE_AVX512}")
#
# macOS doesn't fully support AVX512, it has a different way dealing with AVX512 than Windows and Linux.
#
# Here can find the description:
# https://github.com/apple/darwin-xnu/blob/2ff845c2e033bd0ff64b5b6aa6063a1f8f65aa32/osfmk/i386/fpu.c#L174
if (BUILD_ENABLE_AVX512 AND NOT APPLE)
  INCLUDE(ConfigSimdLevel)
endif ()

set (EXAMPLE_DIRECTORY ${CMAKE_SOURCE_DIR}/examples)

add_subdirectory(c++)

install(
  FILES LICENSE NOTICE
  DESTINATION "share/doc/orc")

if (BUILD_JAVA)
  add_subdirectory(java)
endif()

if (BUILD_TOOLS)
  add_subdirectory(tools)
endif ()

if (BUILD_CPP_TESTS)
  # Add another target called test-out that prints the results on failure
  if (CMAKE_CONFIGURATION_TYPES)
    add_custom_target (test-out
      COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
        --output-on-failure --build-config "$<CONFIGURATION>"
    )
  else ()
    add_custom_target (test-out
      COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
        --output-on-failure
    )
  endif ()
endif ()

INCLUDE(CheckFormat)