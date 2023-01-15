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

cmake_minimum_required (VERSION 3.12.0)
if (POLICY CMP0048)
    cmake_policy(SET CMP0048 NEW)
endif ()
if (POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif ()

project(ORC C CXX)

# Version number of package
SET(CPACK_PACKAGE_VERSION_MAJOR "1")
SET(CPACK_PACKAGE_VERSION_MINOR "9")
SET(CPACK_PACKAGE_VERSION_PATCH "0-SNAPSHOT")
SET(ORC_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake_modules")

option (BUILD_JAVA
    "Include ORC Java library in the build process"
     ON)

option (ANALYZE_JAVA
    "Run static analysis of the Java"
     OFF)

option (BUILD_LIBHDFSPP
    "Include LIBHDFSPP library in the build process"
     ON)

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
    "Enable AVX512 vector decode of bit-packing"
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
SET(CPACK_PACKAGE_VENDOR "Apache ORC")
SET(CPACK_PACKAGE_CONTACT "Apache ORC <dev@orc.apache.org>")

INCLUDE(CPack)
INCLUDE(ExternalProject)

if (BUILD_POSITION_INDEPENDENT_LIB)
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif ()

if(NOT DEFINED ORC_SIMD_LEVEL)
  set(ORC_SIMD_LEVEL
      "DEFAULT"
      CACHE STRING "Compile time SIMD optimization level")
endif()
if(NOT DEFINED ORC_RUNTIME_SIMD_LEVEL)
  set(ORC_RUNTIME_SIMD_LEVEL
      "MAX"
      CACHE STRING "Max runtime SIMD optimization level")
endif()

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
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR
      CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5.0")
    message(FATAL_ERROR "A c++17-compliant compiler is required, please use at least Clang 5")
  else ()
    set (CXX17_FLAGS "-std=c++17")
  endif ()
  set (WARN_FLAGS "-Weverything -Wno-c++98-compat -Wno-missing-prototypes")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++98-compat-pedantic -Wno-padded")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-covered-switch-default")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-missing-noreturn -Wno-unknown-pragmas")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-gnu-zero-variadic-macro-arguments")
  set (WARN_FLAGS "${WARN_FLAGS} -Wno-conversion")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "13.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-reserved-identifier -Wno-suggest-destructor-override -Wno-suggest-override")
  endif()
  if (CMAKE_HOST_APPLE AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "12.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++2a-compat -Wno-unknown-warning-option -Wno-suggest-override -Wno-suggest-destructor-override")
  elseif (CMAKE_HOST_APPLE AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "11.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-c++2a-compat")
  endif ()
  if (STOP_BUILD_ON_WARNING)
    set (WARN_FLAGS "${WARN_FLAGS} -Werror")
  endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR
      CMAKE_CXX_COMPILER_VERSION VERSION_LESS "5.0")
    message(FATAL_ERROR "A c++17-compliant compiler is required, please use at least GCC 5")
  else ()
    set (CXX17_FLAGS "-std=c++17")
  endif ()
  set (WARN_FLAGS "-Wall -Wno-unknown-pragmas -Wno-conversion")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER "12.0")
    set (WARN_FLAGS "${WARN_FLAGS} -Wno-array-bounds -Wno-stringop-overread") # To compile protobuf in Fedora37
  endif ()
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

include(CheckCXXCompilerFlag)
include(CheckCXXSourceCompiles)
message(STATUS "System processor: ${CMAKE_SYSTEM_PROCESSOR}")

if(NOT DEFINED ORC_CPU_FLAG)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "AMD64|X86|x86|i[3456]86|x64")
    set(ORC_CPU_FLAG "x86")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|ARM64|arm64")
    set(ORC_CPU_FLAG "aarch64")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm$|armv[4-7]")
    set(ORC_CPU_FLAG "aarch32")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "powerpc|ppc")
    set(ORC_CPU_FLAG "ppc")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "s390x")
    set(ORC_CPU_FLAG "s390x")
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "riscv64")
    set(ORC_CPU_FLAG "riscv64")
  else()
    message(FATAL_ERROR "Unknown system processor")
  endif()
endif()

# Check architecture specific compiler flags
if(ORC_CPU_FLAG STREQUAL "x86")
  # x86/amd64 compiler flags, msvc/gcc/clang
  if(MSVC)
    set(ORC_SSE4_2_FLAG "")
    set(ORC_AVX2_FLAG "/arch:AVX2")
    set(ORC_AVX512_FLAG "/arch:AVX512")
    set(CXX_SUPPORTS_SSE4_2 TRUE)
  else()
    set(ORC_SSE4_2_FLAG "-msse4.2")
    set(ORC_AVX2_FLAG "-march=haswell")
    # skylake-avx512 consists of AVX512F,AVX512BW,AVX512VL,AVX512CD,AVX512DQ
    set(ORC_AVX512_FLAG "-march=native -mbmi2")
    # Append the avx2/avx512 subset option also, fix issue ORC-9877 for homebrew-cpp
    set(ORC_AVX2_FLAG "${ORC_AVX2_FLAG} -mavx2")
    set(ORC_AVX512_FLAG
        "${ORC_AVX512_FLAG} -mavx512f -mavx512cd -mavx512vl -mavx512dq -mavx512bw -mavx512vbmi")
  endif()
  check_cxx_compiler_flag(${ORC_AVX512_FLAG} CXX_SUPPORTS_AVX512)
  if(MINGW)
    # https://gcc.gnu.org/bugzilla/show_bug.cgi?id=65782
    message(STATUS "Disable AVX512 support on MINGW for now")
  else()
    # Check for AVX512 support in the compiler.
    set(OLD_CMAKE_REQURED_FLAGS ${CMAKE_REQUIRED_FLAGS})
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${ORC_AVX512_FLAG}")
    check_cxx_source_compiles("
      #ifdef _MSC_VER
      #include <intrin.h>
      #else
      #include <immintrin.h>
      #endif

      int main() {
        __m512i mask = _mm512_set1_epi32(0x1);
        char out[32];
        _mm512_storeu_si512(out, mask);
        return 0;
      }"
      CXX_SUPPORTS_AVX512)
    set(CMAKE_REQUIRED_FLAGS ${OLD_CMAKE_REQURED_FLAGS})
  endif()
  # Runtime SIMD level it can get from compiler and ORC_RUNTIME_SIMD_LEVEL
  if(CXX_SUPPORTS_SSE4_2 AND ORC_RUNTIME_SIMD_LEVEL MATCHES
                             "^(SSE4_2|AVX2|AVX512|MAX)$")
    set(ORC_HAVE_RUNTIME_SSE4_2 ON)
    add_definitions(-DORC_HAVE_RUNTIME_SSE4_2)
  endif()
  if(CXX_SUPPORTS_AVX2 AND ORC_RUNTIME_SIMD_LEVEL MATCHES "^(AVX2|AVX512|MAX)$")
    set(ORC_HAVE_RUNTIME_AVX2 ON)
    add_definitions(-DORC_HAVE_RUNTIME_AVX2 -DORC_HAVE_RUNTIME_BMI2)
  endif()
  if(CXX_SUPPORTS_AVX512 AND ORC_RUNTIME_SIMD_LEVEL MATCHES "^(AVX512|MAX)$")
    set(ORC_HAVE_RUNTIME_AVX512 ON)
    add_definitions(-DORC_HAVE_RUNTIME_AVX512 -DORC_HAVE_RUNTIME_BMI2)
  endif()
  if(ORC_SIMD_LEVEL STREQUAL "DEFAULT")
    set(ORC_SIMD_LEVEL "AVX512")
  endif()

elseif(ORC_CPU_FLAG STREQUAL "ppc")
  # power compiler flags, gcc/clang only
  set(ORC_ALTIVEC_FLAG "-maltivec")
  check_cxx_compiler_flag(${ORC_ALTIVEC_FLAG} CXX_SUPPORTS_ALTIVEC)
  if(ORC_SIMD_LEVEL STREQUAL "DEFAULT")
    set(ORC_SIMD_LEVEL "NONE")
  endif()
elseif(ORC_CPU_FLAG STREQUAL "aarch64")
  # Arm64 compiler flags, gcc/clang only
  set(ORC_ARMV8_MARCH "armv8-a")
  check_cxx_compiler_flag("-march=${ORC_ARMV8_MARCH}+sve" CXX_SUPPORTS_SVE)
  if(ORC_SIMD_LEVEL STREQUAL "DEFAULT")
    set(ORC_SIMD_LEVEL "NEON")
  endif()
endif()

# Only enable additional instruction sets if they are supported
if(ORC_CPU_FLAG STREQUAL "x86")
  if(MINGW)
    # Enable _xgetbv() intrinsic to query OS support for ZMM register saves
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mxsave")
  endif()
  if(ORC_SIMD_LEVEL STREQUAL "AVX512")
    if(NOT CXX_SUPPORTS_AVX512)
      message(FATAL_ERROR "AVX512 required but compiler doesn't support it.")
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ORC_AVX512_FLAG}")
    add_definitions(-DORC_HAVE_AVX512 -DORC_HAVE_AVX2 -DORC_HAVE_BMI2
                    -DORC_HAVE_SSE4_2)
  elseif(ORC_SIMD_LEVEL STREQUAL "AVX2")
    if(NOT CXX_SUPPORTS_AVX2)
      message(FATAL_ERROR "AVX2 required but compiler doesn't support it.")
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ORC_AVX2_FLAG}")
    add_definitions(-DORC_HAVE_AVX2 -DORC_HAVE_BMI2 -DORC_HAVE_SSE4_2)
  elseif(ORC_SIMD_LEVEL STREQUAL "SSE4_2")
    if(NOT CXX_SUPPORTS_SSE4_2)
      message(FATAL_ERROR "SSE4.2 required but compiler doesn't support it.")
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ORC_SSE4_2_FLAG}")
    add_definitions(-DORC_HAVE_SSE4_2)
  elseif(NOT ORC_SIMD_LEVEL STREQUAL "NONE")
    message(WARNING "ORC_SIMD_LEVEL=${ORC_SIMD_LEVEL} not supported by x86.")
  endif()
endif()

if (BUILD_CPP_ENABLE_METRICS)
  message(STATUS "Enable the metrics collection")
  add_compile_definitions(ENABLE_METRICS=1)
else ()
  message(STATUS "Disable the metrics collection")
  add_compile_definitions(ENABLE_METRICS=0)
endif ()

if (BUILD_ENABLE_AVX512 AND CXX_SUPPORTS_AVX512 AND ORC_SIMD_LEVEL STREQUAL "AVX512")
  message(STATUS "Enable the AVX512 vector decode of bit-packing")
  add_compile_definitions(ENABLE_AVX512=1)
else ()
  message(STATUS "Disable the AVX512 vector decode of bit-packing")
  add_compile_definitions(ENABLE_AVX512=0)
endif ()

enable_testing()

INCLUDE(CheckSourceCompiles)
INCLUDE(ThirdpartyToolchain)

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
