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

INCLUDE(CheckCXXCompilerFlag)
message(STATUS "System processor: ${CMAKE_SYSTEM_PROCESSOR}")

if(NOT DEFINED ORC_SIMD_LEVEL)
  set(ORC_SIMD_LEVEL
      "DEFAULT"
      CACHE STRING "Compile time SIMD optimization level")
endif()

if(NOT DEFINED ORC_CPU_FLAG)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "AMD64|X86|x86|i[3456]86|x64")
    set(ORC_CPU_FLAG "x86")
  else()
    message(STATUS "Unsupported system processor for SIMD optimization")
  endif()
endif()

# Check architecture specific compiler flags
if(ORC_CPU_FLAG STREQUAL "x86")
  # x86/amd64 compiler flags, msvc/gcc/clang
  if(MSVC)
    set(ORC_AVX512_FLAG "/arch:AVX512")
  else()
    # "arch=native" selects the CPU to generate code for at compilation time by determining the processor type of the compiling machine.
    # Using -march=native enables all instruction subsets supported by the local machine.
    # Using -mtune=native produces code optimized for the local machine under the constraints of the selected instruction set.
    set(ORC_AVX512_FLAG "-march=native -mtune=native")
  endif()
  check_cxx_compiler_flag("-mavx512f -mavx512cd -mavx512vl -mavx512dq -mavx512bw" COMPILER_SUPPORT_AVX512)

  if(MINGW)
    # https://gcc.gnu.org/bugzilla/show_bug.cgi?id=65782
    message(STATUS "Disable AVX512 support on MINGW for now")
  else()
    # Check for AVX512 support in the compiler.
    set(OLD_CMAKE_REQURED_FLAGS ${CMAKE_REQUIRED_FLAGS})
    set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} ${ORC_AVX512_FLAG}")
    CHECK_CXX_SOURCE_COMPILES("
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

  if(CXX_SUPPORTS_AVX512)
    execute_process(COMMAND grep flags /proc/cpuinfo
                    COMMAND head -1
                    OUTPUT_VARIABLE flags_ver)
    message(STATUS "CPU ${flags_ver}")
  endif()

  # Runtime SIMD level it can get from compiler
  if(CXX_SUPPORTS_AVX512 AND COMPILER_SUPPORT_AVX512)
    message(STATUS "Enabled the AVX512 for RLE bit-unpacking")
    set(ORC_HAVE_RUNTIME_AVX512 ON)
    set(ORC_SIMD_LEVEL "AVX512")
    add_definitions(-DORC_HAVE_RUNTIME_AVX512)
  else()
    message(STATUS "WARNING: AVX512 required but compiler doesn't support it, failed to enable AVX512.")
    set(ORC_HAVE_RUNTIME_AVX512 OFF)
  endif()
  if(ORC_SIMD_LEVEL STREQUAL "DEFAULT")
    set(ORC_SIMD_LEVEL "NONE")
  endif()

  if(ORC_SIMD_LEVEL STREQUAL "AVX512")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ORC_AVX512_FLAG}")
  endif()
endif()

message(STATUS "ORC_HAVE_RUNTIME_AVX512: ${ORC_HAVE_RUNTIME_AVX512}, ORC_SIMD_LEVEL: ${ORC_SIMD_LEVEL}")
