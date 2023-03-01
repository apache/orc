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
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm$|armv[4-7]")
    set(ORC_CPU_FLAG "aarch32")
  else()
    message(STATUS "Unknown system processor")
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
    set(ORC_AVX512_FLAG "-march=native -mtune=native")
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

  message(STATUS "BUILD_ENABLE_AVX512: ${BUILD_ENABLE_AVX512}")
  # Runtime SIMD level it can get from compiler
  if(BUILD_ENABLE_AVX512 AND CXX_SUPPORTS_AVX512)
    message(STATUS "Enable the AVX512 vector decode of bit-packing, compiler support AVX512")
    set(ORC_HAVE_RUNTIME_AVX512 ON)
    set(ORC_SIMD_LEVEL "AVX512")
    add_definitions(-DORC_HAVE_RUNTIME_AVX512)
  elseif(BUILD_ENABLE_AVX512 AND NOT CXX_SUPPORTS_AVX512)
    message(FATAL_ERROR "AVX512 required but compiler doesn't support it.")
  elseif(NOT BUILD_ENABLE_AVX512)
    set(ORC_HAVE_RUNTIME_AVX512 OFF)
    message(STATUS "Disable the AVX512 vector decode of bit-packing")
  endif()
  if(ORC_SIMD_LEVEL STREQUAL "DEFAULT")
    set(ORC_SIMD_LEVEL "NONE")
  endif()

  # Enable additional instruction sets if they are supported
  if(MINGW)
    # Enable _xgetbv() intrinsic to query OS support for ZMM register saves
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mxsave")
  endif()
  if(ORC_SIMD_LEVEL STREQUAL "AVX512")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ORC_AVX512_FLAG}")
  elseif(NOT ORC_SIMD_LEVEL STREQUAL "NONE")
    message(WARNING "ORC_SIMD_LEVEL=${ORC_SIMD_LEVEL} not supported by x86.")
  endif()
endif()

message(STATUS "ORC_HAVE_RUNTIME_AVX512: ${ORC_HAVE_RUNTIME_AVX512}, ORC_SIMD_LEVEL: ${ORC_SIMD_LEVEL}")
