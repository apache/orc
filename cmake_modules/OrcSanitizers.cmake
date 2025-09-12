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

# =============================================================================
# Sanitizer Options
# =============================================================================

option(ORC_ENABLE_ASAN "Enable Address Sanitizer" OFF)

option(ORC_ENABLE_UBSAN "Enable Undefined Behavior Sanitizer" OFF)

# =============================================================================
# Sanitizer Support Detection
# =============================================================================

function(orc_check_sanitizer_support)
  set(ORC_SANITIZERS_SUPPORTED
      FALSE
      PARENT_SCOPE)

  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(ORC_SANITIZERS_SUPPORTED
        TRUE
        PARENT_SCOPE)
    message(DEBUG "Sanitizers supported with ${CMAKE_CXX_COMPILER_ID} compiler")
  else()
    message(DEBUG
            "Sanitizers not supported with ${CMAKE_CXX_COMPILER_ID} compiler")
  endif()
endfunction()

# =============================================================================
# Address Sanitizer Configuration
# =============================================================================

function(orc_configure_asan)
  if(NOT ORC_ENABLE_ASAN)
    return()
  endif()

  orc_check_sanitizer_support()
  if(NOT ORC_SANITIZERS_SUPPORTED)
    message(
      WARNING "Address Sanitizer is only supported for GCC and Clang compilers")
    return()
  endif()

  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer"
      PARENT_SCOPE)
  set(CMAKE_C_FLAGS
      "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer"
      PARENT_SCOPE)

  message(STATUS "Address Sanitizer enabled")
endfunction()

# =============================================================================
# Undefined Behavior Sanitizer Configuration
# =============================================================================

function(orc_configure_ubsan)
  if(NOT ORC_ENABLE_UBSAN)
    return()
  endif()

  orc_check_sanitizer_support()
  if(NOT ORC_SANITIZERS_SUPPORTED)
    message(
      WARNING
        "Undefined Behavior Sanitizer is only supported for GCC and Clang compilers"
    )
    return()
  endif()

  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr -fno-sanitize-recover=all"
      PARENT_SCOPE)
  set(CMAKE_C_FLAGS
      "${CMAKE_C_FLAGS} -fsanitize=undefined -fno-sanitize=alignment,vptr -fno-sanitize-recover=all"
      PARENT_SCOPE)

  message(STATUS "Undefined Behavior Sanitizer enabled")
endfunction()

# =============================================================================
# GCC-Specific Threading Configuration
# =============================================================================

function(orc_configure_gcc_sanitizer_threading)
  if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    return()
  endif()

  if(NOT (ORC_ENABLE_ASAN OR ORC_ENABLE_UBSAN))
    return()
  endif()

  # GCC sanitizers require explicit pthread linking
  set(CMAKE_THREAD_LIBS_INIT
      "-lpthread"
      PARENT_SCOPE)
  set(THREADS_PREFER_PTHREAD_FLAG
      ON
      PARENT_SCOPE)

  message(STATUS "Forcing pthread linking for GCC with sanitizers")
endfunction()

# =============================================================================
# Combined Sanitizer Setup
# =============================================================================

function(orc_setup_sanitizers)
  orc_configure_asan()
  orc_configure_ubsan()
  orc_configure_gcc_sanitizer_threading()

  if(ORC_ENABLE_ASAN OR ORC_ENABLE_UBSAN)
    message(STATUS "Sanitizer configuration:")
    message(STATUS "  Address Sanitizer: ${ORC_ENABLE_ASAN}")
    message(STATUS "  Undefined Behavior Sanitizer: ${ORC_ENABLE_UBSAN}")
  endif()
endfunction()

# =============================================================================
# Module Initialization
# =============================================================================

orc_setup_sanitizers()
