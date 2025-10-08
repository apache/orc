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

INCLUDE(ExternalProject)
INCLUDE(FetchContent)

set(ORC_SYSTEM_DEPENDENCIES)
set(ORC_INSTALL_INTERFACE_TARGETS)

set(ORC_FORMAT_VERSION "1.1.1")
set(LZ4_VERSION "1.10.0")
set(SNAPPY_VERSION "1.2.2")
set(ZLIB_VERSION "1.3.1")
set(GTEST_VERSION "1.17.0")
set(PROTOBUF_VERSION "3.5.1")
set(ZSTD_VERSION "1.5.7")
set(SPARSEHASH_VERSION "2.11.1")

option(ORC_PREFER_STATIC_PROTOBUF "Prefer static protobuf library, if available" ON)
option(ORC_PREFER_STATIC_SNAPPY   "Prefer static snappy library, if available"   ON)
option(ORC_PREFER_STATIC_LZ4      "Prefer static lz4 library, if available"      ON)
option(ORC_PREFER_STATIC_ZSTD     "Prefer static zstd library, if available"     ON)
option(ORC_PREFER_STATIC_ZLIB     "Prefer static zlib library, if available"     ON)
option(ORC_PREFER_STATIC_GMOCK    "Prefer static gmock library, if available"    ON)

# zstd requires us to add the threads
FIND_PACKAGE(Threads REQUIRED)

set(THIRDPARTY_DIR "${PROJECT_BINARY_DIR}/c++/libs/thirdparty")
set(THIRDPARTY_LOG_OPTIONS LOG_CONFIGURE 1
                           LOG_BUILD 1
                           LOG_INSTALL 1
                           LOG_DOWNLOAD 1)
set(THIRDPARTY_CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}")
if (CMAKE_GENERATOR_TOOLSET)
  list(APPEND THIRDPARTY_CONFIGURE_COMMAND -T "${CMAKE_GENERATOR_TOOLSET}")
endif ()

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

if (DEFINED ENV{SNAPPY_HOME})
  set (SNAPPY_HOME "$ENV{SNAPPY_HOME}")
endif ()

if (DEFINED ENV{ZLIB_HOME})
  set (ZLIB_HOME "$ENV{ZLIB_HOME}")
endif ()

if (DEFINED ENV{LZ4_HOME})
  set (LZ4_HOME "$ENV{LZ4_HOME}")
endif ()

if (DEFINED ENV{PROTOBUF_HOME})
  set (PROTOBUF_HOME "$ENV{PROTOBUF_HOME}")
endif ()

if (DEFINED ENV{ZSTD_HOME})
  set (ZSTD_HOME "$ENV{ZSTD_HOME}")
endif ()

if (DEFINED ENV{GTEST_HOME})
  set (GTEST_HOME "$ENV{GTEST_HOME}")
endif ()

# ----------------------------------------------------------------------
# Macros for adding third-party libraries
macro (orc_add_resolved_library target_name link_lib include_dir)
  add_library (${target_name} INTERFACE IMPORTED GLOBAL)
  target_link_libraries (${target_name} INTERFACE ${link_lib})
  target_include_directories (${target_name} SYSTEM INTERFACE ${include_dir})
endmacro ()

macro (orc_add_built_library external_project_name target_name link_lib include_dir)
  file (MAKE_DIRECTORY "${include_dir}")

  add_library (${target_name} STATIC IMPORTED)
  set_target_properties (${target_name} PROPERTIES IMPORTED_LOCATION "${link_lib}")
  target_include_directories (${target_name} BEFORE INTERFACE "${include_dir}")

  add_dependencies (${target_name} ${external_project_name})
  if (INSTALL_VENDORED_LIBS)
    install (FILES "${link_lib}" DESTINATION "${CMAKE_INSTALL_LIBDIR}")
  endif ()
endmacro ()

function(orc_provide_cmake_module MODULE_NAME)
  set(module "${PROJECT_SOURCE_DIR}/cmake_modules/${MODULE_NAME}.cmake")
  if(EXISTS "${module}")
    message(STATUS "Providing CMake module for ${MODULE_NAME} as part of CMake package")
    install(FILES "${module}" DESTINATION "${ORC_INSTALL_CMAKE_DIR}")
  endif()
endfunction()

function(orc_provide_find_module PACKAGE_NAME)
  orc_provide_cmake_module("Find${PACKAGE_NAME}")
endfunction()

# ----------------------------------------------------------------------
# FetchContent

include(FetchContent)
set(FC_DECLARE_COMMON_OPTIONS)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
  list(APPEND FC_DECLARE_COMMON_OPTIONS EXCLUDE_FROM_ALL TRUE)
endif()

macro(prepare_fetchcontent)
  set(BUILD_SHARED_LIBS OFF)
  set(BUILD_STATIC_LIBS ON)
  set(CMAKE_COMPILE_WARNING_AS_ERROR OFF)
  set(CMAKE_WARN_DEPRECATED OFF)
  set(CMAKE_EXPORT_NO_PACKAGE_REGISTRY ON)
  set(CMAKE_POLICY_VERSION_MINIMUM 3.25)
  if(BUILD_POSITION_INDEPENDENT_LIB)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
  endif()
  # Use "NEW" for CMP0077 by default.
  #
  # https://cmake.org/cmake/help/latest/policy/CMP0077.html
  #
  # option() honors normal variables.
  set(CMAKE_POLICY_DEFAULT_CMP0077 NEW CACHE STRING "")
endmacro()

# ----------------------------------------------------------------------
# ORC Format
if(DEFINED ENV{ORC_FORMAT_URL})
  set(ORC_FORMAT_SOURCE_URL "$ENV{ORC_FORMAT_URL}")
  message(STATUS "Using ORC_FORMAT_URL: ${ORC_FORMAT_SOURCE_URL}")
else()
  set(ORC_FORMAT_SOURCE_URL "https://www.apache.org/dyn/closer.lua/orc/orc-format-${ORC_FORMAT_VERSION}/orc-format-${ORC_FORMAT_VERSION}.tar.gz?action=download" )
  message(STATUS "Using DEFAULT URL: ${ORC_FORMAT_SOURCE_URL}")
endif()
ExternalProject_Add (orc-format_ep
  URL ${ORC_FORMAT_SOURCE_URL}
  URL_HASH SHA256=584dfe2a4202946178fd8fc7d1239be7805b9ed4596ab2042dee739e7880992b
  CONFIGURE_COMMAND ""
  BUILD_COMMAND     ""
  INSTALL_COMMAND     ""
  TEST_COMMAND     ""
)

# ----------------------------------------------------------------------
# Protobuf
#
# XXX: It must be processed before ZLIB, otherwise ZLIB_LIBRARIES will interfere with building protobuf.

if (ORC_PACKAGE_KIND STREQUAL "conan")
  find_package (Protobuf REQUIRED CONFIG)
  add_library (orc_protobuf INTERFACE)
  target_link_libraries(orc_protobuf INTERFACE protobuf::protobuf)
  list (APPEND ORC_SYSTEM_DEPENDENCIES Protobuf)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:protobuf::protobuf>")
elseif (ORC_PACKAGE_KIND STREQUAL "vcpkg")
  find_package(Protobuf CONFIG REQUIRED)
  add_library (orc_protobuf INTERFACE IMPORTED)
  target_link_libraries(orc_protobuf INTERFACE protobuf::libprotobuf)
  list (APPEND ORC_SYSTEM_DEPENDENCIES Protobuf)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:protobuf::libprotobuf>")
  set (PROTOBUF_EXECUTABLE protobuf::protoc)
elseif (NOT "${PROTOBUF_HOME}" STREQUAL "")
  find_package (ProtobufAlt REQUIRED)

  if (ORC_PREFER_STATIC_PROTOBUF AND PROTOBUF_STATIC_LIB)
    orc_add_resolved_library (orc_protobuf ${PROTOBUF_STATIC_LIB} ${PROTOBUF_INCLUDE_DIR})
  else ()
    orc_add_resolved_library (orc_protobuf ${PROTOBUF_LIBRARY} ${PROTOBUF_INCLUDE_DIR})
  endif ()

  if (ORC_PREFER_STATIC_PROTOBUF AND PROTOC_STATIC_LIB)
    orc_add_resolved_library (orc_protoc ${PROTOC_STATIC_LIB} ${PROTOBUF_INCLUDE_DIR})
  else ()
    orc_add_resolved_library (orc_protoc ${PROTOC_LIBRARY} ${PROTOBUF_INCLUDE_DIR})
  endif ()

  list (APPEND ORC_SYSTEM_DEPENDENCIES ProtobufAlt)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:protobuf::libprotobuf>")
  orc_provide_find_module (ProtobufAlt)
else ()
  block(PROPAGATE ORC_SYSTEM_DEPENDENCIES ORC_INSTALL_INTERFACE_TARGETS PROTOBUF_EXECUTABLE)
    prepare_fetchcontent()

    set(protobuf_INSTALL OFF)
    set(protobuf_BUILD_TESTS OFF)
    set(protobuf_BUILD_PROTOBUF_BINARIES ON)
    set(protobuf_BUILD_PROTOC_BINARIES ON)
    set(protobuf_WITH_ZLIB OFF)
    set(protobuf_BUILD_SHARED_LIBS OFF)

    # Set compiler flags to suppress warnings before fetching protobuf
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-deprecated-declarations")
    endif()

    fetchcontent_declare(protobuf
      URL "https://github.com/google/protobuf/archive/v${PROTOBUF_VERSION}.tar.gz"
      SOURCE_SUBDIR "cmake"
      FIND_PACKAGE_ARGS
      NAMES Protobuf
      CONFIG
      )
    fetchcontent_makeavailable(protobuf)

    if(protobuf_SOURCE_DIR)
      message(STATUS "Using vendored protobuf")

      add_library(protobuf::libprotobuf ALIAS libprotobuf)
      add_executable(protobuf::protoc ALIAS protoc)

      if(BUILD_POSITION_INDEPENDENT_LIB)
        set_target_properties(libprotobuf PROPERTIES POSITION_INDEPENDENT_CODE ON)
        set_target_properties(protoc PROPERTIES POSITION_INDEPENDENT_CODE ON)
      endif()

      if(INSTALL_VENDORED_LIBS)
        set_target_properties(libprotobuf PROPERTIES OUTPUT_NAME "orc_vendored_protobuf")
        install(TARGETS libprotobuf
                EXPORT orc_targets
                RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
                ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
                LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
      endif()

      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:orc::libprotobuf>")
    else()
      message(STATUS "Using system protobuf")
      list(APPEND ORC_SYSTEM_DEPENDENCIES Protobuf)
      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:protobuf::libprotobuf>")
    endif()

    add_library(orc_protobuf INTERFACE IMPORTED)
    target_link_libraries(orc_protobuf INTERFACE protobuf::libprotobuf)
    set(PROTOBUF_EXECUTABLE protobuf::protoc)
  endblock()
endif ()

add_library (orc::protobuf ALIAS orc_protobuf)

# ----------------------------------------------------------------------
# Snappy
if (ORC_PACKAGE_KIND STREQUAL "conan")
  find_package (Snappy REQUIRED CONFIG)
  add_library (orc_snappy INTERFACE)
  target_link_libraries(orc_snappy INTERFACE Snappy::snappy)
  list (APPEND ORC_SYSTEM_DEPENDENCIES Snappy)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:Snappy::snappy>")
elseif (ORC_PACKAGE_KIND STREQUAL "vcpkg")
  find_package(Snappy CONFIG REQUIRED)
  add_library (orc_snappy INTERFACE IMPORTED)
  target_link_libraries(orc_snappy INTERFACE Snappy::snappy)
  list (APPEND ORC_SYSTEM_DEPENDENCIES Snappy)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:Snappy::snappy>")
elseif (NOT "${SNAPPY_HOME}" STREQUAL "")
  find_package (SnappyAlt REQUIRED)
  if (ORC_PREFER_STATIC_SNAPPY AND SNAPPY_STATIC_LIB)
    orc_add_resolved_library (orc_snappy ${SNAPPY_STATIC_LIB} ${SNAPPY_INCLUDE_DIR})
  else ()
    orc_add_resolved_library (orc_snappy ${SNAPPY_LIBRARY} ${SNAPPY_INCLUDE_DIR})
  endif ()
  list (APPEND ORC_SYSTEM_DEPENDENCIES SnappyAlt)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:Snappy::snappy>")
  orc_provide_find_module (SnappyAlt)
else ()
  block(PROPAGATE ORC_SYSTEM_DEPENDENCIES ORC_INSTALL_INTERFACE_TARGETS)
    prepare_fetchcontent()

    set(SNAPPY_BUILD_TESTS OFF)
    set(SNAPPY_BUILD_BENCHMARKS OFF)
    set(SNAPPY_INSTALL OFF)

    fetchcontent_declare(Snappy
      URL "https://github.com/google/snappy/archive/${SNAPPY_VERSION}.tar.gz"
      FIND_PACKAGE_ARGS
      NAMES Snappy
      CONFIG
      )
    fetchcontent_makeavailable(Snappy)

    if(snappy_SOURCE_DIR)
      message(STATUS "Using vendored snappy")
      if(NOT TARGET Snappy::snappy)
        add_library(Snappy::snappy INTERFACE IMPORTED)
        target_link_libraries(Snappy::snappy INTERFACE snappy)
        target_include_directories(Snappy::snappy INTERFACE ${snappy_SOURCE_DIR} ${snappy_BINARY_DIR})
      endif()

      if(BUILD_POSITION_INDEPENDENT_LIB)
        set_target_properties(snappy POSITION_INDEPENDENT_CODE ON)
      endif()

      if(INSTALL_VENDORED_LIBS)
        set_target_properties(snappy PROPERTIES OUTPUT_NAME "orc_vendored_snappy")

        install(FILES ${snappy_SOURCE_DIR}/snappy-c.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
        install(FILES ${snappy_SOURCE_DIR}/snappy-sinksource.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
        install(FILES ${snappy_SOURCE_DIR}/snappy.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
        install(FILES ${snappy_BINARY_DIR}/snappy-stubs-public.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
        install(TARGETS snappy
                EXPORT orc_targets
                RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
                ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
                LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
      endif()

      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:orc::snappy>")
    else()
      message(STATUS "Using system snappy")
      list(APPEND ORC_SYSTEM_DEPENDENCIES Snappy)
      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:Snappy::snappy>")
    endif()

    add_library(orc_snappy INTERFACE IMPORTED)
    target_link_libraries(orc_snappy INTERFACE Snappy::snappy)
  endblock()
endif ()

add_library (orc::Snappy ALIAS orc_snappy)

# ----------------------------------------------------------------------
# ZLIB

if (ORC_PACKAGE_KIND STREQUAL "conan")
  find_package (ZLIB REQUIRED CONFIG)
  add_library (orc_zlib INTERFACE)
  target_link_libraries(orc_zlib INTERFACE ZLIB::ZLIB)
  list (APPEND ORC_SYSTEM_DEPENDENCIES ZLIB)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:ZLIB::ZLIB>")
elseif (ORC_PACKAGE_KIND STREQUAL "vcpkg")
  # For vcpkg, we need to use the module mode instead of config mode
  find_package(ZLIB REQUIRED)
  add_library (orc_zlib INTERFACE IMPORTED)
  target_link_libraries(orc_zlib INTERFACE ZLIB::ZLIB)
  list (APPEND ORC_SYSTEM_DEPENDENCIES ZLIB)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:ZLIB::ZLIB>")
elseif (NOT "${ZLIB_HOME}" STREQUAL "")
  find_package (ZLIBAlt REQUIRED)
  if (ORC_PREFER_STATIC_ZLIB AND ZLIB_STATIC_LIB)
    orc_add_resolved_library (orc_zlib ${ZLIB_STATIC_LIB} ${ZLIB_INCLUDE_DIR})
  else ()
    orc_add_resolved_library (orc_zlib ${ZLIB_LIBRARY} ${ZLIB_INCLUDE_DIR})
  endif ()
  list (APPEND ORC_SYSTEM_DEPENDENCIES ZLIBAlt)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:ZLIB::ZLIB>")
  orc_provide_find_module (ZLIBAlt)
else ()
  block(PROPAGATE ORC_SYSTEM_DEPENDENCIES ORC_INSTALL_INTERFACE_TARGETS ZLIB_LIBRARIES ZLIB_INCLUDE_DIRS)
    prepare_fetchcontent()

    set(ZLIB_BUILD_EXAMPLES OFF)
    set(ZLIB_BUILD_TESTING OFF)
    set(ZLIB_BUILD_STATIC ON)
    set(ZLIB_BUILD_SHARED OFF)
    set(ZLIB_INSTALL OFF)
    set(ZLIB_PREFIX OFF)

    fetchcontent_declare(zlib
      # URL "https://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz"
      # See https://github.com/madler/zlib/issues/937
      GIT_REPOSITORY "https://github.com/madler/zlib.git"
      GIT_TAG 5a82f71ed1dfc0bec044d9702463dbdf84ea3b71
      FIND_PACKAGE_ARGS
      NAMES ZLIB
      CONFIG
      )
    fetchcontent_makeavailable(zlib)

    if(zlib_SOURCE_DIR)
      message(STATUS "Using vendored zlib")
      if(NOT TARGET ZLIB::ZLIB)
        add_library(ZLIB::ZLIB ALIAS zlibstatic)
      endif()

      if(BUILD_POSITION_INDEPENDENT_LIB)
        set_target_properties(zlibstatic POSITION_INDEPENDENT_CODE ON)
      endif()

      if(INSTALL_VENDORED_LIBS)
        set_target_properties(zlibstatic PROPERTIES OUTPUT_NAME "orc_vendored_zlib")
        install(TARGETS zlibstatic
                EXPORT orc_targets
                RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
                ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
                LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
      endif()

      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:orc::ZLIBSTATIC>")
      set(ZLIB_LIBRARIES "zlibstatic")
      set(ZLIB_INCLUDE_DIRS "${zlib_SOURCE_DIR}")
    else()
      message(STATUS "Using system zlib")
      # FindZLIB guarantees that ZLIB::ZLIB target exists if found
      # See https://cmake.org/cmake/help/latest/module/FindZLIB.html#imported-targets
      if(NOT TARGET ZLIB::ZLIB)
        message(FATAL_ERROR "Using system zlib, but ZLIB::ZLIB not found")
      endif()
      list(APPEND ORC_SYSTEM_DEPENDENCIES ZLIB)
      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:ZLIB::ZLIB>")
    endif()

    add_library(orc_zlib INTERFACE IMPORTED)
    target_link_libraries(orc_zlib INTERFACE ZLIB::ZLIB)
  endblock()
endif()

add_library(orc::zlib ALIAS orc_zlib)

# ----------------------------------------------------------------------
# Zstd

if (ORC_PACKAGE_KIND STREQUAL "conan")
  find_package (ZSTD REQUIRED CONFIG)
  add_library (orc_zstd INTERFACE)
  target_link_libraries (orc_zstd INTERFACE
    $<TARGET_NAME_IF_EXISTS:zstd::libzstd_static>
    $<TARGET_NAME_IF_EXISTS:zstd::libzstd_shared>
  )
  list (APPEND ORC_SYSTEM_DEPENDENCIES ZSTD)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:$<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>>")
elseif (ORC_PACKAGE_KIND STREQUAL "vcpkg")
  find_package(zstd CONFIG REQUIRED)
  add_library(orc_zstd INTERFACE)
  target_link_libraries(orc_zstd INTERFACE $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>)
  list(APPEND ORC_SYSTEM_DEPENDENCIES zstd)
  list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:$<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>>")
elseif (NOT "${ZSTD_HOME}" STREQUAL "")
  find_package (ZSTDAlt REQUIRED)
  if (ORC_PREFER_STATIC_ZSTD AND ZSTD_STATIC_LIB)
    orc_add_resolved_library (orc_zstd ${ZSTD_STATIC_LIB} ${ZSTD_INCLUDE_DIR})
    list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:zstd::libzstd_static>")
  else ()
    orc_add_resolved_library (orc_zstd ${ZSTD_LIBRARY} ${ZSTD_INCLUDE_DIR})
    list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:$<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>>")
  endif ()
  list (APPEND ORC_SYSTEM_DEPENDENCIES ZSTDAlt)
  orc_provide_find_module (ZSTDAlt)
else ()
  block(PROPAGATE ORC_SYSTEM_DEPENDENCIES ORC_INSTALL_INTERFACE_TARGETS)
    prepare_fetchcontent()

    set(ZSTD_BUILD_TESTING OFF)
    set(ZSTD_BUILD_PROGRAMS OFF)
    set(ZSTD_BUILD_STATIC ON)
    set(ZSTD_BUILD_SHARED OFF)
    set(ZSTD_BUILD_CONTRIB OFF)

    fetchcontent_declare(zstd
      URL "https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz"
      SOURCE_SUBDIR "build/cmake"
      FIND_PACKAGE_ARGS
      NAMES zstd
      CONFIG
      )
    fetchcontent_makeavailable(zstd)

    if(zstd_SOURCE_DIR)
      message(STATUS "Using vendored zstd")
      if(NOT TARGET zstd::libzstd_static)
        add_library(zstd::libzstd_static ALIAS libzstd_static)
      endif()
      
      if(BUILD_POSITION_INDEPENDENT_LIB)
        set_target_properties(libzstd_static POSITION_INDEPENDENT_CODE ON)
      endif()

      if(INSTALL_VENDORED_LIBS)
        set_target_properties(libzstd_static PROPERTIES OUTPUT_NAME "orc_vendored_zstd")
        install(TARGETS libzstd_static
                EXPORT orc_targets
                RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
                ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
                LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
      endif()

      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:orc::libzstd_static>")
    else()
      message(STATUS "Using system zstd")
      list(APPEND ORC_SYSTEM_DEPENDENCIES zstd)
      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:$<TARGET_NAME_IF_EXISTS:zstd::libzstd_shared>>")
      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:$<TARGET_NAME_IF_EXISTS:zstd::libzstd_static>>")
    endif()

    add_library(orc_zstd INTERFACE IMPORTED)
    target_link_libraries(orc_zstd INTERFACE
      $<TARGET_NAME_IF_EXISTS:zstd::libzstd_static>
      $<TARGET_NAME_IF_EXISTS:zstd::libzstd_shared>
    )
  endblock()
endif ()

add_library (orc::zstd ALIAS orc_zstd)

# ----------------------------------------------------------------------
# LZ4
if (ORC_PACKAGE_KIND STREQUAL "conan")
  find_package (LZ4 REQUIRED CONFIG)
  add_library (orc_lz4 INTERFACE)
  target_link_libraries (orc_lz4 INTERFACE
    $<TARGET_NAME_IF_EXISTS:LZ4::lz4_shared>
    $<TARGET_NAME_IF_EXISTS:LZ4::lz4_static>
  )
  list (APPEND ORC_SYSTEM_DEPENDENCIES LZ4)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:$<IF:$<TARGET_EXISTS:LZ4::lz4_shared>,LZ4::lz4_shared,LZ4::lz4_static>>")
elseif (ORC_PACKAGE_KIND STREQUAL "vcpkg")
  find_package(lz4 CONFIG REQUIRED)
  add_library (orc_lz4 INTERFACE IMPORTED)
  target_link_libraries(orc_lz4 INTERFACE LZ4::lz4)
  list (APPEND ORC_SYSTEM_DEPENDENCIES lz4)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:LZ4::lz4>")
elseif (NOT "${LZ4_HOME}" STREQUAL "")
  find_package (LZ4Alt REQUIRED)
  if (ORC_PREFER_STATIC_LZ4 AND LZ4_STATIC_LIB)
    orc_add_resolved_library (orc_lz4 ${LZ4_STATIC_LIB} ${LZ4_INCLUDE_DIR})
  else ()
    orc_add_resolved_library (orc_lz4 ${LZ4_LIBRARY} ${LZ4_INCLUDE_DIR})
  endif ()
  list (APPEND ORC_SYSTEM_DEPENDENCIES LZ4Alt)
  list (APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:LZ4::lz4>")
  orc_provide_find_module (LZ4Alt)
else ()
  block(PROPAGATE ORC_SYSTEM_DEPENDENCIES ORC_INSTALL_INTERFACE_TARGETS)
    prepare_fetchcontent()

    set(LZ4_BUILD_CLI OFF)

    fetchcontent_declare(lz4
      URL "https://github.com/lz4/lz4/archive/v${LZ4_VERSION}.tar.gz"
      SOURCE_SUBDIR "build/cmake"
      FIND_PACKAGE_ARGS
      NAMES lz4
      CONFIG
      )
    fetchcontent_makeavailable(lz4)

    if(lz4_SOURCE_DIR)
      message(STATUS "Using vendored LZ4")
      if(NOT TARGET LZ4::lz4_static)
        add_library(LZ4::lz4_static ALIAS lz4_static)
      endif()

      if(BUILD_POSITION_INDEPENDENT_LIB)
        set_target_properties(lz4_static POSITION_INDEPENDENT_CODE ON)
      endif()

      if(INSTALL_VENDORED_LIBS)
        set_target_properties(lz4_static PROPERTIES OUTPUT_NAME "orc_vendored_lz4")
        install(TARGETS lz4_static
                EXPORT orc_targets
                RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
                ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
                LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
      endif()

      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:orc::lz4_static>")
    else()
      message(STATUS "Using system LZ4")
      list(APPEND ORC_SYSTEM_DEPENDENCIES lz4)
      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:$<TARGET_NAME_IF_EXISTS:LZ4::lz4_shared>>")
      list(APPEND ORC_INSTALL_INTERFACE_TARGETS "$<INSTALL_INTERFACE:$<TARGET_NAME_IF_EXISTS:LZ4::lz4_static>>")
    endif()

    add_library(orc_lz4 INTERFACE IMPORTED)
    target_link_libraries(orc_lz4 INTERFACE
      $<TARGET_NAME_IF_EXISTS:LZ4::lz4_shared>
      $<TARGET_NAME_IF_EXISTS:LZ4::lz4_static>
    )
  endblock()
endif ()

add_library (orc::lz4 ALIAS orc_lz4)

# ----------------------------------------------------------------------
# IANA - Time Zone Database

if (WIN32)
  SET(CURRENT_TZDATA_FILE "")
  SET(CURRENT_TZDATA_SHA512 "")
  File(DOWNLOAD "https://cygwin.osuosl.org/noarch/release/tzdata/tzdata-right/sha512.sum" ${CMAKE_CURRENT_BINARY_DIR}/sha512.sum)
  File(READ ${CMAKE_CURRENT_BINARY_DIR}/sha512.sum TZDATA_SHA512_CONTENT)
  string(REPLACE "\n" ";" TZDATA_SHA512_LINE ${TZDATA_SHA512_CONTENT})
  foreach (LINE IN LISTS TZDATA_SHA512_LINE)
      if (LINE MATCHES ".tar.xz$" AND (NOT LINE MATCHES "src.tar.xz$"))
          string(REPLACE " " ";" FILE_ARGS ${LINE})
          list (GET FILE_ARGS 0 FILE_SHA512)
          list (GET FILE_ARGS 2 FILE_NAME)
          if (FILE_NAME STRGREATER CURRENT_TZDATA_FILE)
              SET(CURRENT_TZDATA_FILE ${FILE_NAME})
              SET(CURRENT_TZDATA_SHA512 ${FILE_SHA512})
          endif()
      endif()
  endforeach()

  if (NOT "${CURRENT_TZDATA_FILE}" STREQUAL "")
    ExternalProject_Add(tzdata_ep
      URL "https://cygwin.osuosl.org/noarch/release/tzdata/tzdata-right/${CURRENT_TZDATA_FILE}"
      URL_HASH SHA512=${CURRENT_TZDATA_SHA512}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND "")
    ExternalProject_Get_Property(tzdata_ep SOURCE_DIR)
    set(TZDATA_DIR ${SOURCE_DIR}/share/zoneinfo/right)
  else()
    message(STATUS "WARNING: tzdata were not found")
  endif()
endif ()

# ----------------------------------------------------------------------
# GoogleTest (gtest now includes gmock)

if (BUILD_CPP_TESTS)
  add_library (orc_gmock INTERFACE)
  add_library (orc_gtest INTERFACE)
  add_library (orc::gmock ALIAS orc_gmock)
  add_library (orc::gtest ALIAS orc_gtest)

  if (NOT "${GTEST_HOME}" STREQUAL "")
    find_package (GTestAlt REQUIRED)

    # This is a bit special cased because gmock requires gtest and some
    # distributions statically link gtest inside the gmock shared lib
    if (ORC_PREFER_STATIC_GMOCK AND GMOCK_STATIC_LIB)
      target_link_libraries (orc_gmock INTERFACE ${GMOCK_STATIC_LIB})
      target_link_libraries (orc_gtest INTERFACE ${GTEST_STATIC_LIB})
    else ()
      target_link_libraries (orc_gmock INTERFACE ${GMOCK_LIBRARY})
      target_link_libraries (orc_gtest INTERFACE ${GTEST_LIBRARY})
    endif ()

    target_include_directories (orc_gmock SYSTEM INTERFACE ${GTEST_INCLUDE_DIR})
    target_include_directories (orc_gtest SYSTEM INTERFACE ${GTEST_INCLUDE_DIR})

    if (NOT APPLE AND NOT MSVC)
      target_link_libraries (orc_gmock INTERFACE Threads::Threads)
      target_link_libraries (orc_gtest INTERFACE Threads::Threads)
    endif ()
  else ()
    block()
      prepare_fetchcontent()
      fetchcontent_declare(googletest
        URL "https://github.com/google/googletest/archive/refs/tags/v${GTEST_VERSION}.tar.gz"
        FIND_PACKAGE_ARGS
        NAMES GTest
        CONFIG
        )
      fetchcontent_makeavailable(googletest)

      if(googletest_SOURCE_DIR)
        message(STATUS "Using vendored GTest")
        if(NOT TARGET GTest::gtest)
          add_library(GTest::gtest ALIAS gtest)
        endif()
        if(NOT TARGET GTest::gmock)
          add_library(GTest::gmock ALIAS gmock)
        endif()
        if(BUILD_POSITION_INDEPENDENT_LIB)
          set_target_properties(gtest PROPERTIES POSITION_INDEPENDENT_CODE ON)
          set_target_properties(gmock PROPERTIES POSITION_INDEPENDENT_CODE ON)
        endif()
      else()
        message(STATUS "Using system GTest")
      endif()

      target_link_libraries (orc_gmock INTERFACE GTest::gmock)
      target_link_libraries (orc_gtest INTERFACE GTest::gtest)
    endblock()
  endif ()
endif ()

# ----------------------------------------------------------------------
# SPARSEHASH
if(BUILD_SPARSEHASH)
  block()
    prepare_fetchcontent()

    fetchcontent_declare(sparsehash
      URL "https://github.com/sparsehash/sparsehash-c11/archive/refs/tags/v${SPARSEHASH_VERSION}.tar.gz"
      SOURCE_SUBDIR "sparsehash" # XXX: sparsehash bundles gtest which conflicts with our vendored one
      )
    fetchcontent_makeavailable(sparsehash)
    message(STATUS "Using vendored sparsehash")

    # sparsehash-c11 is header-only
    add_library(orc_sparsehash INTERFACE)
    add_library(orc::sparsehash ALIAS orc_sparsehash)
    target_include_directories(orc_sparsehash INTERFACE $<BUILD_INTERFACE:${sparsehash_SOURCE_DIR}>)
  endblock()
endif()

# ----------------------------------------------------------------------
# LIBHDFSPP
if(BUILD_LIBHDFSPP)
  set (BUILD_LIBHDFSPP FALSE)
  if(ORC_CXX_HAS_THREAD_LOCAL)
    find_package(CyrusSASL)
    find_package(OpenSSL)
    find_package(Threads)
    if (CYRUS_SASL_SHARED_LIB AND OPENSSL_LIBRARIES)
      set (BUILD_LIBHDFSPP TRUE)
      set (LIBHDFSPP_PREFIX "${THIRDPARTY_DIR}/libhdfspp_ep-install")
      set (LIBHDFSPP_INCLUDE_DIR "${LIBHDFSPP_PREFIX}/include")
      set (LIBHDFSPP_STATIC_LIB_NAME hdfspp_static)
      set (LIBHDFSPP_STATIC_LIB "${LIBHDFSPP_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${LIBHDFSPP_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
      set (LIBHDFSPP_SRC_URL "${PROJECT_SOURCE_DIR}/c++/libs/libhdfspp/libhdfspp.tar.gz")
      set (LIBHDFSPP_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                                -DCMAKE_INSTALL_PREFIX=${LIBHDFSPP_PREFIX}
                                -DPROTOBUF_INCLUDE_DIR=${PROTOBUF_INCLUDE_DIR}
                                -DPROTOBUF_LIBRARY=${PROTOBUF_STATIC_LIB}
                                -DPROTOBUF_PROTOC_LIBRARY=${PROTOC_STATIC_LIB}
                                -DPROTOBUF_PROTOC_EXECUTABLE=${PROTOBUF_EXECUTABLE}
                                -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}
                                -DCMAKE_C_FLAGS=${EP_C_FLAGS}
                                -DBUILD_SHARED_LIBS=OFF
                                -DHDFSPP_LIBRARY_ONLY=TRUE
                                -DBUILD_SHARED_HDFSPP=FALSE)

      if (BUILD_POSITION_INDEPENDENT_LIB)
        set(LIBHDFSPP_CMAKE_ARGS ${LIBHDFSPP_CMAKE_ARGS} -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
      endif ()

      ExternalProject_Add (libhdfspp_ep
        DEPENDS orc::protobuf
        URL ${LIBHDFSPP_SRC_URL}
        LOG_DOWNLOAD 0
        LOG_CONFIGURE 0
        LOG_BUILD 0
        LOG_INSTALL 0
        BUILD_BYPRODUCTS "${LIBHDFSPP_STATIC_LIB}"
        CMAKE_ARGS ${LIBHDFSPP_CMAKE_ARGS})

      orc_add_built_library(libhdfspp_ep libhdfspp ${LIBHDFSPP_STATIC_LIB} ${LIBHDFSPP_INCLUDE_DIR})

      set (LIBHDFSPP_LIBRARIES
           libhdfspp
           ${CYRUS_SASL_SHARED_LIB}
           ${OPENSSL_LIBRARIES}
           ${CMAKE_THREAD_LIBS_INIT})

    elseif(CYRUS_SASL_SHARED_LIB)
      message(STATUS
      "WARNING: Libhdfs++ library was not built because the required OpenSSL library was not found")
    elseif(OPENSSL_LIBRARIES)
      message(STATUS
      "WARNING: Libhdfs++ library was not built because the required CyrusSASL library was not found")
    else ()
      message(STATUS
      "WARNING: Libhdfs++ library was not built because the required CyrusSASL and OpenSSL libraries were not found")
    endif(CYRUS_SASL_SHARED_LIB AND OPENSSL_LIBRARIES)
  else(ORC_CXX_HAS_THREAD_LOCAL)
    message(STATUS
    "WARNING: Libhdfs++ library was not built because the required feature
    thread_local storage is not supported by your compiler. Known compilers that
    support this feature: GCC, Visual Studio, Clang (community version),
    Clang (version for iOS 9 and later), Clang (version for Xcode 8 and later)")
  endif(ORC_CXX_HAS_THREAD_LOCAL)
endif(BUILD_LIBHDFSPP)
