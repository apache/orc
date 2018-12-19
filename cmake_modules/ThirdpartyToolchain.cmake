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

set(LZ4_VERSION "1.7.5")
set(SNAPPY_VERSION "1.1.7")
set(ZLIB_VERSION "1.2.11")
set(GTEST_VERSION "1.8.0")
set(PROTOBUF_VERSION "3.5.1")
set(ZSTD_VERSION "1.3.5")

# zstd requires us to add the threads
FIND_PACKAGE(Threads REQUIRED)

set(THIRDPARTY_DIR "${CMAKE_BINARY_DIR}/c++/libs/thirdparty")
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
# Snappy

if (NOT "${SNAPPY_HOME}" STREQUAL "")
  find_package (Snappy REQUIRED)
  set(SNAPPY_VENDORED FALSE)
else ()
  set(SNAPPY_HOME "${THIRDPARTY_DIR}/snappy_ep-install")
  set(SNAPPY_INCLUDE_DIR "${SNAPPY_HOME}/include")
  set(SNAPPY_STATIC_LIB "${SNAPPY_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(SNAPPY_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${SNAPPY_HOME}
                        -DBUILD_SHARED_LIBS=OFF)

  ExternalProject_Add (snappy_ep
    URL "https://github.com/google/snappy/archive/${SNAPPY_VERSION}.tar.gz"
    CMAKE_ARGS ${SNAPPY_CMAKE_ARGS}
    ${THIRDPARTY_LOG_OPTIONS}
    BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")

  set(SNAPPY_VENDORED TRUE)
endif ()

include_directories (SYSTEM ${SNAPPY_INCLUDE_DIR})
add_library (snappy STATIC IMPORTED)
set_target_properties (snappy PROPERTIES IMPORTED_LOCATION ${SNAPPY_STATIC_LIB})

if (SNAPPY_VENDORED)
  add_dependencies (snappy snappy_ep)
  if (INSTALL_VENDORED_LIBS)
    install(FILES "${SNAPPY_STATIC_LIB}"
            DESTINATION "lib")
  endif ()
endif ()

# ----------------------------------------------------------------------
# ZLIB

if (NOT "${ZLIB_HOME}" STREQUAL "")
  find_package (ZLIB REQUIRED)
  set(ZLIB_VENDORED FALSE)
else ()
  set(ZLIB_PREFIX "${THIRDPARTY_DIR}/zlib_ep-install")
  set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
  if (MSVC)
    set(ZLIB_STATIC_LIB_NAME zlibstatic)
    if (${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
      set(ZLIB_STATIC_LIB_NAME ${ZLIB_STATIC_LIB_NAME}d)
    endif ()
  else ()
    set(ZLIB_STATIC_LIB_NAME z)
  endif ()
  set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ZLIB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(ZLIB_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}
                      -DBUILD_SHARED_LIBS=OFF)

  ExternalProject_Add (zlib_ep
    URL "http://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz"
    CMAKE_ARGS ${ZLIB_CMAKE_ARGS}
    ${THIRDPARTY_LOG_OPTIONS}
    BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}")

  set(ZLIB_VENDORED TRUE)
endif ()

include_directories (SYSTEM ${ZLIB_INCLUDE_DIR})
add_library (zlib STATIC IMPORTED)
set_target_properties (zlib PROPERTIES IMPORTED_LOCATION ${ZLIB_STATIC_LIB})

if (ZLIB_VENDORED)
  add_dependencies (zlib zlib_ep)
  if (INSTALL_VENDORED_LIBS)
    install(FILES "${ZLIB_STATIC_LIB}"
            DESTINATION "lib")
  endif ()
endif ()

# ----------------------------------------------------------------------
# Zstd

if (NOT "${ZSTD_HOME}" STREQUAL "")
  find_package (zstd REQUIRED)
  set(ZSTD_VENDORED FALSE)
else ()
  set(ZSTD_HOME "${THIRDPARTY_DIR}/zstd_ep-install")
  set(ZSTD_INCLUDE_DIR "${ZSTD_HOME}/include")
  if (MSVC)
    set(ZSTD_STATIC_LIB_NAME zstd_static)
    if (${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
      set(ZSTD_STATIC_LIB_NAME ${ZSTD_STATIC_LIB_NAME})
    endif ()
  else ()
    set(ZSTD_STATIC_LIB_NAME zstd)
  endif ()
  set(ZSTD_STATIC_LIB "${ZSTD_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ZSTD_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(ZSTD_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${ZSTD_HOME}
          -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_LIBDIR=lib)

  if (CMAKE_VERSION VERSION_GREATER "3.7")
    set(ZSTD_CONFIGURE SOURCE_SUBDIR "build/cmake" CMAKE_ARGS ${ZSTD_CMAKE_ARGS})
  else()
    set(ZSTD_CONFIGURE CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}" ${ZSTD_CMAKE_ARGS}
            "${CMAKE_CURRENT_BINARY_DIR}/zstd_ep-prefix/src/zstd_ep/build/cmake")
  endif()

  ExternalProject_Add(zstd_ep
          URL "https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz"
          ${ZSTD_CONFIGURE}
          ${THIRDPARTY_LOG_OPTIONS}
          BUILD_BYPRODUCTS ${ZSTD_STATIC_LIB})

  set(ZSTD_VENDORED TRUE)
endif ()

include_directories (SYSTEM ${ZSTD_INCLUDE_DIR})
add_library (zstd STATIC IMPORTED)
set_target_properties (zstd PROPERTIES IMPORTED_LOCATION ${ZSTD_STATIC_LIB})

if (ZSTD_VENDORED)
  add_dependencies (zstd zstd_ep)
  if (INSTALL_VENDORED_LIBS)
    install(FILES "${ZSTD_STATIC_LIB}"
            DESTINATION "lib")
  endif ()
endif ()

# ----------------------------------------------------------------------
# LZ4

if (NOT "${LZ4_HOME}" STREQUAL "")
  find_package (LZ4 REQUIRED)
  set(LZ4_VENDORED FALSE)
else ()
  set(LZ4_PREFIX "${THIRDPARTY_DIR}/lz4_ep-install")
  set(LZ4_INCLUDE_DIR "${LZ4_PREFIX}/include")
  set(LZ4_STATIC_LIB "${LZ4_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}lz4${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(LZ4_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${LZ4_PREFIX}
                     -DCMAKE_INSTALL_LIBDIR=lib
                     -DBUILD_SHARED_LIBS=OFF)

  if (CMAKE_VERSION VERSION_GREATER "3.7")
    set(LZ4_CONFIGURE SOURCE_SUBDIR "contrib/cmake_unofficial" CMAKE_ARGS ${LZ4_CMAKE_ARGS})
  else()
    set(LZ4_CONFIGURE CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}" ${LZ4_CMAKE_ARGS}
                                        "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/src/lz4_ep/contrib/cmake_unofficial")
  endif()

  ExternalProject_Add(lz4_ep
    URL "https://github.com/lz4/lz4/archive/v${LZ4_VERSION}.tar.gz"
    ${LZ4_CONFIGURE}
    ${THIRDPARTY_LOG_OPTIONS}
    BUILD_BYPRODUCTS ${LZ4_STATIC_LIB})

  set(LZ4_VENDORED TRUE)
endif ()

include_directories (SYSTEM ${LZ4_INCLUDE_DIR})
add_library (lz4 STATIC IMPORTED)
set_target_properties (lz4 PROPERTIES IMPORTED_LOCATION ${LZ4_STATIC_LIB})

if (LZ4_VENDORED)
  add_dependencies (lz4 lz4_ep)
  if (INSTALL_VENDORED_LIBS)
    install(FILES "${LZ4_STATIC_LIB}"
            DESTINATION "lib")
  endif ()
endif ()

# ----------------------------------------------------------------------
# IANA - Time Zone Database

if (WIN32)
  ExternalProject_Add(tzdata_ep
    URL "ftp://cygwin.osuosl.org/pub/cygwin/noarch/release/tzdata/tzdata-2018c-1.tar.xz"
    URL_HASH MD5=F69FCA5C906FAFF02462D3D06F28267C
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND "")
  ExternalProject_Get_Property(tzdata_ep SOURCE_DIR)
  set(TZDATA_DIR ${SOURCE_DIR}/share/zoneinfo)
endif ()

# ----------------------------------------------------------------------
# GoogleTest (gtest now includes gmock)

if (BUILD_CPP_TESTS)
  if (NOT "${GTEST_HOME}" STREQUAL "")
    find_package (GTest REQUIRED)
    set (GTEST_VENDORED FALSE)
  else ()
    set(GTEST_PREFIX "${THIRDPARTY_DIR}/googletest_ep-install")
    set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
    set(GMOCK_STATIC_LIB "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GTEST_SRC_URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz")
    if(APPLE)
      set(GTEST_CMAKE_CXX_FLAGS " -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
    else()
      set(GTEST_CMAKE_CXX_FLAGS "")
    endif()

    set(GTEST_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                         -DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}
                         -Dgtest_force_shared_crt=ON
                         -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})

    ExternalProject_Add(googletest_ep
      BUILD_IN_SOURCE 1
      URL ${GTEST_SRC_URL}
      ${THIRDPARTY_LOG_OPTIONS}
      CMAKE_ARGS ${GTEST_CMAKE_ARGS}
      BUILD_BYPRODUCTS "${GMOCK_STATIC_LIB}")

    set(GTEST_VENDORED TRUE)
  endif ()

  include_directories (SYSTEM ${GTEST_INCLUDE_DIR})

  add_library (gmock STATIC IMPORTED)
  set_target_properties (gmock PROPERTIES IMPORTED_LOCATION ${GMOCK_STATIC_LIB})

  if (GTEST_VENDORED)
    add_dependencies (gmock googletest_ep)
  endif ()

  set(GTEST_LIBRARIES gmock)
  if (NOT APPLE AND NOT MSVC)
    list (APPEND GTEST_LIBRARIES pthread)
  endif ()
endif ()

# ----------------------------------------------------------------------
# Protobuf

if (NOT "${PROTOBUF_HOME}" STREQUAL "")
  find_package (Protobuf REQUIRED)
  set(PROTOBUF_VENDORED FALSE)
else ()
  set(PROTOBUF_PREFIX "${THIRDPARTY_DIR}/protobuf_ep-install")
  set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
  set(PROTOBUF_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${PROTOBUF_PREFIX}
                          -DCMAKE_INSTALL_LIBDIR=lib
                          -DBUILD_SHARED_LIBS=OFF
                          -Dprotobuf_BUILD_TESTS=OFF)
  if (MSVC)
    set(PROTOBUF_STATIC_LIB_PREFIX lib)
    list(APPEND PROTOBUF_CMAKE_ARGS -Dprotobuf_MSVC_STATIC_RUNTIME=OFF
                                    -Dprotobuf_DEBUG_POSTFIX=)
  else ()
    set(PROTOBUF_STATIC_LIB_PREFIX ${CMAKE_STATIC_LIBRARY_PREFIX})
  endif ()
  set(PROTOBUF_STATIC_LIB "${PROTOBUF_PREFIX}/lib/${PROTOBUF_STATIC_LIB_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(PROTOC_STATIC_LIB "${PROTOBUF_PREFIX}/lib/${PROTOBUF_STATIC_LIB_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(PROTOBUF_EXECUTABLE "${PROTOBUF_PREFIX}/bin/protoc${CMAKE_EXECUTABLE_SUFFIX}")

  if (CMAKE_VERSION VERSION_GREATER "3.7")
    set(PROTOBUF_CONFIGURE SOURCE_SUBDIR "cmake" CMAKE_ARGS ${PROTOBUF_CMAKE_ARGS})
  else()
    set(PROTOBUF_CONFIGURE CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}" ${PROTOBUF_CMAKE_ARGS}
                                             "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-prefix/src/protobuf_ep/cmake")
  endif()

  ExternalProject_Add(protobuf_ep
    URL "https://github.com/google/protobuf/archive/v${PROTOBUF_VERSION}.tar.gz"
    ${PROTOBUF_CONFIGURE}
    ${THIRDPARTY_LOG_OPTIONS}
    BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}" "${PROTOC_STATIC_LIB}")

  set(PROTOBUF_VENDORED TRUE)
endif ()

include_directories (SYSTEM ${PROTOBUF_INCLUDE_DIR})

add_library (protobuf STATIC IMPORTED)
set_target_properties (protobuf PROPERTIES IMPORTED_LOCATION ${PROTOBUF_STATIC_LIB})

add_library (protoc STATIC IMPORTED)
set_target_properties (protoc PROPERTIES IMPORTED_LOCATION ${PROTOC_STATIC_LIB})

if (PROTOBUF_VENDORED)
  add_dependencies (protoc protobuf_ep)
  add_dependencies (protobuf protobuf_ep)
  if (INSTALL_VENDORED_LIBS)
    install(FILES "${PROTOBUF_STATIC_LIB}" "${PROTOC_STATIC_LIB}"
            DESTINATION "lib")
  endif ()
endif ()

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
      set (LIBHDFSPP_SRC_URL "${CMAKE_SOURCE_DIR}/c++/libs/libhdfspp/libhdfspp.tar.gz")
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

      ExternalProject_Add (libhdfspp_ep
        DEPENDS protobuf
        URL ${LIBHDFSPP_SRC_URL}
        LOG_DOWNLOAD 0
        LOG_CONFIGURE 0
        LOG_BUILD 0
        LOG_INSTALL 0
        BUILD_BYPRODUCTS "${LIBHDFSPP_STATIC_LIB}"
        CMAKE_ARGS ${LIBHDFSPP_CMAKE_ARGS})

      include_directories (SYSTEM ${LIBHDFSPP_INCLUDE_DIR})

      add_library (libhdfspp STATIC IMPORTED)
      set_target_properties (libhdfspp PROPERTIES IMPORTED_LOCATION ${LIBHDFSPP_STATIC_LIB})
      add_dependencies (libhdfspp libhdfspp_ep)
      if (INSTALL_VENDORED_LIBS)
        install(FILES "${LIBHDFSPP_STATIC_LIB}"
                DESTINATION "lib")
      endif ()

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
