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

set (LZ4_VERSION "1.7.5")
set (SNAPPY_VERSION "1.1.4")
set (ZLIB_VERSION "1.2.11")
set (GTEST_VERSION "1.8.0")
set (PROTOBUF_VERSION "2.6.0")

set (THIRDPARTY_DIR "${CMAKE_BINARY_DIR}/c++/libs/thirdparty")

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

if (DEFINED ENV{GTEST_HOME})
  set (GTEST_HOME "$ENV{GTEST_HOME}")
endif ()

# ----------------------------------------------------------------------
# Snappy

if (NOT "${SNAPPY_HOME}" STREQUAL "")
  find_package (Snappy REQUIRED)
  set(SNAPPY_VENDORED FALSE)
else ()
  set (SNAPPY_PREFIX "${THIRDPARTY_DIR}/snappy_ep-install")
  set (SNAPPY_HOME "${SNAPPY_PREFIX}")
  set (SNAPPY_INCLUDE_DIR "${SNAPPY_PREFIX}/include")
  set (SNAPPY_STATIC_LIB_NAME snappy)
  set (SNAPPY_STATIC_LIB "${SNAPPY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set (SNAPPY_SRC_URL "https://github.com/google/snappy/releases/download/${SNAPPY_VERSION}/snappy-${SNAPPY_VERSION}.tar.gz")
  if (${UPPERCASE_BUILD_TYPE} EQUAL "RELEASE")
     set (SNAPPY_CXXFLAGS "CXXFLAGS='-DNDEBUG -O2'")
  endif ()

  ExternalProject_Add (snappy_ep
    CONFIGURE_COMMAND "./configure" "--disable-shared" "--prefix=${SNAPPY_PREFIX}" ${SNAPPY_CXXFLAGS}
    BUILD_COMMAND ${MAKE}
    BUILD_IN_SOURCE 1
    INSTALL_DIR ${SNAPPY_PREFIX}
    URL ${SNAPPY_SRC_URL}
    LOG_DOWNLOAD 1
    LOG_CONFIGURE 1
    LOG_BUILD 1
    LOG_INSTALL 1
    BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")

  set (SNAPPY_VENDORED TRUE)
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
  set (ZLIB_VENDORED FALSE)
else ()
  set (ZLIB_PREFIX "${THIRDPARTY_DIR}/zlib_ep-install")
  set (ZLIB_HOME "${ZLIB_PREFIX}")
  set (ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
  set (ZLIB_STATIC_LIB_NAME z)
  set (ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ZLIB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set (ZLIB_SRC_URL "http://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz")
  set (ZLIB_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                       -DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}
                       -DCMAKE_C_FLAGS=${EP_C_FLAGS}
                       -DBUILD_SHARED_LIBS=OFF)

  ExternalProject_Add (zlib_ep
    URL ${ZLIB_SRC_URL}
    LOG_DOWNLOAD 1
    LOG_CONFIGURE 1
    LOG_BUILD 1
    LOG_INSTALL 1
    BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
    CMAKE_ARGS ${ZLIB_CMAKE_ARGS})

  set (ZLIB_VENDORED TRUE)
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
# LZ4

if (NOT "${LZ4_HOME}" STREQUAL "")
  find_package (LZ4 REQUIRED)
  set (LZ4_VENDORED FALSE)
else ()
  set (LZ4_PREFIX "${THIRDPARTY_DIR}/lz4_ep-install")
  set (LZ4_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/src/lz4_ep")
  set (LZ4_INCLUDE_DIR "${LZ4_PREFIX}/include")
  set (LZ4_STATIC_LIB_NAME lz4)
  set (LZ4_STATIC_LIB "${LZ4_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${LZ4_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set (LZ4_SRC_URL "https://github.com/lz4/lz4/archive/v${LZ4_VERSION}.tar.gz")

  ExternalProject_Add(lz4_ep
    CONFIGURE_COMMAND ""
    INSTALL_COMMAND make "PREFIX=${LZ4_PREFIX}" install
    BUILD_IN_SOURCE 1
    BUILD_COMMAND ${MAKE}
    URL ${LZ4_SRC_URL}
    LOG_DOWNLOAD 1
    LOG_BUILD 1
    LOG_INSTALL 1
    BUILD_BYPRODUCTS ${LZ4_STATIC_LIB}
    )

  set (LZ4_VENDORED TRUE)
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
# GoogleTest (gtest now includes gmock)

if (BUILD_CPP_TESTS)
  if (NOT "${GTEST_HOME}" STREQUAL "")
    find_package (GTest REQUIRED)
    set (GTEST_VENDORED FALSE)
  else ()
    set (GTEST_PREFIX "${THIRDPARTY_DIR}/googletest_ep-install")
    set (GTEST_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
    set (GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
    set (GMOCK_STATIC_LIB "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set (GTEST_SRC_URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz")
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
      LOG_DOWNLOAD 1
      LOG_CONFIGURE 1
      LOG_BUILD 1
      LOG_INSTALL 1
      BUILD_BYPRODUCTS "${GMOCK_STATIC_LIB}"
      CMAKE_ARGS ${GTEST_CMAKE_ARGS}
      )

    set (GTEST_VENDORED TRUE)
  endif ()

  include_directories (SYSTEM ${GTEST_INCLUDE_DIR})

  add_library (gmock STATIC IMPORTED)
  set_target_properties (gmock PROPERTIES IMPORTED_LOCATION ${GMOCK_STATIC_LIB})

  if (GTEST_VENDORED)
    add_dependencies (gmock googletest_ep)
  endif ()

  set (GTEST_LIBRARIES gmock)
  if (NOT APPLE AND NOT MSVC)
    list (APPEND GTEST_LIBRARIES pthread)
  endif ()
endif ()

# ----------------------------------------------------------------------
# Protobuf

if (NOT "${PROTOBUF_HOME}" STREQUAL "")
  find_package (Protobuf REQUIRED)
  set (PROTOBUF_VENDORED FALSE)
else ()
  set (PROTOBUF_PREFIX "${THIRDPARTY_DIR}/protobuf_ep-install")
  set (PROTOBUF_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-prefix/src/protobuf_ep")
  set (PROTOBUF_INCLUDE_DIR "${PROTOBUF_PREFIX}/include")
  set (PROTOBUF_STATIC_LIB "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protobuf${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set (PROTOC_STATIC_LIB "${PROTOBUF_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}protoc${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set (PROTOBUF_EXECUTABLE "${PROTOBUF_PREFIX}/bin/protoc")
  set (PROTOBUF_SRC_URL "https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-${PROTOBUF_VERSION}.tar.gz")

  ExternalProject_Add(protobuf_ep
    CONFIGURE_COMMAND "./configure" "--disable-shared" "--prefix=${PROTOBUF_PREFIX}"
    BUILD_IN_SOURCE 1
    URL ${PROTOBUF_SRC_URL}
    LOG_DOWNLOAD 1
    LOG_CONFIGURE 1
    LOG_BUILD 1
    LOG_INSTALL 1
    BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}" "${PROTOC_STATIC_LIB}"
    )

  set (PROTOBUF_VENDORED TRUE)
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
  if(ORC_CXX_HAS_THREAD_LOCAL)
    find_package(CyrusSASL)
    find_package(OpenSSL)
    find_package(Threads)

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

  else(ORC_CXX_HAS_THREAD_LOCAL)
    message(STATUS
    "WARNING: Libhdfs++ library was not built because the required feature
    thread_local storage is not supported by your compiler. Known compilers that
    support this feature: GCC, Visual Studio, Clang (community version),
    Clang (version for iOS 9 and later), Clang (version for Xcode 8 and later)")
  endif(ORC_CXX_HAS_THREAD_LOCAL)
endif(BUILD_LIBHDFSPP)
