set (THIRDPARTY_DIR "${CMAKE_BINARY_DIR}/c++/libs/thirdparty")

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

# ----------------------------------------------------------------------
# Snappy

set (SNAPPY_PREFIX "${THIRDPARTY_DIR}/snappy_ep-install")
set (SNAPPY_HOME "${SNAPPY_PREFIX}")
set (SNAPPY_INCLUDE_DIRS "${SNAPPY_PREFIX}/include")
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
include_directories (SYSTEM ${SNAPPY_INCLUDE_DIRS})
add_library (snappy STATIC IMPORTED)
set_target_properties (snappy PROPERTIES IMPORTED_LOCATION ${SNAPPY_STATIC_LIB})
set (SNAPPY_LIBRARIES snappy)
add_dependencies (snappy snappy_ep)
install(DIRECTORY ${SNAPPY_PREFIX}/lib DESTINATION .
                                       PATTERN "pkgconfig" EXCLUDE
                                       PATTERN "*.so*" EXCLUDE
                                       PATTERN "*.dylib" EXCLUDE)

# ----------------------------------------------------------------------
# ZLIB

set (ZLIB_PREFIX "${THIRDPARTY_DIR}/zlib_ep-install")
set (ZLIB_HOME "${ZLIB_PREFIX}")
set (ZLIB_INCLUDE_DIRS "${ZLIB_PREFIX}/include")
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

include_directories (SYSTEM ${ZLIB_INCLUDE_DIRS})
add_library (zlib STATIC IMPORTED)
set_target_properties (zlib PROPERTIES IMPORTED_LOCATION ${ZLIB_STATIC_LIB})
set (ZLIB_LIBRARIES zlib)
add_dependencies (zlib zlib_ep)
install(DIRECTORY ${ZLIB_PREFIX}/lib DESTINATION .
                                     PATTERN "pkgconfig" EXCLUDE
                                     PATTERN "*.so*" EXCLUDE
                                     PATTERN "*.dylib" EXCLUDE)

# ----------------------------------------------------------------------
# LZ4

set (LZ4_PREFIX "${THIRDPARTY_DIR}/lz4_ep-install")
set (LZ4_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/src/lz4_ep")
set (LZ4_INCLUDE_DIRS "${LZ4_PREFIX}/include")
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
include_directories (SYSTEM ${LZ4_INCLUDE_DIRS})
add_library (lz4 STATIC IMPORTED)
set_target_properties (lz4 PROPERTIES IMPORTED_LOCATION ${LZ4_STATIC_LIB})
set (LZ4_LIBRARIES lz4)
add_dependencies (lz4 lz4_ep)
install(DIRECTORY ${LZ4_PREFIX}/lib DESTINATION .
                                    PATTERN "pkgconfig" EXCLUDE
                                    PATTERN "*.so*" EXCLUDE
                                    PATTERN "*.dylib" EXCLUDE)

# ----------------------------------------------------------------------
# GoogleTest (gtest now includes gmock)

set (GTEST_PREFIX "${THIRDPARTY_DIR}/googletest_ep-install")
set (GTEST_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
set (GTEST_INCLUDE_DIRS "${GTEST_PREFIX}/include")
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

include_directories (SYSTEM ${GTEST_INCLUDE_DIRS})

add_library (gmock STATIC IMPORTED)
set_target_properties (gmock PROPERTIES IMPORTED_LOCATION ${GMOCK_STATIC_LIB})
add_dependencies (gmock googletest_ep)

set (GTEST_LIBRARIES gmock)
if(NOT APPLE AND NOT MSVC)
  list (APPEND GTEST_LIBRARIES pthread)
endif(NOT APPLE AND NOT MSVC)

# ----------------------------------------------------------------------
# Protobuf

set (PROTOBUF_PREFIX "${THIRDPARTY_DIR}/protobuf_ep-install")
set (PROTOBUF_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf_ep-prefix/src/protobuf_ep")
set (PROTOBUF_INCLUDE_DIRS "${PROTOBUF_PREFIX}/include")
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
  BUILD_BYPRODUCTS "${PROTOBUF_STATIC_LIB}"
  )

include_directories (SYSTEM ${PROTOBUF_INCLUDE_DIRS})

add_library (protobuf STATIC IMPORTED)
set_target_properties (protobuf PROPERTIES IMPORTED_LOCATION ${PROTOBUF_STATIC_LIB})
add_dependencies (protobuf protobuf_ep)
set (PROTOBUF_LIBRARIES protobuf)

add_library (protoc STATIC IMPORTED)
set_target_properties (protoc PROPERTIES IMPORTED_LOCATION ${PROTOC_STATIC_LIB})
add_dependencies (protoc protobuf_ep)

install(DIRECTORY ${PROTOBUF_PREFIX}/lib DESTINATION .
                                         PATTERN "pkgconfig" EXCLUDE
                                         PATTERN "*.so*" EXCLUDE
                                         PATTERN "*.dylib" EXCLUDE)

# ----------------------------------------------------------------------
# LIBHDFSPP

if(BUILD_LIBHDFSPP)
  if(ORC_CXX_HAS_THREAD_LOCAL)
    find_package(CyrusSASL)
    find_package(OpenSSL)
    find_package(Threads)

    set (LIBHDFSPP_PREFIX "${THIRDPARTY_DIR}/libhdfspp_ep-install")
    set (LIBHDFSPP_INCLUDE_DIRS "${LIBHDFSPP_PREFIX}/include")
    set (LIBHDFSPP_STATIC_LIB_NAME hdfspp_static)
    set (LIBHDFSPP_STATIC_LIB "${LIBHDFSPP_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${LIBHDFSPP_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set (LIBHDFSPP_SRC_URL "${CMAKE_SOURCE_DIR}/c++/libs/libhdfspp/libhdfspp.tar.gz")
    set (LIBHDFSPP_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                          -DCMAKE_INSTALL_PREFIX=${LIBHDFSPP_PREFIX}
                          -DPROTOBUF_INCLUDE_DIR=${PROTOBUF_INCLUDE_DIRS}
                          -DPROTOBUF_LIBRARY=${PROTOBUF_STATIC_LIB}
                          -DPROTOBUF_PROTOC_LIBRARY=${PROTOC_STATIC_LIB}
                          -DPROTOBUF_PROTOC_EXECUTABLE=${PROTOBUF_EXECUTABLE}
                          -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}
                          -DCMAKE_C_FLAGS=${EP_C_FLAGS}
                          -DBUILD_SHARED_LIBS=OFF
                          -DHDFSPP_LIBRARY_ONLY=TRUE
                          -DBUILD_SHARED_HDFSPP=FALSE)

    ExternalProject_Add (libhdfspp_ep
      DEPENDS protobuf_ep
      URL ${LIBHDFSPP_SRC_URL}
      LOG_DOWNLOAD 0
      LOG_CONFIGURE 0
      LOG_BUILD 0
      LOG_INSTALL 0
      BUILD_BYPRODUCTS "${LIBHDFSPP_STATIC_LIB}"
      CMAKE_ARGS ${LIBHDFSPP_CMAKE_ARGS})

    include_directories (SYSTEM ${LIBHDFSPP_INCLUDE_DIRS})
    add_library (libhdfspp STATIC IMPORTED)
    set_target_properties (libhdfspp PROPERTIES IMPORTED_LOCATION ${LIBHDFSPP_STATIC_LIB})
    set (LIBHDFSPP_LIBRARIES libhdfspp ${CYRUS_SASL_SHARED_LIB} ${OPENSSL_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
    add_dependencies (libhdfspp libhdfspp_ep)
    install(DIRECTORY ${LIBHDFSPP_PREFIX}/lib DESTINATION .
                                         PATTERN "pkgconfig" EXCLUDE
                                         PATTERN "*.so*" EXCLUDE
                                         PATTERN "*.dylib" EXCLUDE)
  else(ORC_CXX_HAS_THREAD_LOCAL)
    message(WARNING
    "WARNING: Libhdfs++ library was not built because the required feature \
    thread_local storage is not supported by your compiler. Known compilers that \
    support this feature: GCC, Visual Studio, Clang (community version), \
    Clang (version for iOS 9 and later), Clang (version for Xcode 8 and later)")
  endif(ORC_CXX_HAS_THREAD_LOCAL)
endif(BUILD_LIBHDFSPP)
