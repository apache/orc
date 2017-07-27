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
  LOG_INSTALL 1
  LOG_BUILD 1
  LOG_CONFIGURE 1
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
  BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
  LOG_INSTALL 1
  LOG_DOWNLOAD 1
  LOG_CONFIGURE 1
  LOG_BUILD 1
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
  LOG_INSTALL 1
  LOG_BUILD 1
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
  LOG_INSTALL 1
  LOG_BUILD 1
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
