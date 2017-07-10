set (LZ4_VERSION "1.7.5")
set (SNAPPY_VERSION "1.1.3")
set (ZLIB_VERSION "1.2.11")
set (THIRDPARTY_DIR "${CMAKE_BINARY_DIR}/c++/libs/thirdparty")

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

# ----------------------------------------------------------------------
# Snappy

set (SNAPPY_PREFIX "${THIRDPARTY_DIR}/snappy_ep-install")
set (SNAPPY_HOME "${SNAPPY_PREFIX}")
set (SNAPPY_INCLUDE_DIRS "${SNAPPY_PREFIX}/include")
set (SNAPPY_STATIC_LIB_NAME snappy)
set (SNAPPY_STATIC_LIB "${SNAPPY_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${SNAPPY_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
set (SNAPPY_SRC_URL "${CMAKE_SOURCE_DIR}/c++/libs/snappy-${SNAPPY_VERSION}.tar.gz")
if (${UPPERCASE_BUILD_TYPE} EQUAL "RELEASE")
   set (SNAPPY_CXXFLAGS "CXXFLAGS='-DNDEBUG -O2'")
endif ()

ExternalProject_Add (snappy_ep
  CONFIGURE_COMMAND ./configure "--prefix=${SNAPPY_PREFIX}" ${SNAPPY_CXXFLAGS}
  BUILD_IN_SOURCE 1
  BUILD_COMMAND ${MAKE}
  INSTALL_DIR ${SNAPPY_PREFIX}
  URL ${SNAPPY_SRC_URL}
  LOG_INSTALL 1
  LOG_BUILD 1
  LOG_CONFIGURE 1
  BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")
include_directories (SYSTEM ${SNAPPY_INCLUDE_DIRS})
add_library (snappy STATIC IMPORTED)
set_target_properties (snappy PROPERTIES IMPORTED_LOCATION ${SNAPPY_STATIC_LIB})
set (SNAPPY_LIBRARIES snappy)
add_dependencies (snappy snappy_ep)

# ----------------------------------------------------------------------
# ZLIB

set (ZLIB_PREFIX "${THIRDPARTY_DIR}/zlib_ep-install")
set (ZLIB_HOME "${ZLIB_PREFIX}")
set (ZLIB_INCLUDE_DIRS "${ZLIB_PREFIX}/include")
set (ZLIB_STATIC_LIB_NAME z)
set (ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${ZLIB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
set (ZLIB_SRC_URL "${CMAKE_SOURCE_DIR}/c++/libs/zlib-${ZLIB_VERSION}.tar.gz")
set (ZLIB_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                      -DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}
                      -DCMAKE_C_FLAGS=${EP_C_FLAGS}
                      -DBUILD_SHARED_LIBS=OFF)

ExternalProject_Add (zlib_ep
  URL ${ZLIB_SRC_URL}
  BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
  LOG_INSTALL 1
  LOG_CONFIGURE 1
  LOG_BUILD 1
  CMAKE_ARGS ${ZLIB_CMAKE_ARGS})

include_directories (SYSTEM ${ZLIB_INCLUDE_DIRS})
add_library (zlib STATIC IMPORTED)
set_target_properties (zlib PROPERTIES IMPORTED_LOCATION ${ZLIB_STATIC_LIB})
set (ZLIB_LIBRARIES zlib)
add_dependencies (zlib zlib_ep)

# ----------------------------------------------------------------------
# LZ4

set (LZ4_PREFIX "${THIRDPARTY_DIR}/lz4_ep-install")
set (LZ4_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/lz4_ep-prefix/src/lz4_ep")
set (LZ4_INCLUDE_DIRS "${LZ4_PREFIX}/include")
set (LZ4_STATIC_LIB_NAME lz4)
set (LZ4_STATIC_LIB "${LZ4_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}${LZ4_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
set (LZ4_SRC_URL "${CMAKE_SOURCE_DIR}/c++/libs/lz4-${LZ4_VERSION}.tar.gz")
set (LZ4_BUILD_COMMAND "make -j4")

ExternalProject_Add(lz4_ep
  CONFIGURE_COMMAND ""
  INSTALL_COMMAND make "PREFIX=${LZ4_PREFIX}" install
  BUILD_IN_SOURCE 1
  BUILD_COMMAND ${MAKE}
  URL ${LZ4_SRC_URL}
  LOG_INSTALL 1
  LOG_BUILD 1
  BUILD_BYPRODUCTS ${LZ4_STATIC_LIB}
  )
include_directories (SYSTEM ${LZ4_INCLUDE_DIRS})
add_library (lz4 STATIC IMPORTED)
set_target_properties (lz4 PROPERTIES IMPORTED_LOCATION ${LZ4_STATIC_LIB})
set (LZ4_LIBRARIES lz4)
add_dependencies (lz4 lz4_ep)
