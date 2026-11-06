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

cmake_minimum_required (VERSION 3.25.0)
project (orc-config-test NONE)

include(CMakePackageConfigHelpers)

set(package_dir "${CMAKE_CURRENT_BINARY_DIR}/package")
file(MAKE_DIRECTORY "${package_dir}/include")
set(ORC_SYSTEM_DEPENDENCIES OrcConfigTestPresent OrcConfigTestDependency)
configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/../../orcConfig.cmake.in"
  "${package_dir}/orcConfig.cmake"
  INSTALL_DESTINATION .)

# Use mock dependencies so the tests do not depend on installed libraries.
file(WRITE "${package_dir}/FindOrcConfigTestPresent.cmake" [=[
set(OrcConfigTestPresent_FOUND TRUE)
add_library(OrcConfigTest::Present INTERFACE IMPORTED)
]=])
file(WRITE "${package_dir}/FindOrcConfigTestDependency.cmake" [=[
set(OrcConfigTestDependency_FOUND "${DEPENDENCY_FOUND}")
]=])
file(WRITE "${package_dir}/orcTargets.cmake" [=[
add_library(orc::orc UNKNOWN IMPORTED)
set_target_properties(orc::orc PROPERTIES
  IMPORTED_CONFIGURATIONS RELEASE
  IMPORTED_LOCATION_RELEASE "${CMAKE_CURRENT_LIST_DIR}/liborc.a"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_LIST_DIR}/include"
  INTERFACE_LINK_LIBRARIES OrcConfigTest::Present)
]=])

if(MODULE_PATH_STATE STREQUAL "UNSET")
  unset(CMAKE_MODULE_PATH)
elseif(MODULE_PATH_STATE STREQUAL "EMPTY")
  set(CMAKE_MODULE_PATH "")
elseif(MODULE_PATH_STATE STREQUAL "SET")
  set(caller_module_dir "${CMAKE_CURRENT_BINARY_DIR}/caller modules")
  file(MAKE_DIRECTORY "${caller_module_dir}")
  file(WRITE "${caller_module_dir}/FindOrcConfigTestCaller.cmake"
    "set(OrcConfigTestCaller_FOUND TRUE)\n")
  set(CMAKE_MODULE_PATH "${caller_module_dir};${CMAKE_CURRENT_BINARY_DIR}/other modules")
else()
  message(FATAL_ERROR "Unknown module path state: ${MODULE_PATH_STATE}")
endif()
set(module_path_before "${CMAKE_MODULE_PATH}")

find_package(orc CONFIG QUIET PATHS "${package_dir}" NO_DEFAULT_PATH)

if(NOT "${CMAKE_MODULE_PATH}" STREQUAL "${module_path_before}")
  message(FATAL_ERROR "orcConfig.cmake changed CMAKE_MODULE_PATH")
endif()
if(MODULE_PATH_STATE STREQUAL "UNSET")
  if(DEFINED CMAKE_MODULE_PATH)
    message(FATAL_ERROR "orcConfig.cmake defined CMAKE_MODULE_PATH")
  endif()
elseif(NOT DEFINED CMAKE_MODULE_PATH)
  message(FATAL_ERROR "orcConfig.cmake undefined CMAKE_MODULE_PATH")
endif()

if(NOT TARGET OrcConfigTest::Present)
  message(FATAL_ERROR "The dependency's imported target is not visible")
endif()
if(DEPENDENCY_FOUND)
  if(NOT orc_FOUND OR NOT ORC_FOUND OR NOT TARGET orc::orc)
    message(FATAL_ERROR "ORC was not found with all dependencies available")
  endif()
  if(NOT ORC_STATIC_LIB OR NOT ORC_INCLUDE_DIR OR NOT ORC_INCLUDE_DIRS)
    message(FATAL_ERROR "ORC compatibility variables were not set")
  endif()
else()
  if(orc_FOUND)
    message(FATAL_ERROR "ORC was found with a missing dependency")
  endif()
  if(NOT orc_NOT_FOUND_MESSAGE MATCHES "OrcConfigTestDependency")
    message(FATAL_ERROR "The missing dependency diagnostic was not propagated")
  endif()
  if(TARGET orc::orc)
    message(FATAL_ERROR "ORC targets were included despite a missing dependency")
  endif()
endif()

if(MODULE_PATH_STATE STREQUAL "SET")
  find_package(OrcConfigTestCaller REQUIRED MODULE)
endif()
