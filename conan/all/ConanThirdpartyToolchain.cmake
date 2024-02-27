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

# ----------------------------------------------------------------------
# Snappy

find_package (Snappy REQUIRED)

add_library (orc_snappy INTERFACE)
add_library (orc::snappy ALIAS orc_snappy)
target_link_libraries(orc_snappy INTERFACE Snappy::snappy)

# ----------------------------------------------------------------------
# ZLIB

find_package (ZLIB REQUIRED)

add_library (orc_zlib INTERFACE)
add_library (orc::zlib ALIAS orc_zlib)
target_link_libraries (orc_zlib INTERFACE ZLIB::ZLIB)

# ----------------------------------------------------------------------
# Zstd

find_package (ZSTD REQUIRED)

add_library (orc_zstd INTERFACE)
add_library (orc::zstd ALIAS orc_zstd)
target_link_libraries (orc_zstd INTERFACE
  $<TARGET_NAME_IF_EXISTS:zstd::libzstd_static>
  $<TARGET_NAME_IF_EXISTS:zstd::libzstd_shared>
)

# ----------------------------------------------------------------------
# LZ4

find_package (LZ4 REQUIRED)

add_library (orc_lz4 INTERFACE)
add_library (orc::lz4 ALIAS orc_lz4)
target_link_libraries (orc_lz4 INTERFACE
  $<TARGET_NAME_IF_EXISTS:LZ4::lz4_shared>
  $<TARGET_NAME_IF_EXISTS:LZ4::lz4_static>
)

# ----------------------------------------------------------------------
# Protobuf

find_package (Protobuf REQUIRED)

add_library (orc_protobuf INTERFACE)
add_library (orc::protobuf ALIAS orc_protobuf)
target_link_libraries (orc_protobuf INTERFACE protobuf::protobuf)
