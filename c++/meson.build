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

# required dependencies
protobuf_dep = dependency('protobuf', fallback: ['protobuf', 'protobuf_dep'])
lz4_dep = dependency('liblz4')
snappy_dep = dependency('snappy')
zlib_dep = dependency('zlib')
zstd_dep = dependency('libzstd')
sparsehash_c11_dep = dependency('sparsehash-c11')

# optional dependencies (should be set later in configuration)
gtest_dep = disabler()
gmock_dep = disabler()

subdir('include/orc')
subdir('src')

if get_option('tests').enabled()
    gtest_dep = dependency('gtest')
    gmock_dep = dependency('gmock')
    subdir('test')
endif

pkg = import('pkgconfig')
pkg.generate(orc_lib)
