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

cdata = configuration_data()
cdata.set('ORC_VERSION', meson.project_version())
cdata.set('ORC_CXX_HAS_CSTDINT', 1)

configure_file(
    input: 'orc-config.hh.in',
    output: 'orc-config.hh',
    configuration: cdata,
    format: 'cmake',
    install: true,
    install_dir: 'orc',
)

install_headers(
    [
        'BloomFilter.hh',
        'ColumnPrinter.hh',
        'Common.hh',
        'Exceptions.hh',
        'Geospatial.hh',
        'Int128.hh',
        'MemoryPool.hh',
        'OrcFile.hh',
        'Reader.hh',
        'Statistics.hh',
        'Type.hh',
        'Vector.hh',
        'Writer.hh',
    ],
    subdir: 'orc',
)

install_headers(
    [
        'sargs/Literal.hh',
        'sargs/SearchArgument.hh',
        'sargs/TruthValue.hh',
    ],
    subdir: 'orc/sargs',
)
