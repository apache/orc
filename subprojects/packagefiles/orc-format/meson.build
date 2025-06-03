# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

project(
    'orc-format',
    'cpp',
    version: '1.1.0',
    license: 'Apache-2.0',
    meson_version: '>=1.3.0',
)

protobuf_dep = dependency('protobuf', fallback: ['protobuf', 'protobuf_dep'])
# in newer versions of protobuf from the wrapdb, find_program('protoc') will
# "do the right thing", but the protobuf wrapdb version we are using now does
# not call override_find_program('protoc', ...)
if protobuf_dep.type_name() == 'internal'
    protoc = subproject('protobuf').get_variable('protoc')
else
    protoc = find_program('protoc')
endif

fs = import('fs')
proto_file = meson.project_source_root() / 'src/main/proto/orc/proto/orc_proto.proto'
proto_parent_dir = fs.parent(proto_file)

orc_proto_files = custom_target(
    'orc-proto',
    input: [proto_file],
    output: ['orc_proto.pb.h', 'orc_proto.pb.cc'],
    command: [
        protoc,
        '-I',
        proto_parent_dir,
        '--cpp_out',
        meson.current_build_dir(),
        '@INPUT@',
    ],
)

orc_format_proto_dep = declare_dependency(
    sources: orc_proto_files[0],
    include_directories: include_directories('.'),
)
