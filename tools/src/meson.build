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

tools_incdir = include_directories(
    '../../c++/include',
    '../../c++/src',    
)

tools = {
    'orc-contents': {
        'sources': ['FileContents.cc', 'ToolsHelper.cc'],
    },
    'orc-scan': {
        'sources': ['FileScan.cc', 'ToolsHelper.cc'],
    },
    'orc-metadata': {
        'sources': ['FileMetadata.cc', 'ToolsHelper.cc'],
    },
    'orc-statistics': {
        'sources': ['FileStatistics.cc'],
    },
    'orc-memory': {
        'sources': ['FileMemory.cc', 'ToolsHelper.cc'],
    },
    'timezone-dump': {
        'sources': ['TimezoneDump.cc'],        
    },
    'csv-import': {
        'sources': ['CSVFileImport.cc'],        
    },
}

foreach tool_name, val : tools
    executable(
        tool_name,
        sources: val['sources'],
        include_directories: tools_incdir,
        dependencies: [orc_dep, protobuf_dep],
        install: true,
    )
endforeach
