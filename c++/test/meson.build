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

test_incdir = include_directories(
    '../include',
    '../src',
)

test_sources = [
    'MemoryInputStream.cc',
    'MemoryOutputStream.cc',
    'MockStripeStreams.cc',
    'TestAttributes.cc',
    'TestBlockBuffer.cc',
    'TestBufferedOutputStream.cc',
    'TestBloomFilter.cc',
    'TestByteRle.cc',
    'TestByteRLEEncoder.cc',
    'TestColumnPrinter.cc',
    'TestColumnReader.cc',
    'TestColumnStatistics.cc',
    'TestCompression.cc',
    'TestConvertColumnReader.cc',
    'TestDecompression.cc',
    'TestDecimal.cc',
    'TestDictionaryEncoding.cc',
    'TestDriver.cc',
    'TestInt128.cc',
    'TestMurmur3.cc',
    'TestPredicateLeaf.cc',
    'TestPredicatePushdown.cc',
    'TestReader.cc',
    'TestRleDecoder.cc',
    'TestRleEncoder.cc',
    'TestRLEV2Util.cc',
    'TestSargsApplier.cc',
    'TestSearchArgument.cc',
    'TestSchemaEvolution.cc',
    'TestStatistics.cc',
    'TestStripeIndexStatistics.cc',
    'TestTimestampStatistics.cc',
    'TestTimezone.cc',
    'TestType.cc',
    'TestUtil.cc',
    'TestWriter.cc',
    'TestCache.cc',        
]

orc_test = executable(
    'orc-test',
    sources: test_sources,
    include_directories: test_incdir,
    dependencies: [
        orc_dep,
        lz4_dep,
        protobuf_dep,
        snappy_dep,
        zlib_dep,
        gtest_dep,
        gmock_dep,
        sparsehash_c11_dep,
    ],
)

exc = executable(
    'create-test-files',
    sources: ['CreateTestFiles.cc'],
    include_directories: test_incdir,    
    dependencies: [
        orc_dep,
        protobuf_dep,
    ],
)
test('orc-test', exc)
