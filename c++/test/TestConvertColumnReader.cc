/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/Type.hh"
#include "wrap/gtest-wrapper.h"

#include "MockStripeStreams.hh"
#include "OrcTest.hh"
#include "SchemaEvolution.hh"

#include "ConvertColumnReader.hh"
#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"

namespace orc {

  static std::unique_ptr<Reader> createReader(MemoryPool& memoryPool,
                                              std::unique_ptr<InputStream> stream) {
    ReaderOptions options;
    options.setMemoryPool(memoryPool);
    return createReader(std::move(stream), options);
  }

  TEST(ConvertColumnReader, betweenNumericWithoutOverflows) {
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024;
    constexpr int TEST_CASES = 1024;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> fileType(
        Type::buildTypeFromString("struct<t1:boolean,t2:int,t3:double,t4:float>"));
    std::shared_ptr<Type> readType(
        Type::buildTypeFromString("struct<t1:int,t2:boolean,t3:bigint,t4:boolean>"));
    WriterOptions options;
    options.setUseTightNumericVector(true);
    auto writer = createWriter(*fileType, &memStream, options);
    auto batch = writer->createRowBatch(TEST_CASES);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& c0 = dynamic_cast<ByteVectorBatch&>(*structBatch.fields[0]);
    auto& c1 = dynamic_cast<IntVectorBatch&>(*structBatch.fields[1]);
    auto& c2 = dynamic_cast<DoubleVectorBatch&>(*structBatch.fields[2]);
    auto& c3 = dynamic_cast<FloatVectorBatch&>(*structBatch.fields[3]);

    structBatch.numElements = c0.numElements = c1.numElements = c2.numElements = c3.numElements =
        TEST_CASES;

    for (size_t i = 0; i < TEST_CASES; i++) {
      c0.data[i] = i % 2 || i % 3 ? true : false;
      c1.data[i] = static_cast<int>((TEST_CASES / 2 - i) * TEST_CASES);
      c2.data[i] = static_cast<double>(TEST_CASES - i) / (TEST_CASES / 2);
      c3.data[i] = static_cast<float>(TEST_CASES - i) / (TEST_CASES / 2);
    }

    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    auto pool = getDefaultPool();
    auto reader = createReader(*pool, std::move(inStream));
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setReadType(readType);
    rowReaderOpts.setUseTightNumericVector(true);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto readBatch = rowReader->createRowBatch(TEST_CASES);
    EXPECT_EQ(true, rowReader->next(*readBatch));
    auto& readStructBatch = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& readC0 = dynamic_cast<IntVectorBatch&>(*readStructBatch.fields[0]);
    auto& readC1 = dynamic_cast<ByteVectorBatch&>(*readStructBatch.fields[1]);
    auto& readC2 = dynamic_cast<LongVectorBatch&>(*readStructBatch.fields[2]);
    auto& readC3 = dynamic_cast<ByteVectorBatch&>(*readStructBatch.fields[3]);

    for (size_t i = 0; i < TEST_CASES; i++) {
      EXPECT_EQ(readC0.data[i], i % 2 || i % 3 ? 1 : 0);
      EXPECT_TRUE(readC1.data[i] == true || i == TEST_CASES / 2);
      EXPECT_EQ(readC2.data[i],
                i > TEST_CASES / 2 ? 0 : static_cast<int64_t>((TEST_CASES - i) / (TEST_CASES / 2)));
      EXPECT_TRUE(readC3.data[i] == true || i > TEST_CASES / 2);
    }

    rowReaderOpts.setUseTightNumericVector(false);
    rowReader = reader->createRowReader(rowReaderOpts);
    readBatch = rowReader->createRowBatch(TEST_CASES);
    EXPECT_THROW(rowReader->next(*readBatch), SchemaEvolutionError);
  }

  TEST(ConvertColumnReader, betweenNumricOverflows) {
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> fileType(
        Type::buildTypeFromString("struct<t1:double,t2:bigint,t3:bigint>"));
    std::shared_ptr<Type> readType(Type::buildTypeFromString("struct<t1:int,t2:int,t3:float>"));
    WriterOptions options;
    auto writer = createWriter(*fileType, &memStream, options);
    auto batch = writer->createRowBatch(2);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& c0 = dynamic_cast<DoubleVectorBatch&>(*structBatch.fields[0]);
    auto& c1 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[1]);
    auto& c2 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[2]);

    c0.data[0] = 1e35;
    c0.data[1] = 1e9 + 7;
    c1.data[0] = (1LL << 31);
    c1.data[1] = (1LL << 31) - 1;
    c2.data[0] = (1LL << 62) + 112312;
    c2.data[1] = (1LL << 20) + 77553;

    structBatch.numElements = c0.numElements = c1.numElements = c2.numElements = 2;
    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    auto pool = getDefaultPool();
    auto reader = createReader(*pool, std::move(inStream));
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setReadType(readType);
    rowReaderOpts.setUseTightNumericVector(true);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto readBatch = rowReader->createRowBatch(2);
    EXPECT_EQ(true, rowReader->next(*readBatch));
    auto& readStructBatch = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& readC0 = dynamic_cast<IntVectorBatch&>(*readStructBatch.fields[0]);
    auto& readC1 = dynamic_cast<IntVectorBatch&>(*readStructBatch.fields[1]);
    auto& readC2 = dynamic_cast<FloatVectorBatch&>(*readStructBatch.fields[2]);
    EXPECT_EQ(readC0.notNull[0], false);
    EXPECT_EQ(readC1.notNull[0], false);
    EXPECT_EQ(readC2.notNull[0], true);
    EXPECT_TRUE(readC0.notNull[1]);
    EXPECT_TRUE(readC1.notNull[1]);
    EXPECT_TRUE(readC2.notNull[1]);

    rowReaderOpts.throwOnSchemaEvolutionOverflow(true);
    rowReader = reader->createRowReader(rowReaderOpts);
    readBatch = rowReader->createRowBatch(2);
    EXPECT_THROW(rowReader->next(*readBatch), SchemaEvolutionError);
  }
}  // namespace orc
