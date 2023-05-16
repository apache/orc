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

  using BooleanVectorBatch = ByteVectorBatch;

  static std::unique_ptr<Reader> createReader(MemoryPool& memoryPool,
                                              std::unique_ptr<InputStream> stream) {
    ReaderOptions options;
    options.setMemoryPool(memoryPool);
    return createReader(std::move(stream), options);
  }

  TEST(ConvertColumnReader, betweenNumericWithoutOverflows) {
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
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
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
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

  // Test for converting from boolean to string/char/varchar
  // Create a file with schema struct<t1:boolean,t2:boolean,t3:boolean,t4:boolean,t5:boolean> and
  // write 1024 rows with alternating true and false values. Read the file with schema
  // struct<t1:string,t2:char(3),t3:char(7),t4:varchar(3),t5:varchar(7)> and verify that the values
  // are "TRUE" and "FALSE".
  TEST(ConvertColumnReader, booleanToString) {
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> fileType(Type::buildTypeFromString(
        "struct<t1:boolean,t2:boolean,t3:boolean,t4:boolean,t5:boolean>"));
    std::shared_ptr<Type> readType(Type::buildTypeFromString(
        "struct<t1:string,t2:char(3),t3:char(7),t4:varchar(3),t5:varchar(7)>"));
    WriterOptions options;
    options.setUseTightNumericVector(true);
    auto writer = createWriter(*fileType, &memStream, options);
    auto batch = writer->createRowBatch(1024);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);
    auto& c0 = dynamic_cast<BooleanVectorBatch&>(*structBatch.fields[0]);
    auto& c1 = dynamic_cast<BooleanVectorBatch&>(*structBatch.fields[1]);
    auto& c2 = dynamic_cast<BooleanVectorBatch&>(*structBatch.fields[2]);
    auto& c3 = dynamic_cast<BooleanVectorBatch&>(*structBatch.fields[3]);
    auto& c4 = dynamic_cast<BooleanVectorBatch&>(*structBatch.fields[4]);

    for (size_t i = 0; i < 1024; i++) {
      c0.data[i] = static_cast<char>(i % 2 == 0 ? 0 : 1);
      c1.data[i] = static_cast<char>(i % 3 == 0 ? 0 : 1);
      c2.data[i] = static_cast<char>(i % 5 == 0 ? 0 : 1);
      c3.data[i] = static_cast<char>(i % 7 == 0 ? 0 : 1);
      c4.data[i] = static_cast<char>(i % 11 == 0 ? 0 : 1);
    }

    structBatch.numElements = c0.numElements = c1.numElements = c2.numElements = c3.numElements =
        c4.numElements = 1024;
    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    auto pool = getDefaultPool();
    auto reader = createReader(*pool, std::move(inStream));
    RowReaderOptions rowReaderOpts;
    rowReaderOpts.setReadType(readType);
    rowReaderOpts.setUseTightNumericVector(true);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    auto readBatch = rowReader->createRowBatch(1024);
    EXPECT_EQ(true, rowReader->next(*readBatch));
    auto& readStructBatch = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& readC0 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[0]);
    auto& readC1 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[1]);
    auto& readC2 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[2]);
    auto& readC3 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[3]);
    auto& readC4 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[4]);

    for (size_t i = 0; i < 1024; i++) {
      EXPECT_EQ(std::string(readC0.data[i], readC0.length[i]), i % 2 == 0 ? "FALSE" : "TRUE");
      EXPECT_EQ(std::string(readC1.data[i], readC1.length[i]), i % 3 == 0 ? "FAL" : "TRU");
      EXPECT_EQ(std::string(readC2.data[i], readC2.length[i]), i % 5 == 0 ? "FALSE  " : "TRUE   ");
      EXPECT_EQ(std::string(readC3.data[i], readC3.length[i]), i % 7 == 0 ? "FAL" : "TRU");
      EXPECT_EQ(std::string(readC4.data[i], readC4.length[i]), i % 11 == 0 ? "FALSE" : "TRUE");
    }
  }

  TEST(ConvertColumnReader, TestConvertNumericToStringVariant) {
    // Create a memory buffer to hold the ORC file data
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
    constexpr int64_t TEST_CASES = 1024;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> fileType(Type::buildTypeFromString(
        "struct<t1:tinyint,t2:smallint,t3:int,t4:bigint,t5:float,t6:double,"
        "t7:tinyint,t8:smallint,t9:int,t10:bigint,t11:float,t12:double,"
        "t13:tinyint,t14:smallint,t15:int,t16:bigint,t17:float,t18:double"
        ">"));
    std::shared_ptr<Type> readType(Type::buildTypeFromString(
        "struct<t1:string,t2:string,t3:string,t4:string,t5:string,t6:string,"
        "t7:char(1),t8:char(2),t9:char(3),t10:char(4),t11:char(5),t12:char(6),"
        "t13:varchar(1),t14:varchar(2),t15:varchar(3),t16:varchar(4),t17:varchar(5),t18:varchar(6)"
        ">"));
    WriterOptions options;
    options.setUseTightNumericVector(true);
    auto writer = createWriter(*fileType, &memStream, options);
    auto batch = writer->createRowBatch(TEST_CASES);
    auto& structBatch = dynamic_cast<StructVectorBatch&>(*batch);

    for (int i = 0; i < 3; i++) {
      auto& col0 = dynamic_cast<ByteVectorBatch&>(*structBatch.fields[i * 6]);
      auto& col1 = dynamic_cast<ShortVectorBatch&>(*structBatch.fields[i * 6 + 1]);
      auto& col2 = dynamic_cast<IntVectorBatch&>(*structBatch.fields[i * 6 + 2]);
      auto& col3 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[i * 6 + 3]);
      auto& col4 = dynamic_cast<FloatVectorBatch&>(*structBatch.fields[i * 6 + 4]);
      auto& col5 = dynamic_cast<DoubleVectorBatch&>(*structBatch.fields[i * 6 + 5]);
      for (int j = 0; j < TEST_CASES; j++) {
        int flag = j % 2 == 0 ? -1 : 1;
        col0.data[j] = static_cast<char>(flag * (j % 128));
        col1.data[j] = static_cast<short>(flag * (j % 32768));
        col2.data[j] = flag * j;
        col3.data[j] = flag * j;
        col4.data[j] = static_cast<float>(flag * j) * 1.234f;
        col5.data[j] = static_cast<double>(flag * j) * 1.234;
      }
      col0.numElements = col1.numElements = col2.numElements = col3.numElements = col4.numElements =
          col5.numElements = TEST_CASES;
    }

    structBatch.numElements = TEST_CASES;
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

    // column 0 to 17
    auto& readC1 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[0]);
    auto& readC2 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[1]);
    auto& readC3 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[2]);
    auto& readC4 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[3]);
    auto& readC5 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[4]);
    auto& readC6 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[5]);
    auto& readC7 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[6]);
    auto& readC8 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[7]);
    auto& readC9 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[8]);
    auto& readC10 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[9]);
    auto& readC11 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[10]);
    auto& readC12 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[11]);
    auto& readC13 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[12]);
    auto& readC14 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[13]);
    auto& readC15 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[14]);
    auto& readC16 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[15]);
    auto& readC17 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[16]);
    auto& readC18 = dynamic_cast<StringVectorBatch&>(*readStructBatch.fields[17]);

    std::vector<std::vector<std::string>> origin(30, std::vector<std::string>(TEST_CASES));
    for (int j = 0; j < TEST_CASES; j++) {
      for (int k = 0; k < 5; k++) {
        int flag = j % 2 == 0 ? -1 : 1;
        origin[6 * k + 0][j] = std::to_string(static_cast<char>(flag * (j % 128)));
        origin[6 * k + 1][j] = std::to_string(static_cast<short>(flag * (j % 32768)));
        origin[6 * k + 2][j] = std::to_string(flag * j);
        origin[6 * k + 3][j] = std::to_string(flag * j);
        origin[6 * k + 4][j] = std::to_string(static_cast<float>(flag * j) * 1.234f);
        origin[6 * k + 5][j] = std::to_string(static_cast<double>(flag * j) * 1.234);
      }
    }

    for (int i = 0; i < TEST_CASES; i++) {
      std::vector<std::string> expected(19);
      for (int j = 1; j <= 18; j++) {
        expected[j] = origin[j - 1][i];
      }
      for (int i = 7; i <= 12; i++) {
        int length = i - 6;
        if (expected[i].size() > length) {
          expected[i].resize(length);
        } else {
          expected[i].resize(length, ' ');
        }
      }
      for (int i = 13; i <= 18; i++) {
        int length = i - 12;
        if (expected[i].size() > length) {
          expected[i].resize(length);
        }
      }
      EXPECT_EQ(expected[1], std::string(readC1.data[i], readC1.length[i])) << i;
      EXPECT_EQ(expected[2], std::string(readC2.data[i], readC2.length[i])) << i;
      EXPECT_EQ(expected[3], std::string(readC3.data[i], readC3.length[i])) << i;
      EXPECT_EQ(expected[4], std::string(readC4.data[i], readC4.length[i])) << i;
      EXPECT_EQ(expected[5], std::string(readC5.data[i], readC5.length[i])) << i;
      EXPECT_EQ(expected[6], std::string(readC6.data[i], readC6.length[i])) << i;
      EXPECT_EQ(expected[7], std::string(readC7.data[i], readC7.length[i])) << i;
      EXPECT_EQ(expected[8], std::string(readC8.data[i], readC8.length[i])) << i;
      EXPECT_EQ(expected[9], std::string(readC9.data[i], readC9.length[i])) << i;
      EXPECT_EQ(expected[10], std::string(readC10.data[i], readC10.length[i])) << i;
      EXPECT_EQ(expected[11], std::string(readC11.data[i], readC11.length[i])) << i;
      EXPECT_EQ(expected[12], std::string(readC12.data[i], readC12.length[i])) << i;
      EXPECT_EQ(expected[13], std::string(readC13.data[i], readC13.length[i])) << i;
      EXPECT_EQ(expected[14], std::string(readC14.data[i], readC14.length[i])) << i;
      EXPECT_EQ(expected[15], std::string(readC15.data[i], readC15.length[i])) << i;
      EXPECT_EQ(expected[16], std::string(readC16.data[i], readC16.length[i])) << i;
      EXPECT_EQ(expected[17], std::string(readC17.data[i], readC17.length[i])) << i;
      EXPECT_EQ(expected[18], std::string(readC18.data[i], readC18.length[i])) << i;
    }
  }

  // Test conversion from numeric to decimal64/decimal128
  TEST(ConvertColumnReader, TestConvertNumericToDecimal) {
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
    constexpr int TEST_CASES = 1024;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> fileType(
        Type::buildTypeFromString("struct<c1:bigint,c2:double,c3:bigint,c4:double>"));
    std::shared_ptr<Type> readType(Type::buildTypeFromString(
        "struct<c1:decimal(10,2),c2:decimal(10,4),c3:decimal(20,3),c4:decimal(20,3)>"));
    WriterOptions options;
    options.setUseTightNumericVector(true);
    auto writer = createWriter(*fileType, &memStream, options);
    auto batch = writer->createRowBatch(TEST_CASES);
    auto structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    auto& c1 = dynamic_cast<LongVectorBatch&>(*structBatch->fields[0]);
    auto& c2 = dynamic_cast<DoubleVectorBatch&>(*structBatch->fields[1]);
    auto& c3 = dynamic_cast<LongVectorBatch&>(*structBatch->fields[2]);
    auto& c4 = dynamic_cast<DoubleVectorBatch&>(*structBatch->fields[3]);

    for (int i = 0; i < TEST_CASES; i++) {
      c1.data[i] = i * 12;
      c3.data[i] = i * 16;
      if (i % 2) {
        c2.data[i] = i * 13 + 0.55555;
        c4.data[i] = i * 1234 + 0.55555;
      } else {
        c2.data[i] = i * 17 + 0.11111;
        c4.data[i] = i * 1234 + 0.11111;
      }
    }
    structBatch->numElements = c1.numElements = c2.numElements = c3.numElements = c4.numElements =
        TEST_CASES;
    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    auto pool = getDefaultPool();
    auto reader = createReader(*pool, std::move(inStream));
    RowReaderOptions rowReaderOptions;
    rowReaderOptions.setUseTightNumericVector(true);
    rowReaderOptions.setReadType(readType);
    auto rowReader = reader->createRowReader(rowReaderOptions);
    auto readBatch = rowReader->createRowBatch(TEST_CASES);
    EXPECT_EQ(true, rowReader->next(*readBatch));

    auto& readStructBatch = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& readC1 = dynamic_cast<Decimal64VectorBatch&>(*readStructBatch.fields[0]);
    auto& readC2 = dynamic_cast<Decimal64VectorBatch&>(*readStructBatch.fields[1]);
    auto& readC3 = dynamic_cast<Decimal128VectorBatch&>(*readStructBatch.fields[2]);
    auto& readC4 = dynamic_cast<Decimal128VectorBatch&>(*readStructBatch.fields[3]);
    EXPECT_EQ(TEST_CASES, readBatch->numElements);
    for (int i = 0; i < TEST_CASES; i++) {
      EXPECT_TRUE(readC1.notNull[i]) << i;
      EXPECT_TRUE(readC2.notNull[i]) << i;
      EXPECT_TRUE(readC3.notNull[i]) << i;
      EXPECT_TRUE(readC4.notNull[i]) << i;

      EXPECT_EQ(i * 1200, readC1.values[i]) << i;
      EXPECT_EQ(i * 16000, readC3.values[i].toLong()) << i;
      if (i % 2) {
        EXPECT_EQ(1LL * i * 130000 + 5556, readC2.values[i]);
        EXPECT_EQ(1LL * i * 1234000 + 556, readC4.values[i].toLong());
      } else {
        EXPECT_EQ(1LL * i * 170000 + 1111, readC2.values[i]);
        EXPECT_EQ(1LL * i * 1234000 + 111, readC4.values[i].toLong());
      }
    }
  }

  // Test conversion from numeric to timestamp/timestamp with local timezone
  TEST(ConvertColumnReader, TestConvertNumericToTimestamp) {
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
    constexpr int TEST_CASES = 1024;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> fileType(
        Type::buildTypeFromString("struct<c1:bigint,c2:double,c3:bigint,c4:double>"));
    std::shared_ptr<Type> readType(
        Type::buildTypeFromString("struct<c1:timestamp,c2:timestamp,c3:timestamp with local time "
                                  "zone,c4:timestamp with local time zone>"));
    WriterOptions options;
    options.setUseTightNumericVector(true);
    auto writer = createWriter(*fileType, &memStream, options);
    auto batch = writer->createRowBatch(TEST_CASES);
    auto structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    auto& c1 = dynamic_cast<LongVectorBatch&>(*structBatch->fields[0]);
    auto& c2 = dynamic_cast<DoubleVectorBatch&>(*structBatch->fields[1]);
    auto& c3 = dynamic_cast<LongVectorBatch&>(*structBatch->fields[2]);
    auto& c4 = dynamic_cast<DoubleVectorBatch&>(*structBatch->fields[3]);

    for (int i = 0; i < TEST_CASES; i++) {
      c1.data[i] = (i - TEST_CASES / 2) * 3600 + i;
      c2.data[i] = (i - TEST_CASES / 2) * 3600 + i;
      c3.data[i] = (i - TEST_CASES / 2) * 3600 + i * i;
      c4.data[i] = (i - TEST_CASES / 2) * 3600 + i * i;
      if (i % 2) {
        c2.data[i] += 0.55555;
        c4.data[i] += 0.777;
      } else {
        c2.data[i] += 0.11111;
        c4.data[i] += 0.333;
      }
    }

    structBatch->numElements = c1.numElements = c2.numElements = c3.numElements = c4.numElements =
        TEST_CASES;
    writer->add(*batch);
    writer->close();

    auto inStream = std::make_unique<MemoryInputStream>(memStream.getData(), memStream.getLength());
    auto pool = getDefaultPool();
    auto reader = createReader(*pool, std::move(inStream));
    RowReaderOptions rowReaderOptions;
    rowReaderOptions.setTimezoneName("Asia/Shanghai");
    rowReaderOptions.setUseTightNumericVector(true);
    rowReaderOptions.setReadType(readType);
    auto rowReader = reader->createRowReader(rowReaderOptions);
    auto readBatch = rowReader->createRowBatch(TEST_CASES);
    EXPECT_EQ(true, rowReader->next(*readBatch));

    auto& readStructBatch = dynamic_cast<StructVectorBatch&>(*readBatch);
    auto& readC1 = dynamic_cast<TimestampVectorBatch&>(*readStructBatch.fields[0]);
    auto& readC2 = dynamic_cast<TimestampVectorBatch&>(*readStructBatch.fields[1]);
    auto& readC3 = dynamic_cast<TimestampVectorBatch&>(*readStructBatch.fields[2]);
    auto& readC4 = dynamic_cast<TimestampVectorBatch&>(*readStructBatch.fields[3]);
    EXPECT_EQ(TEST_CASES, readBatch->numElements);
    for (int i = 0; i < TEST_CASES; i++) {
      EXPECT_TRUE(readC1.notNull[i]) << i;
      EXPECT_TRUE(readC2.notNull[i]) << i;
      EXPECT_TRUE(readC3.notNull[i]) << i;
      EXPECT_TRUE(readC4.notNull[i]) << i;

      EXPECT_EQ((i - TEST_CASES / 2) * 3600 + i - 8 * 3600, readC1.data[i]) << i;
      EXPECT_EQ((i - TEST_CASES / 2) * 3600 + i - 8 * 3600, readC2.data[i]) << i;
      EXPECT_EQ((i - TEST_CASES / 2) * 3600 + i * i, readC3.data[i]) << i;
      EXPECT_EQ((i - TEST_CASES / 2) * 3600 + i * i, readC4.data[i]) << i;
      EXPECT_EQ(0, readC1.nanoseconds[i]) << i;
      EXPECT_EQ(0, readC3.nanoseconds[i]) << i;
      if (i % 2) {
        EXPECT_TRUE(std::abs(555550000 - readC2.nanoseconds[i]) <= 1) << i;
        EXPECT_TRUE(std::abs(777000000 - readC4.nanoseconds[i]) <= 1) << i;
      } else {
        EXPECT_TRUE(std::abs(111110000 - readC2.nanoseconds[i]) <= 1) << i;
        EXPECT_TRUE(std::abs(333000000 - readC4.nanoseconds[i]) <= 1) << i;
      }
    }
  }

}  // namespace orc
