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
        "struct<t1:string,t2:char(6),t3:char(7),t4:varchar(5),t5:varchar(7)>"));
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
      EXPECT_EQ(std::string(readC0.data[i], static_cast<size_t>(readC0.length[i])),
                i % 2 == 0 ? "FALSE" : "TRUE");
      EXPECT_EQ(std::string(readC1.data[i], static_cast<size_t>(readC1.length[i])),
                i % 3 == 0 ? "FALSE " : "TRUE  ");
      EXPECT_EQ(std::string(readC2.data[i], static_cast<size_t>(readC2.length[i])),
                i % 5 == 0 ? "FALSE  " : "TRUE   ");
      EXPECT_EQ(std::string(readC3.data[i], static_cast<size_t>(readC3.length[i])),
                i % 7 == 0 ? "FALSE" : "TRUE");
      EXPECT_EQ(std::string(readC4.data[i], static_cast<size_t>(readC4.length[i])),
                i % 11 == 0 ? "FALSE" : "TRUE");
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

    for (size_t i = 0; i < 3; i++) {
      auto& col0 = dynamic_cast<ByteVectorBatch&>(*structBatch.fields[i * 6]);
      auto& col1 = dynamic_cast<ShortVectorBatch&>(*structBatch.fields[i * 6 + 1]);
      auto& col2 = dynamic_cast<IntVectorBatch&>(*structBatch.fields[i * 6 + 2]);
      auto& col3 = dynamic_cast<LongVectorBatch&>(*structBatch.fields[i * 6 + 3]);
      auto& col4 = dynamic_cast<FloatVectorBatch&>(*structBatch.fields[i * 6 + 4]);
      auto& col5 = dynamic_cast<DoubleVectorBatch&>(*structBatch.fields[i * 6 + 5]);
      for (int j = 0; j < TEST_CASES; j++) {
        int flag = j % 2 == 0 ? -1 : 1;
        uint64_t idx = static_cast<uint64_t>(j);
        col0.data[idx] = static_cast<int8_t>(flag * (j % 128));
        col1.data[idx] = static_cast<int16_t>(flag * (j % 32768));
        col2.data[idx] = flag * j;
        col3.data[idx] = flag * j;
        col4.data[idx] = static_cast<float>(flag * j) * 1.234f;
        col5.data[idx] = static_cast<double>(flag * j) * 1.234;
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
      for (size_t k = 0; k < 5; k++) {
        int flag = j % 2 == 0 ? -1 : 1;
        uint64_t idx = static_cast<uint64_t>(j);
        origin[6 * k + 0][idx] = std::to_string(static_cast<int8_t>(flag * (j % 128)));
        origin[6 * k + 1][idx] = std::to_string(static_cast<int16_t>(flag * (j % 32768)));
        origin[6 * k + 2][idx] = std::to_string(flag * j);
        origin[6 * k + 3][idx] = std::to_string(flag * j);
        origin[6 * k + 4][idx] = std::to_string(static_cast<float>(flag * j) * 1.234f);
        origin[6 * k + 5][idx] = std::to_string(static_cast<double>(flag * j) * 1.234);
      }
    }
    for (size_t i = 0; i < TEST_CASES; i++) {
      std::vector<std::string> expected(19);
      std::vector<bool> isNull(19, false);
      for (size_t j = 1; j <= 18; j++) {
        expected[j] = origin[j - 1][i];
      }
      for (size_t j = 7; j <= 12; j++) {
        size_t length = j - 6;
        if (expected[j].size() > length) {
          isNull[j] = true;
        } else {
          expected[j].resize(length, ' ');
        }
      }
      for (size_t j = 13; j <= 18; j++) {
        size_t length = j - 12;
        if (expected[j].size() > length) {
          isNull[j] = true;
        }
      }
#define TEST_COLUMN(index)                                                                    \
  if (isNull[index]) {                                                                        \
    EXPECT_EQ(false, readC##index.notNull[i])                                                 \
        << i << " " << expected[index] << " "                                                 \
        << std::string(readC##index.data[i], static_cast<size_t>(readC##index.length[i]));    \
  } else {                                                                                    \
    EXPECT_EQ(true, readC##index.notNull[i]) << i;                                            \
    EXPECT_EQ(expected[index],                                                                \
              std::string(readC##index.data[i], static_cast<size_t>(readC##index.length[i]))) \
        << i;                                                                                 \
  }
      TEST_COLUMN(1)
      TEST_COLUMN(2)
      TEST_COLUMN(3)
      TEST_COLUMN(4)
      TEST_COLUMN(5)
      TEST_COLUMN(6)
      TEST_COLUMN(7)
      TEST_COLUMN(8)
      TEST_COLUMN(9)
      TEST_COLUMN(10)
      TEST_COLUMN(11)
      TEST_COLUMN(12)
      TEST_COLUMN(13)
      TEST_COLUMN(14)
      TEST_COLUMN(15)
      TEST_COLUMN(16)
      TEST_COLUMN(17)
      TEST_COLUMN(18)
#undef TEST_COLUMN
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

    for (int32_t i = 0; i < TEST_CASES / 2; i++) {
      size_t idx = static_cast<size_t>(i);
      c1.data[idx] = i * 12;
      c3.data[idx] = i * 16;
      c2.data[idx] = (i % 2 ? -1 : 1) * (i * 1000 + 0.111111 * (i % 9));
      c4.data[idx] = (i % 2 ? -1 : 1) * (i * 1000 + 0.111111 * (i % 9));
    }
    for (int32_t i = TEST_CASES / 2; i < TEST_CASES; i++) {
      size_t idx = static_cast<size_t>(i);
      c1.data[idx] = 12345678910LL * i;
      c2.data[idx] = 12345678910.1234 * i;
      c3.data[idx] = 12345678910LL * i;
      c4.data[idx] = (123456.0 * i + (0.1111 * (i % 9))) * (i % 2 ? -1 : 1);
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
    for (int i = 0; i < TEST_CASES / 2; i++) {
      size_t idx = static_cast<size_t>(i);
      EXPECT_TRUE(readC1.notNull[idx]) << i;
      EXPECT_TRUE(readC2.notNull[idx]) << i;
      EXPECT_TRUE(readC3.notNull[idx]) << i;
      EXPECT_TRUE(readC4.notNull[idx]) << i;

      EXPECT_EQ(i * 1200, readC1.values[idx]) << i;
      EXPECT_EQ(i * 16000, readC3.values[idx].toLong()) << i;
      EXPECT_EQ((1LL * i * 10000000 + (1111 * (i % 9) + (i % 9 > 4))) * (i % 2 ? -1 : 1),
                readC2.values[idx])
          << i;
      EXPECT_EQ((1LL * i * 1000000 + (111 * (i % 9) + (i % 9 > 4))) * (i % 2 ? -1 : 1),
                readC4.values[idx].toLong())
          << i;
    }
    for (int i = TEST_CASES / 2; i < TEST_CASES; i++) {
      size_t idx = static_cast<size_t>(i);
      EXPECT_FALSE(readC1.notNull[idx]) << i;
      EXPECT_FALSE(readC2.notNull[idx]) << i;
      EXPECT_TRUE(readC3.notNull[idx]) << i;
      EXPECT_TRUE(readC4.notNull[idx]) << i;

      EXPECT_EQ(12345678910000LL * i, readC3.values[idx].toLong()) << i;
      EXPECT_EQ((123456000LL * i + (111 * (i % 9) + (i % 9 > 4))) * (i % 2 ? -1 : 1),
                readC4.values[idx].toLong())
          << i;
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
      size_t idx = static_cast<size_t>(i);
      c1.data[idx] = (i - TEST_CASES / 2) * 3600 + i;
      c2.data[idx] = (i - TEST_CASES / 2) * 3600 + i;
      c3.data[idx] = (i - TEST_CASES / 2) * 3600 + i * i;
      c4.data[idx] = (i - TEST_CASES / 2) * 3600 + i * i;
      if (i % 2) {
        c2.data[idx] += 0.55555;
        c4.data[idx] += 0.777;
      } else {
        c2.data[idx] += 0.11111;
        c4.data[idx] += 0.333;
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
      size_t idx = static_cast<size_t>(i);
      EXPECT_TRUE(readC1.notNull[idx]) << i;
      EXPECT_TRUE(readC2.notNull[idx]) << i;
      EXPECT_TRUE(readC3.notNull[idx]) << i;
      EXPECT_TRUE(readC4.notNull[idx]) << i;

      EXPECT_EQ((i - TEST_CASES / 2) * 3600 + i - 8 * 3600, readC1.data[idx]) << i;
      EXPECT_EQ((i - TEST_CASES / 2) * 3600 + i - 8 * 3600, readC2.data[idx]) << i;
      EXPECT_EQ((i - TEST_CASES / 2) * 3600 + i * i, readC3.data[idx]) << i;
      EXPECT_EQ((i - TEST_CASES / 2) * 3600 + i * i, readC4.data[idx]) << i;
      EXPECT_EQ(0, readC1.nanoseconds[idx]) << i;
      EXPECT_EQ(0, readC3.nanoseconds[idx]) << i;
      if (i % 2) {
        EXPECT_TRUE(std::abs(555550000 - readC2.nanoseconds[idx]) <= 1) << i;
        EXPECT_TRUE(std::abs(777000000 - readC4.nanoseconds[idx]) <= 1) << i;
      } else {
        EXPECT_TRUE(std::abs(111110000 - readC2.nanoseconds[idx]) <= 1) << i;
        EXPECT_TRUE(std::abs(333000000 - readC4.nanoseconds[idx]) <= 1) << i;
      }
    }
  }

  TEST(ConvertColumnReader, TestConvertDecimalToNumeric) {
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
    constexpr int TEST_CASES = 1024;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> fileType(Type::buildTypeFromString(
        "struct<c1:decimal(10,2),c2:decimal(10,4),c3:decimal(20,4),c4:decimal(20,4)>"));
    std::shared_ptr<Type> readType(
        Type::buildTypeFromString("struct<c1:boolean,c2:smallint,c3:int,c4:double>"));
    WriterOptions options;
    options.setUseTightNumericVector(true);
    auto writer = createWriter(*fileType, &memStream, options);
    auto batch = writer->createRowBatch(TEST_CASES);
    auto structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    auto& c1 = dynamic_cast<Decimal64VectorBatch&>(*structBatch->fields[0]);
    auto& c2 = dynamic_cast<Decimal64VectorBatch&>(*structBatch->fields[1]);
    auto& c3 = dynamic_cast<Decimal128VectorBatch&>(*structBatch->fields[2]);
    auto& c4 = dynamic_cast<Decimal128VectorBatch&>(*structBatch->fields[3]);

    for (uint32_t i = 0; i < TEST_CASES / 2; i++) {
      int64_t flag = i % 2 ? 1 : -1;
      c1.values[i] = flag * (!(i % 2) ? static_cast<int64_t>(0) : static_cast<int64_t>(i * 123));
      c2.values[i] = flag * (static_cast<int64_t>(i * 10000) + i);
      c3.values[i] = flag * (static_cast<int64_t>(i * 10000) + i);
      c4.values[i] = flag * (static_cast<int64_t>(i * 10000) + i);
    }
    for (uint32_t i = TEST_CASES / 2; i < TEST_CASES; i++) {
      c1.values[i] = 0;
      c2.values[i] = (static_cast<int64_t>(std::numeric_limits<int16_t>::max()) + i) * 10000 + i;
      c3.values[i] = (static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + i) * 10000 + i;
      c4.values[i] = 0;
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
    auto& readC1 = dynamic_cast<BooleanVectorBatch&>(*readStructBatch.fields[0]);
    auto& readC2 = dynamic_cast<ShortVectorBatch&>(*readStructBatch.fields[1]);
    auto& readC3 = dynamic_cast<IntVectorBatch&>(*readStructBatch.fields[2]);
    auto& readC4 = dynamic_cast<DoubleVectorBatch&>(*readStructBatch.fields[3]);
    EXPECT_EQ(TEST_CASES, readBatch->numElements);
    for (int i = 0; i < TEST_CASES / 2; i++) {
      size_t idx = static_cast<size_t>(i);
      EXPECT_TRUE(readC1.notNull[idx]) << i;
      EXPECT_TRUE(readC2.notNull[idx]) << i;
      EXPECT_TRUE(readC3.notNull[idx]) << i;
      EXPECT_TRUE(readC4.notNull[idx]) << i;

      int64_t flag = i % 2 ? 1 : -1;
      EXPECT_EQ(!(i % 2) ? 0 : 1, readC1.data[idx]) << i;
      EXPECT_EQ(flag * i, readC2.data[idx]) << i;
      EXPECT_EQ(flag * i, readC3.data[idx]) << i;
      EXPECT_DOUBLE_EQ(1.0001 * flag * i, readC4.data[idx]) << i;
    }
    for (int i = TEST_CASES / 2; i < TEST_CASES; i++) {
      size_t idx = static_cast<size_t>(i);
      EXPECT_TRUE(readC1.notNull[idx]) << i;
      EXPECT_FALSE(readC2.notNull[idx]) << i;
      EXPECT_FALSE(readC3.notNull[idx]) << i;
      EXPECT_TRUE(readC4.notNull[idx]) << i;
    }
  }

  TEST(ConvertColumnReader, TestConvertDecimalToDecimal) {
    constexpr int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;
    constexpr int TEST_CASES = 1024;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> fileType(Type::buildTypeFromString(
        "struct<c1:decimal(10,4),c2:decimal(10,4),c3:decimal(20,4),c4:decimal(20,4)>"));
    std::shared_ptr<Type> readType(Type::buildTypeFromString(
        "struct<c1:decimal(9,5),c2:decimal(20,5),c3:decimal(10,3),c4:decimal(19,3)>"));
    WriterOptions options;
    options.setUseTightNumericVector(true);
    auto writer = createWriter(*fileType, &memStream, options);
    auto batch = writer->createRowBatch(TEST_CASES);
    auto structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    auto& c1 = dynamic_cast<Decimal64VectorBatch&>(*structBatch->fields[0]);
    auto& c2 = dynamic_cast<Decimal64VectorBatch&>(*structBatch->fields[1]);
    auto& c3 = dynamic_cast<Decimal128VectorBatch&>(*structBatch->fields[2]);
    auto& c4 = dynamic_cast<Decimal128VectorBatch&>(*structBatch->fields[3]);

    for (uint32_t i = 0; i < TEST_CASES / 2; i++) {
      int64_t flag = i % 2 ? 1 : -1;
      c1.values[i] = flag * (static_cast<int64_t>(i * 10000) + i);
      c2.values[i] = flag * (static_cast<int64_t>(i * 10000) + i);
      c3.values[i] = flag * (static_cast<int64_t>(i * 10000) + i);
      c4.values[i] = flag * (static_cast<int64_t>(i * 10000) + i);
    }
    for (uint32_t i = TEST_CASES / 2; i < TEST_CASES; i++) {
      c1.values[i] = 100000000ll + i;
      c2.values[i] = 100000000ll + i;
      c3.values[i] = (Int128("100000000000") += i);
      c4.values[i] = (Int128("100000000000000000000") += i);
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
    auto& readC2 = dynamic_cast<Decimal128VectorBatch&>(*readStructBatch.fields[1]);
    auto& readC3 = dynamic_cast<Decimal64VectorBatch&>(*readStructBatch.fields[2]);
    auto& readC4 = dynamic_cast<Decimal128VectorBatch&>(*readStructBatch.fields[3]);
    EXPECT_EQ(TEST_CASES, readBatch->numElements);
    for (int i = 0; i < TEST_CASES / 2; i++) {
      size_t idx = static_cast<size_t>(i);
      EXPECT_TRUE(readC1.notNull[idx]) << i;
      EXPECT_TRUE(readC2.notNull[idx]) << i;
      EXPECT_TRUE(readC3.notNull[idx]) << i;
      EXPECT_TRUE(readC4.notNull[idx]) << i;

      int64_t flag = i % 2 ? 1 : -1;
      EXPECT_EQ(readC1.values[idx], flag * (i * 100000 + i * 10));
      EXPECT_EQ(readC2.values[idx].toLong(), flag * (i * 100000 + i * 10));
      EXPECT_EQ(readC3.values[idx], flag * (i * 1000 + (i + 5) / 10));
      EXPECT_EQ(readC4.values[idx].toLong(), flag * (i * 1000 + (i + 5) / 10));
    }
    for (int i = TEST_CASES / 2; i < TEST_CASES; i++) {
      size_t idx = static_cast<size_t>(i);
      EXPECT_FALSE(readC1.notNull[idx]) << i;
      EXPECT_TRUE(readC2.notNull[idx]) << i;
      EXPECT_FALSE(readC3.notNull[idx]) << i;
      EXPECT_FALSE(readC4.notNull[idx]) << i;
    }
  }

}  // namespace orc
