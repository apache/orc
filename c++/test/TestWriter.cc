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

#include "orc/ColumnPrinter.hh"
#include "orc/OrcFile.hh"

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

#include <cmath>
#include <ctime>
#include <sstream>

#ifdef __clang__
  DIAGNOSTIC_IGNORE("-Wmissing-variable-declarations")
#endif

namespace orc {

  using ::testing::TestWithParam;
  using ::testing::Values;

  const int DEFAULT_MEM_STREAM_SIZE = 100 * 1024 * 1024; // 100M

  std::unique_ptr<Writer> createWriter(
                                      uint64_t stripeSize,
                                      uint64_t compresionblockSize,
                                      CompressionKind compression,
                                      const Type& type,
                                      MemoryPool* memoryPool,
                                      OutputStream* stream,
                                      FileVersion version){
    WriterOptions options;
    options.setStripeSize(stripeSize);
    options.setCompressionBlockSize(compresionblockSize);
    options.setCompression(compression);
    options.setMemoryPool(memoryPool);
    options.setRowIndexStride(0);
    options.setFileVersion(version);
    return createWriter(type, stream, options);
  }

  std::unique_ptr<Reader> createReader(
                                      MemoryPool * memoryPool,
                                      std::unique_ptr<InputStream> stream) {
    ReaderOptions options;
    options.setMemoryPool(*memoryPool);
    return createReader(std::move(stream), options);
  }

  std::unique_ptr<RowReader> createRowReader(Reader* reader) {
    RowReaderOptions rowReaderOpts;
    return reader->createRowReader(rowReaderOpts);
  }

  class WriterTest : public TestWithParam<FileVersion> {
    // You can implement all the usual fixture class members here.
    // To access the test parameter, call GetParam() from class
    // TestWithParam<T>.
    virtual void SetUp();

    protected:
      FileVersion fileVersion;
    public:
      WriterTest(): fileVersion(FileVersion::v_0_11()) {}
  };

  void WriterTest::SetUp() {
    fileVersion = GetParam();
  }

  TEST_P(WriterTest, writeEmptyFile) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    ORC_UNIQUE_PTR<Type> type(Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 16 * 1024; // 16K
    uint64_t compressionBlockSize = 1024; // 1k

    std::unique_ptr<Writer> writer = createWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      CompressionKind_ZLIB,
                                      *type,
                                      pool,
                                      &memStream,
                                      fileVersion);
    writer->close();

    std::unique_ptr<InputStream> inStream(
            new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(
                                                pool,
                                                std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(fileVersion, reader->getFormatVersion());
    EXPECT_EQ(WriterVersion_ORC_135, reader->getWriterVersion());
    EXPECT_EQ(0, reader->getNumberOfRows());

    WriterId writerId = WriterId::ORC_CPP_WRITER;
    EXPECT_EQ(writerId, reader->getWriterId());
    EXPECT_EQ(1, reader->getWriterIdValue());

    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeIntFileOneStripe) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    ORC_UNIQUE_PTR<Type> type(Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 16 * 1024; // 16K
    uint64_t compressionBlockSize = 1024; // 1k

    std::unique_ptr<Writer> writer = createWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      CompressionKind_ZLIB,
                                      *type,
                                      pool,
                                      &memStream,
                                      fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(1024);
    StructVectorBatch* structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    LongVectorBatch* longBatch =
      dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);

    for (uint64_t i = 0; i < 1024; ++i) {
      longBatch->data[i] = static_cast<int64_t>(i);
    }
    structBatch->numElements = 1024;
    longBatch->numElements = 1024;

    writer->add(*batch);

    for (uint64_t i = 1024; i < 2000; ++i) {
      longBatch->data[i - 1024] = static_cast<int64_t>(i);
    }
    structBatch->numElements = 2000 - 1024;
    longBatch->numElements = 2000 - 1024;

    writer->add(*batch);
    writer->addUserMetadata("name0","value0");
    writer->addUserMetadata("name1","value1");
    writer->close();

    std::unique_ptr<InputStream> inStream(
            new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(
                                                pool,
                                                std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(2000, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(2048);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(2000, batch->numElements);
    EXPECT_FALSE(rowReader->next(*batch));

    std::list<std::string> keys = reader->getMetadataKeys();
    EXPECT_EQ(keys.size(), 2);
    std::list<std::string>::const_iterator itr = keys.begin();
    EXPECT_EQ(*itr, "name0");
    EXPECT_EQ(reader->getMetadataValue(*itr), "value0");
    itr++;
    EXPECT_EQ(*itr, "name1");
    EXPECT_EQ(reader->getMetadataValue(*itr), "value1");

    for (uint64_t i = 0; i < 2000; ++i) {
      structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
      longBatch = dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);
      EXPECT_EQ(i, longBatch->data[i]);
    }
  }

  TEST_P(WriterTest, writeIntFileMultipleStripes) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    ORC_UNIQUE_PTR<Type> type(Type::buildTypeFromString("struct<col1:int>"));

    uint64_t stripeSize = 1024; // 1K
    uint64_t compressionBlockSize = 1024; // 1k

    std::unique_ptr<Writer> writer = createWriter(
                                      stripeSize,
                                      compressionBlockSize,
                                      CompressionKind_ZLIB,
                                      *type,
                                      pool,
                                      &memStream,
                                      fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(65535);
    StructVectorBatch* structBatch =
      dynamic_cast<StructVectorBatch*>(batch.get());
    LongVectorBatch* longBatch =
      dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);

    for (uint64_t j = 0; j < 10; ++j) {
      for (uint64_t i = 0; i < 65535; ++i) {
        longBatch->data[i] = static_cast<int64_t>(i);
      }
      structBatch->numElements = 65535;
      longBatch->numElements = 65535;

      writer->add(*batch);
    }

    writer->close();

    std::unique_ptr<InputStream> inStream(
            new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(
                                                pool,
                                                std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(655350, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(65535);
    for (uint64_t j = 0; j < 10; ++j) {
      EXPECT_TRUE(rowReader->next(*batch));
      EXPECT_EQ(65535, batch->numElements);

      for (uint64_t i = 0; i < 65535; ++i) {
        structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
        longBatch = dynamic_cast<LongVectorBatch*>(structBatch->fields[0]);
        EXPECT_EQ(i, longBatch->data[i]);
      }
    }
    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeStringAndBinaryColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:string,col2:binary>"));

    uint64_t stripeSize = 1024;     // 1K
    uint64_t compressionBlockSize = 1024;      // 1k

    char dataBuffer[327675];
    uint64_t offset = 0;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(65535);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    StringVectorBatch * strBatch =
      dynamic_cast<StringVectorBatch *>(structBatch->fields[0]);
    StringVectorBatch * binBatch =
      dynamic_cast<StringVectorBatch *>(structBatch->fields[1]);

    for (uint64_t i = 0; i < 65535; ++i) {
      std::ostringstream os;
      os << i;
      strBatch->data[i] = dataBuffer + offset;
      strBatch->length[i] = static_cast<int64_t>(os.str().size());
      binBatch->data[i] = dataBuffer + offset;
      binBatch->length[i] = static_cast<int64_t>(os.str().size());
      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());
      offset += os.str().size();
    }

    structBatch->numElements = 65535;
    strBatch->numElements = 65535;
    binBatch->numElements = 65535;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(),
                             memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(65535, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(65535);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(65535, batch->numElements);

    for (uint64_t i = 0; i < 65535; ++i) {
      structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
      strBatch = dynamic_cast<StringVectorBatch *>(structBatch->fields[0]);
      binBatch = dynamic_cast<StringVectorBatch *>(structBatch->fields[1]);
      std::string str(
        strBatch->data[i],
        static_cast<size_t>(strBatch->length[i]));
      std::string bin(
        binBatch->data[i],
        static_cast<size_t>(binBatch->length[i]));
      EXPECT_EQ(i, static_cast<uint64_t>(atoi(str.c_str())));
      EXPECT_EQ(i, static_cast<uint64_t>(atoi(bin.c_str())));
    }

    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeFloatAndDoubleColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:double,col2:float>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 655350;

    std::vector<double> data(rowCount);
    for (uint64_t i = 0; i < rowCount; ++i) {
      data[i] = 100000 * (std::rand() * 1.0 / RAND_MAX);
    }

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    DoubleVectorBatch * doubleBatch =
      dynamic_cast<DoubleVectorBatch *>(structBatch->fields[0]);
    DoubleVectorBatch * floatBatch =
      dynamic_cast<DoubleVectorBatch *>(structBatch->fields[1]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      doubleBatch->data[i] = data[i];
      floatBatch->data[i] = data[i];
    }

    structBatch->numElements = rowCount;
    doubleBatch->numElements = rowCount;
    floatBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    doubleBatch = dynamic_cast<DoubleVectorBatch *>(structBatch->fields[0]);
    floatBatch = dynamic_cast<DoubleVectorBatch *>(structBatch->fields[1]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_TRUE(std::abs(data[i] - doubleBatch->data[i]) < 0.000001);
      EXPECT_TRUE(std::abs(static_cast<float>(data[i]) -
                           static_cast<float>(floatBatch->data[i])) < 0.000001f);
    }
    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeShortIntLong) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:smallint,col2:int,col3:bigint>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    LongVectorBatch * smallIntBatch =
      dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);
    LongVectorBatch * intBatch =
      dynamic_cast<LongVectorBatch *>(structBatch->fields[1]);
    LongVectorBatch * bigIntBatch =
      dynamic_cast<LongVectorBatch *>(structBatch->fields[2]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      smallIntBatch->data[i] = static_cast<int16_t>(i);
      intBatch->data[i] = static_cast<int32_t>(i);
      bigIntBatch->data[i] = static_cast<int64_t>(i);
    }
    structBatch->numElements = rowCount;
    smallIntBatch->numElements = rowCount;
    intBatch->numElements = rowCount;
    bigIntBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    smallIntBatch = dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);
    intBatch = dynamic_cast<LongVectorBatch *>(structBatch->fields[1]);
    bigIntBatch = dynamic_cast<LongVectorBatch *>(structBatch->fields[2]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int16_t>(i), smallIntBatch->data[i]);
      EXPECT_EQ(static_cast<int32_t>(i), intBatch->data[i]);
      EXPECT_EQ(static_cast<int64_t>(i), bigIntBatch->data[i]);
    }
  }

  TEST_P(WriterTest, writeTinyint) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:tinyint>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    LongVectorBatch * byteBatch =
      dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      byteBatch->data[i] = static_cast<int8_t>(i);
    }
    structBatch->numElements = rowCount;
    byteBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    byteBatch = dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int8_t>(i), static_cast<int8_t>(byteBatch->data[i]));
    }
  }

  TEST_P(WriterTest, writeBooleanColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:boolean>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    LongVectorBatch * byteBatch =
      dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      byteBatch->data[i] = (i % 3) == 0 ? 1 : 0;
    }
    structBatch->numElements = rowCount;
    byteBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    byteBatch = dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ((i % 3) == 0 ? 1 : 0, byteBatch->data[i]);
    }
  }

  TEST_P(WriterTest, writeDate) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:date>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 1024;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);

    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    LongVectorBatch * longBatch =
      dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      longBatch->data[i] = static_cast<int32_t>(i);
    }
    structBatch->numElements = rowCount;
    longBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    longBatch = dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int32_t>(i), longBatch->data[i]);
    }
  }

  TEST_P(WriterTest, writeTimestamp) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:timestamp>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 102400;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    TimestampVectorBatch * tsBatch =
      dynamic_cast<TimestampVectorBatch *>(structBatch->fields[0]);

    std::vector<std::time_t> times(rowCount);
    for (uint64_t i = 0; i < rowCount; ++i) {
      time_t currTime = -14210715; // 1969-07-20 12:34:45
      times[i] = static_cast<int64_t>(currTime) + static_cast<int64_t >(i * 3660);
      tsBatch->data[i] = times[i];
      tsBatch->nanoseconds[i] = static_cast<int64_t>(i * 1000);
    }
    structBatch->numElements = rowCount;
    tsBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    tsBatch = dynamic_cast<TimestampVectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(times[i], tsBatch->data[i]);
      EXPECT_EQ(i * 1000, tsBatch->nanoseconds[i]);
    }
  }

  TEST_P(WriterTest, writeCharAndVarcharColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:char(3),col2:varchar(4)>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 65535;

    char dataBuffer[327675];
    uint64_t offset = 0;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);

    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    StringVectorBatch * charBatch =
      dynamic_cast<StringVectorBatch *>(structBatch->fields[0]);
    StringVectorBatch * varcharBatch =
      dynamic_cast<StringVectorBatch *>(structBatch->fields[1]);

    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      charBatch->data[i] = dataBuffer + offset;
      charBatch->length[i] = static_cast<int64_t>(os.str().size());

      varcharBatch->data[i] = charBatch->data[i];
      varcharBatch->length[i] = charBatch->length[i];

      memcpy(dataBuffer + offset, os.str().c_str(), os.str().size());
      offset += os.str().size();
    }

    structBatch->numElements = rowCount;
    charBatch->numElements = rowCount;
    varcharBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    for (uint64_t i = 0; i < rowCount; ++i) {
      structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
      charBatch = dynamic_cast<StringVectorBatch *>(structBatch->fields[0]);
      varcharBatch = dynamic_cast<StringVectorBatch *>(structBatch->fields[1]);

      EXPECT_EQ(3, charBatch->length[i]);
      EXPECT_FALSE(varcharBatch->length[i] > 4);

      // test char data
      std::string charsRead(
        charBatch->data[i],
        static_cast<size_t>(charBatch->length[i]));

      std::ostringstream os;
      os << i;
      std::string charsExpected = os.str().substr(0, 3);
      while (charsExpected.length() < 3) {
        charsExpected += ' ';
      }
      EXPECT_EQ(charsExpected, charsRead);

      // test varchar data
      std::string varcharRead(
        varcharBatch->data[i],
        static_cast<size_t>(varcharBatch->length[i]));
      std::string varcharExpected = os.str().substr(0, 4);
      EXPECT_EQ(varcharRead, varcharExpected);
    }

    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST_P(WriterTest, writeDecimal64Column) {
    const uint64_t maxPrecision = 18;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:decimal(18,5)>"));

    uint64_t stripeSize = 16 * 1024; // 16K
    uint64_t compressionBlockSize = 1024; // 1k
    uint64_t rowCount = 1024;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    Decimal64VectorBatch * decBatch =
      dynamic_cast<Decimal64VectorBatch *>(structBatch->fields[0]);

    // write positive decimals
    for (uint64_t i = 0; i < rowCount; ++i) {
      decBatch->values[i] = static_cast<int64_t>(i + 10000);
    }
    structBatch->numElements = decBatch->numElements = rowCount;
    writer->add(*batch);

    // write negative decimals
    for (uint64_t i = 0; i < rowCount; ++i) {
      decBatch->values[i] = static_cast<int64_t>(i - 10000);
    }
    structBatch->numElements = decBatch->numElements = rowCount;
    writer->add(*batch);

    // write all precision decimals
    int64_t dec;
    for (uint64_t i = dec = 0; i < maxPrecision; ++i) {
      dec = dec * 10 + 9;
      decBatch->values[i] = dec;
      decBatch->values[i + maxPrecision] = -dec;
    }
    structBatch->numElements = decBatch->numElements = 2 * maxPrecision;
    writer->add(*batch);

    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ((rowCount + maxPrecision) * 2, reader->getNumberOfRows());

    // test reading positive decimals
    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    decBatch = dynamic_cast<Decimal64VectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int64_t>(i + 10000), decBatch->values[i]);
    }

    // test reading negative decimals
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    decBatch = dynamic_cast<Decimal64VectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      EXPECT_EQ(static_cast<int64_t>(i - 10000), decBatch->values[i]);
    }

    // test reading all precision decimals
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    decBatch = dynamic_cast<Decimal64VectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = dec = 0; i < maxPrecision; ++i) {
      dec = dec * 10 + 9;
      EXPECT_EQ(dec, decBatch->values[i]);
      EXPECT_EQ(-dec, decBatch->values[i + maxPrecision]);
    }
  }

  TEST_P(WriterTest, writeDecimal128Column) {
    const uint64_t maxPrecision = 38;
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:decimal(38,10)>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 1024;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    Decimal128VectorBatch * decBatch =
      dynamic_cast<Decimal128VectorBatch *>(structBatch->fields[0]);

    // write positive decimals
    std::string base = "1" + std::string(1, '0');
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      decBatch->values[i] = Int128(base + os.str());
    }
    structBatch->numElements = decBatch->numElements = rowCount;
    writer->add(*batch);

    // write negative decimals
    std::string nbase = "-" + base;
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      decBatch->values[i] = Int128(nbase + os.str());
    }
    structBatch->numElements = rowCount;
    decBatch->numElements = rowCount;
    writer->add(*batch);

    // write all precision decimals
    for (uint64_t i = 0; i < maxPrecision; ++i) {
      std::string expected = std::string(i + 1, '9');
      decBatch->values[i] = Int128(expected);
      decBatch->values[i + maxPrecision] = Int128("-" + expected);
    }
    structBatch->numElements = decBatch->numElements = 2 * maxPrecision;
    writer->add(*batch);

    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ((rowCount + maxPrecision) * 2, reader->getNumberOfRows());

    // test reading positive decimals
    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    decBatch = dynamic_cast<Decimal128VectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      EXPECT_EQ(base + os.str(), decBatch->values[i].toString());
    }

    // test reading negative decimals
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    decBatch = dynamic_cast<Decimal128VectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < rowCount; ++i) {
      std::ostringstream os;
      os << i;
      EXPECT_EQ(nbase + os.str(), decBatch->values[i].toString());
    }

    // test reading all precision decimals
    EXPECT_EQ(true, rowReader->next(*batch));
    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    decBatch = dynamic_cast<Decimal128VectorBatch *>(structBatch->fields[0]);
    for (uint64_t i = 0; i < maxPrecision; ++i) {
      std::string expected = std::string(i + 1, '9');
      EXPECT_EQ(expected, decBatch->values[i].toString());
      EXPECT_EQ("-" + expected, decBatch->values[i + maxPrecision].toString());
    }
  }

  TEST_P(WriterTest, writeListColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();

    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:array<int>>"));

    uint64_t stripeSize = 1024 * 1024;
    uint64_t compressionBlockSize = 64 * 1024;
    uint64_t rowCount = 1024;
    uint64_t maxListLength = 10;
    uint64_t offset = 0;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch =
      writer->createRowBatch(rowCount * maxListLength);

    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    ListVectorBatch * listBatch =
      dynamic_cast<ListVectorBatch *>(structBatch->fields[0]);
    LongVectorBatch * intBatch =
      dynamic_cast<LongVectorBatch *>(listBatch->elements.get());
    int64_t * data = intBatch->data.data();
    int64_t * offsets = listBatch->offsets.data();

    for (uint64_t i = 0; i < rowCount; ++i) {
      offsets[i] = static_cast<int64_t>(offset);
      for (uint64_t length = i % maxListLength + 1; length != 0; --length) {
        data[offset++] = static_cast<int64_t >(i);
      }
    }
    offsets[rowCount] = static_cast<int64_t>(offset);

    structBatch->numElements = rowCount;
    listBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount * maxListLength);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    listBatch = dynamic_cast<ListVectorBatch *>(structBatch->fields[0]);
    intBatch = dynamic_cast<LongVectorBatch *>(listBatch->elements.get());
    data = intBatch->data.data();
    offsets = listBatch->offsets.data();

    EXPECT_EQ(rowCount, listBatch->numElements);
    EXPECT_EQ(offset, intBatch->numElements);

    for (uint64_t i = 0; i < rowCount; ++i) {
      uint64_t length = i % maxListLength + 1;
      for (int64_t j = 0; j != length; ++j) {
        EXPECT_EQ(static_cast<int64_t>(i), data[offsets[i] + j]);
      }
    }
  }

  TEST_P(WriterTest, writeMapColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();
    std::unique_ptr<Type> type(
      Type::buildTypeFromString("struct<col1:map<string,int>>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 1024, maxListLength = 10, offset = 0;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch =
      writer->createRowBatch(rowCount * maxListLength);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    MapVectorBatch * mapBatch =
      dynamic_cast<MapVectorBatch *>(structBatch->fields[0]);
    StringVectorBatch * keyBatch =
      dynamic_cast<StringVectorBatch *>(mapBatch->keys.get());
    LongVectorBatch * elemBatch =
      dynamic_cast<LongVectorBatch *>(mapBatch->elements.get());

    char dataBuffer[327675]; // 300k
    uint64_t strOffset = 0;

    int64_t * offsets = mapBatch->offsets.data();
    char ** keyData = keyBatch->data.data();
    int64_t * keyLength = keyBatch->length.data();
    int64_t * elemData = elemBatch->data.data();

    for (uint64_t i = 0; i < rowCount; ++i) {
      offsets[i] = static_cast<int64_t>(offset);
      for (uint64_t j = 0; j != i % maxListLength + 1; ++j) {
        std::ostringstream os;
        os << (i + j);
        memcpy(dataBuffer + strOffset, os.str().c_str(), os.str().size());
        keyData[offset] = dataBuffer + strOffset;

        keyLength[offset] = static_cast<int64_t>(os.str().size());
        elemData[offset++] = static_cast<int64_t>(i);

        strOffset += os.str().size();
      }
    }
    offsets[rowCount] = static_cast<int64_t>(offset);

    structBatch->numElements = rowCount;
    mapBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(
      pool,
      std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount * maxListLength);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    mapBatch = dynamic_cast<MapVectorBatch *>(structBatch->fields[0]);
    keyBatch = dynamic_cast<StringVectorBatch *>(mapBatch->keys.get());
    elemBatch = dynamic_cast<LongVectorBatch *>(mapBatch->elements.get());
    offsets = mapBatch->offsets.data();
    keyData = keyBatch->data.data();
    keyLength = keyBatch->length.data();
    elemData = elemBatch->data.data();

    EXPECT_EQ(rowCount, mapBatch->numElements);
    EXPECT_EQ(offset, keyBatch->numElements);
    EXPECT_EQ(offset, elemBatch->numElements);

    for (uint64_t i = 0; i != rowCount; ++i) {
      for (int64_t j = 0; j != i % maxListLength + 1; ++j) {
        std::ostringstream os;
        os << i + static_cast<uint64_t>(j);
        uint64_t lenRead = static_cast<uint64_t>(keyLength[offsets[i] + j]);
        EXPECT_EQ(os.str(), std::string(keyData[offsets[i] + j], lenRead));
        EXPECT_EQ(static_cast<int64_t>(i), elemData[offsets[i] + j]);
      }
    }
  }

  TEST_P(WriterTest, writeUnionColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:uniontype<int,double,boolean>>"));

    uint64_t stripeSize = 16 * 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 3333;

    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    UnionVectorBatch * unionBatch =
      dynamic_cast<UnionVectorBatch *>(structBatch->fields[0]);
    unsigned char * tags = unionBatch->tags.data();
    uint64_t * offsets = unionBatch->offsets.data();

    LongVectorBatch * intBatch =
      dynamic_cast<LongVectorBatch *>(unionBatch->children[0]);
    DoubleVectorBatch * doubleBatch =
      dynamic_cast<DoubleVectorBatch *>(unionBatch->children[1]);
    LongVectorBatch * boolBatch =
      dynamic_cast<LongVectorBatch *>(unionBatch->children[2]);
    int64_t *intData = intBatch->data.data();
    double *doubleData = doubleBatch->data.data();
    int64_t *boolData = boolBatch->data.data();

    uint64_t intOffset = 0, doubleOffset = 0, boolOffset = 0, tag = 0;
    for (uint64_t i = 0; i < rowCount; ++i) {
      tags[i] = static_cast<unsigned char>(tag);
      switch(tag) {
        case 0:
          offsets[i] = intOffset;
          intData[intOffset++] = static_cast<int64_t>(i);
          break;
        case 1:
          offsets[i] = doubleOffset;
          doubleData[doubleOffset++] = static_cast<double>(i) + 0.5;
          break;
        case 2:
          offsets[i] = boolOffset;
          boolData[boolOffset++] = (i % 2 == 0) ? 1 : 0;
          break;
      }
      tag = (tag + 1) % 3;
    }

    structBatch->numElements = rowCount;
    unionBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(
      pool,
      std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    EXPECT_EQ(true, rowReader->next(*batch));

    structBatch =dynamic_cast<StructVectorBatch *>(batch.get());
    unionBatch =dynamic_cast<UnionVectorBatch *>(structBatch->fields[0]);
    tags = unionBatch->tags.data();
    offsets = unionBatch->offsets.data();

    intBatch =  dynamic_cast<LongVectorBatch *>(unionBatch->children[0]);
    doubleBatch = dynamic_cast<DoubleVectorBatch *>(unionBatch->children[1]);
    boolBatch = dynamic_cast<LongVectorBatch *>(unionBatch->children[2]);
    intData = intBatch->data.data();
    doubleData = doubleBatch->data.data();
    boolData = boolBatch->data.data();

    EXPECT_EQ(rowCount, unionBatch->numElements);
    EXPECT_EQ(rowCount / 3, intBatch->numElements);
    EXPECT_EQ(rowCount / 3, doubleBatch->numElements);
    EXPECT_EQ(rowCount / 3, boolBatch->numElements);

    uint64_t offset;
    for (uint64_t i = 0; i < rowCount; ++i) {
      tag = tags[i];
      offset = offsets[i];

      switch(tag) {
        case 0:
          EXPECT_EQ(i, intData[offset]);
          break;
        case 1:
          EXPECT_TRUE(
            std::abs(static_cast<double>(i) + 0.5 - doubleData[offset]) < 0.000001);
          break;
        case 2:
          EXPECT_EQ(i % 2 == 0 ? 1 : 0, boolData[offset]);
          break;
      }
    }
  }

  TEST_P(WriterTest, writeUTF8CharAndVarcharColumn) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();
    std::unique_ptr<Type> type(Type::buildTypeFromString(
      "struct<col1:char(2),col2:varchar(2)>"));

    uint64_t stripeSize = 1024;
    uint64_t compressionBlockSize = 1024;
    uint64_t rowCount = 3;
    std::unique_ptr<Writer> writer = createWriter(stripeSize,
                                                  compressionBlockSize,
                                                  CompressionKind_ZLIB,
                                                  *type,
                                                  pool,
                                                  &memStream,
                                                  fileVersion);
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(rowCount);
    StructVectorBatch * structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    StringVectorBatch * charBatch =
      dynamic_cast<StringVectorBatch *>(structBatch->fields[0]);
    StringVectorBatch * varcharBatch =
      dynamic_cast<StringVectorBatch *>(structBatch->fields[1]);
    std::vector<std::vector<char>> strs;

    // input character is 'à' (0xC3, 0xA0)
    // in total 3 rows, each has 1, 2, and 3 'à' respectively
    std::vector<char> vec;
    for (uint64_t i = 0; i != rowCount; ++i) {
      vec.push_back(static_cast<char>(0xC3));
      vec.push_back(static_cast<char>(0xA0));
      strs.push_back(vec);
      charBatch->data[i] = varcharBatch->data[i] = strs.back().data();
      charBatch->length[i] = varcharBatch->length[i] = static_cast<int64_t>(strs.back().size());
    }

    structBatch->numElements = rowCount;
    charBatch->numElements = rowCount;
    varcharBatch->numElements = rowCount;

    writer->add(*batch);
    writer->close();

    // read and verify data
    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(pool, std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ(rowCount, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(rowCount);
    structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
    charBatch = dynamic_cast<StringVectorBatch *>(structBatch->fields[0]);
    varcharBatch = dynamic_cast<StringVectorBatch *>(structBatch->fields[1]);

    EXPECT_EQ(true, rowReader->next(*batch));
    EXPECT_EQ(rowCount, batch->numElements);

    char expectedPadded[3] = {static_cast<char>(0xC3), static_cast<char>(0xA0), ' '};
    char expectedOneChar[2] = {static_cast<char>(0xC3), static_cast<char>(0xA0)};
    char expectedTwoChars[4] = {static_cast<char>(0xC3), static_cast<char>(0xA0),
                                static_cast<char>(0xC3), static_cast<char>(0xA0)};

    EXPECT_EQ(3, charBatch->length[0]);
    EXPECT_EQ(4, charBatch->length[1]);
    EXPECT_EQ(4, charBatch->length[2]);
    EXPECT_TRUE(memcmp(charBatch->data[0], expectedPadded, 3) == 0);
    EXPECT_TRUE(memcmp(charBatch->data[1], expectedTwoChars, 4) == 0);
    EXPECT_TRUE(memcmp(charBatch->data[2], expectedTwoChars, 4) == 0);

    EXPECT_EQ(2, varcharBatch->length[0]);
    EXPECT_EQ(4, varcharBatch->length[1]);
    EXPECT_EQ(4, varcharBatch->length[2]);
    EXPECT_TRUE(memcmp(varcharBatch->data[0], expectedOneChar, 2) == 0);
    EXPECT_TRUE(memcmp(varcharBatch->data[1], expectedTwoChars, 4) == 0);
    EXPECT_TRUE(memcmp(varcharBatch->data[2], expectedTwoChars, 4) == 0);

    EXPECT_FALSE(rowReader->next(*batch));
  }


  INSTANTIATE_TEST_CASE_P(OrcTest, WriterTest, Values(FileVersion::v_0_11(), FileVersion::v_0_12()));
}
