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

namespace orc {

  const int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024; // 10M

  std::unique_ptr<Writer> createWriter(
                                      uint64_t stripeSize,
                                      uint64_t compresionblockSize,
                                      CompressionKind compression,
                                      const Type& type,
                                      MemoryPool* memoryPool,
                                      OutputStream* stream,
                                      FileVersion version = FileVersion(0, 12)){
    WriterOptions options;
    options.setStripeSize(stripeSize);
    options.setCompressionBlockSize(compresionblockSize);
    options.setCompression(compression);
    options.setMemoryPool(memoryPool);
    options.setEnableStats(false);
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

  TEST(Writer, writeEmptyFile) {
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
                                      &memStream);
    writer->close();

    std::unique_ptr<InputStream> inStream(
            new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(
                                                pool,
                                                std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ("0.12", reader->getFormatVersion());
    EXPECT_EQ(WriterVersion_ORC_135, reader->getWriterVersion());
    EXPECT_EQ(0, reader->getNumberOfRows());

    std::unique_ptr<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
    EXPECT_FALSE(rowReader->next(*batch));
  }

  TEST(Writer, writeIntFileOneStripe) {
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
                                      FileVersion(0, 11));
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
    writer->close();

    std::unique_ptr<InputStream> inStream(
            new MemoryInputStream (memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(
                                                pool,
                                                std::move(inStream));
    std::unique_ptr<RowReader> rowReader = createRowReader(reader.get());
    EXPECT_EQ("0.11", reader->getFormatVersion());
    EXPECT_EQ(2000, reader->getNumberOfRows());

    batch = rowReader->createRowBatch(2048);
    EXPECT_TRUE(rowReader->next(*batch));
    EXPECT_EQ(2000, batch->numElements);
    EXPECT_FALSE(rowReader->next(*batch));

    for (uint64_t i = 0; i < 2000; ++i) {
      structBatch = dynamic_cast<StructVectorBatch *>(batch.get());
      longBatch = dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);
      EXPECT_EQ(i, longBatch->data[i]);
    }
  }

  TEST(Writer, writeIntFileMultipleStripes) {
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
                                      &memStream);
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
}

