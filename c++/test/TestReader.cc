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

#include "orc/OrcFile.hh"

#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestReader, testWriterVersions) {
    EXPECT_EQ("original", writerVersionToString(WriterVersion_ORIGINAL));
    EXPECT_EQ("HIVE-8732", writerVersionToString(WriterVersion_HIVE_8732));
    EXPECT_EQ("HIVE-4243", writerVersionToString(WriterVersion_HIVE_4243));
    EXPECT_EQ("HIVE-12055", writerVersionToString(WriterVersion_HIVE_12055));
    EXPECT_EQ("HIVE-13083", writerVersionToString(WriterVersion_HIVE_13083));
    EXPECT_EQ("future - 99",
              writerVersionToString(static_cast<WriterVersion>(99)));
  }

  TEST(TestReader, testCompressionNames) {
    EXPECT_EQ("none", compressionKindToString(CompressionKind_NONE));
    EXPECT_EQ("zlib", compressionKindToString(CompressionKind_ZLIB));
    EXPECT_EQ("snappy", compressionKindToString(CompressionKind_SNAPPY));
    EXPECT_EQ("lzo", compressionKindToString(CompressionKind_LZO));
    EXPECT_EQ("lz4", compressionKindToString(CompressionKind_LZ4));
    EXPECT_EQ("zstd", compressionKindToString(CompressionKind_ZSTD));
    EXPECT_EQ("unknown - 99",
              compressionKindToString(static_cast<CompressionKind>(99)));
  }

  TEST(TestReader, testGetRowNumber) {
    const int DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024; // 10M
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    std::unique_ptr<Type> type(Type::buildTypeFromString("struct<col1:int>"));
    std::unique_ptr<Writer> writer = createWriter(*type,
                                                  &memStream,
                                                  WriterOptions());
    size_t batchSize = 1024;
    std::unique_ptr<ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
    StructVectorBatch* structBatch =
      dynamic_cast<StructVectorBatch *>(batch.get());
    LongVectorBatch* longBatch =
      dynamic_cast<LongVectorBatch *>(structBatch->fields[0]);

    for (size_t i = 0; i < batchSize; ++i) {
      longBatch->data[i] = static_cast<int64_t>(i);
    }
    structBatch->numElements = batchSize;
    longBatch->numElements = batchSize;

    writer->add(*batch);
    writer->close();

    std::unique_ptr<InputStream> inStream(
      new MemoryInputStream(memStream.getData(), memStream.getLength()));
    std::unique_ptr<Reader> reader = createReader(std::move(inStream),
                                                  ReaderOptions());
    std::unique_ptr<RowReader> rowReader = reader->createRowReader();
    EXPECT_EQ(batchSize, reader->getNumberOfRows());

    EXPECT_EQ(-1, rowReader->getRowNumber());

    batch = rowReader->createRowBatch(10);
    rowReader->next(*batch);
    EXPECT_EQ(9, rowReader->getRowNumber());

    rowReader->next(*batch);
    EXPECT_EQ(19, rowReader->getRowNumber());

    rowReader->next(*batch);
    EXPECT_EQ(29, rowReader->getRowNumber());

  }

}  // namespace
