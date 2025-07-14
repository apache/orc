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
#include "TestUtil.hh"

#include "wrap/gtest-wrapper.h"

#include <cstdint>
#include <memory>
#include <utility>

namespace orc {

#define ENSURE_DYNAMIC_CAST_NOT_NULL(PTR) \
  if (PTR == NULL) throw std::logic_error("dynamic_cast returns null");

  const int DEFAULT_MEM_STREAM_SIZE = 1024 * 1024;  // 1M

  static std::unique_ptr<Writer> createWriter(uint64_t stripeSize, const Type& type,
                                              MemoryPool* memoryPool, OutputStream* stream) {
    WriterOptions options;
    options.setStripeSize(stripeSize);
    options.setCompressionBlockSize(256);
    options.setMemoryBlockSize(256);
    options.setCompression(CompressionKind_ZLIB);
    options.setMemoryPool(memoryPool);
    options.setRowIndexStride(10);
    return createWriter(type, stream, options);
  }

  static std::unique_ptr<Reader> createReader(MemoryPool* memoryPool,
                                              MemoryOutputStream& memStream) {
    std::unique_ptr<InputStream> inStream(
        new MemoryInputStream(memStream.getData(), memStream.getLength()));
    ReaderOptions options;
    options.setMemoryPool(*memoryPool);
    return createReader(std::move(inStream), options);
  }

  TEST(Statistics, geometryStatsWithNull) {
    std::unique_ptr<Type> const type(Type::buildTypeFromString("struct<col1:geometry(OGC:CRS84)>"));

    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* const pool = getDefaultPool();
    uint64_t const stripeSize = 32;  // small stripe size to garantee multi stripes
    std::unique_ptr<Writer> writer = createWriter(stripeSize, *type, pool, &memStream);

    uint64_t const batchCount = 1000;
    uint64_t const batches = 10;
    std::unique_ptr<ColumnVectorBatch> const batch = writer->createRowBatch(batchCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    ENSURE_DYNAMIC_CAST_NOT_NULL(structBatch);

    StringVectorBatch* strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
    ENSURE_DYNAMIC_CAST_NOT_NULL(strBatch);

    // create str values
    std::vector<std::string> wkbs;
    std::array<double, 4> mins = {geospatial::INF, geospatial::INF, geospatial::INF,
                                  geospatial::INF};
    std::array<double, 4> maxs = {-geospatial::INF, -geospatial::INF, -geospatial::INF,
                                  -geospatial::INF};
    for (uint64_t i = 1; i < batchCount - 1; ++i) {
      if (i % 3 == 0) {
        wkbs.push_back(MakeWKBPoint({i * 1.0, i * 1.0}, false, false));
        mins[0] = std::min(mins[0], i * 1.0);
        maxs[0] = std::max(maxs[0], i * 1.0);
        mins[1] = std::min(mins[1], i * 1.0);
        maxs[1] = std::max(maxs[1], i * 1.0);
      } else if (i % 3 == 1) {
        wkbs.push_back(MakeWKBPoint({i * 1.0, i * 1.0, i * 1.0}, true, false));
        mins[0] = std::min(mins[0], i * 1.0);
        maxs[0] = std::max(maxs[0], i * 1.0);
        mins[1] = std::min(mins[1], i * 1.0);
        maxs[1] = std::max(maxs[1], i * 1.0);
        mins[2] = std::min(mins[2], i * 1.0);
        maxs[2] = std::max(maxs[2], i * 1.0);
      } else if (i % 3 == 2) {
        wkbs.push_back(MakeWKBPoint({i * 1.0, i * 1.0, i * 1.0, i * 1.0}, true, true));
        mins[0] = std::min(mins[0], i * 1.0);
        maxs[0] = std::max(maxs[0], i * 1.0);
        mins[1] = std::min(mins[1], i * 1.0);
        maxs[1] = std::max(maxs[1], i * 1.0);
        mins[2] = std::min(mins[2], i * 1.0);
        maxs[2] = std::max(maxs[2], i * 1.0);
        mins[3] = std::min(mins[3], i * 1.0);
        maxs[3] = std::max(maxs[3], i * 1.0);
      }
    }
    for (uint64_t i = 1; i < batchCount - 1; ++i) {
      strBatch->data[i] = const_cast<char*>(wkbs[i - 1].c_str());
      strBatch->length[i] = static_cast<int32_t>(wkbs[i - 1].length());
    }

    structBatch->numElements = batchCount;
    strBatch->numElements = batchCount;

    structBatch->hasNulls = true;
    structBatch->notNull[0] = '\0';
    structBatch->notNull[batchCount - 1] = '\0';
    strBatch->hasNulls = true;
    strBatch->notNull[0] = '\0';
    strBatch->notNull[batchCount - 1] = '\0';

    for (uint64_t i = 0; i < batches; ++i) {
      writer->add(*batch.get());
    }
    writer->close();

    std::unique_ptr<Reader> reader = createReader(pool, memStream);

    // check column 1 (string) file stats
    auto stats1 = reader->getColumnStatistics(1);
    const GeospatialColumnStatistics* geoFileStats =
        dynamic_cast<const GeospatialColumnStatistics*>(stats1.get());
    ENSURE_DYNAMIC_CAST_NOT_NULL(geoFileStats);
    EXPECT_EQ(geoFileStats->getGeospatialTypes().size(), 3);
    EXPECT_EQ(geoFileStats->getGeospatialTypes()[0], 1);
    EXPECT_EQ(geoFileStats->getGeospatialTypes()[1], 1001);
    EXPECT_EQ(geoFileStats->getGeospatialTypes()[2], 3001);
    std::array<bool, 4> expectValid = {true, true, true, true};
    std::array<bool, 4> expectEmpty = {false, false, false, false};
    EXPECT_EQ(geoFileStats->getBoundingBox().dimensionValid(), expectValid);
    EXPECT_EQ(geoFileStats->getBoundingBox().dimensionEmpty(), expectEmpty);
    EXPECT_EQ(geoFileStats->getBoundingBox().lowerBound(), mins);
    EXPECT_EQ(geoFileStats->getBoundingBox().upperBound(), maxs);
  }

  TEST(Statistics, geographyStatsWithNull) {
    std::unique_ptr<Type> const type(
        Type::buildTypeFromString("struct<col1:geography(OGC:CRS84,speherial)>"));

    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool* const pool = getDefaultPool();
    uint64_t const stripeSize = 32;  // small stripe size to garantee multi stripes
    std::unique_ptr<Writer> writer = createWriter(stripeSize, *type, pool, &memStream);

    uint64_t const batchCount = 1000;
    uint64_t const batches = 10;
    std::unique_ptr<ColumnVectorBatch> const batch = writer->createRowBatch(batchCount);
    StructVectorBatch* structBatch = dynamic_cast<StructVectorBatch*>(batch.get());
    ENSURE_DYNAMIC_CAST_NOT_NULL(structBatch);

    StringVectorBatch* strBatch = dynamic_cast<StringVectorBatch*>(structBatch->fields[0]);
    ENSURE_DYNAMIC_CAST_NOT_NULL(strBatch);

    // create str values
    std::vector<std::string> wkbs;
    std::array<double, 4> mins = {geospatial::INF, geospatial::INF, geospatial::INF,
                                  geospatial::INF};
    std::array<double, 4> maxs = {-geospatial::INF, -geospatial::INF, -geospatial::INF,
                                  -geospatial::INF};
    for (uint64_t i = 1; i < batchCount - 1; ++i) {
      if (i % 3 == 0) {
        wkbs.push_back(MakeWKBPoint({i * 1.0, i * 1.0}, false, false));
        mins[0] = std::min(mins[0], i * 1.0);
        maxs[0] = std::max(maxs[0], i * 1.0);
        mins[1] = std::min(mins[1], i * 1.0);
        maxs[1] = std::max(maxs[1], i * 1.0);
      } else if (i % 3 == 1) {
        wkbs.push_back(MakeWKBPoint({i * 1.0, i * 1.0, i * 1.0}, true, false));
        mins[0] = std::min(mins[0], i * 1.0);
        maxs[0] = std::max(maxs[0], i * 1.0);
        mins[1] = std::min(mins[1], i * 1.0);
        maxs[1] = std::max(maxs[1], i * 1.0);
        mins[2] = std::min(mins[2], i * 1.0);
        maxs[2] = std::max(maxs[2], i * 1.0);
      } else if (i % 3 == 2) {
        wkbs.push_back(MakeWKBPoint({i * 1.0, i * 1.0, i * 1.0, i * 1.0}, true, true));
        mins[0] = std::min(mins[0], i * 1.0);
        maxs[0] = std::max(maxs[0], i * 1.0);
        mins[1] = std::min(mins[1], i * 1.0);
        maxs[1] = std::max(maxs[1], i * 1.0);
        mins[2] = std::min(mins[2], i * 1.0);
        maxs[2] = std::max(maxs[2], i * 1.0);
        mins[3] = std::min(mins[3], i * 1.0);
        maxs[3] = std::max(maxs[3], i * 1.0);
      }
    }
    for (uint64_t i = 1; i < batchCount - 1; ++i) {
      strBatch->data[i] = const_cast<char*>(wkbs[i - 1].c_str());
      strBatch->length[i] = static_cast<int32_t>(wkbs[i - 1].length());
    }

    structBatch->numElements = batchCount;
    strBatch->numElements = batchCount;

    structBatch->hasNulls = true;
    structBatch->notNull[0] = '\0';
    structBatch->notNull[batchCount - 1] = '\0';
    strBatch->hasNulls = true;
    strBatch->notNull[0] = '\0';
    strBatch->notNull[batchCount - 1] = '\0';

    for (uint64_t i = 0; i < batches; ++i) {
      writer->add(*batch.get());
    }
    writer->close();

    std::unique_ptr<Reader> reader = createReader(pool, memStream);

    // check column 1 (string) file stats
    auto stats1 = reader->getColumnStatistics(1);
    const GeospatialColumnStatistics* geoFileStats =
        dynamic_cast<const GeospatialColumnStatistics*>(stats1.get());
    ENSURE_DYNAMIC_CAST_NOT_NULL(geoFileStats);
    EXPECT_EQ(geoFileStats->getGeospatialTypes().size(), 0);
    std::array<bool, 4> expectValid = {false, false, false, false};
    EXPECT_EQ(geoFileStats->getBoundingBox().dimensionValid(), expectValid);
  }
}  // namespace orc