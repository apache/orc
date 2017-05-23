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

#include <cstdlib>

#include "MemoryOutputStream.hh"
#include "RLEv1.hh"

#include "wrap/orc-proto-wrapper.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

  const int DEFAULT_MEM_STREAM_SIZE = 1024 * 1024; // 1M

  void generateData(
                         uint64_t numValues,
                         int64_t start,
                         int64_t delta,
                         bool random,
                         int64_t* data,
                         uint64_t numNulls = 0,
                         char* notNull = nullptr) {
    if (numNulls != 0 && notNull != nullptr) {
      memset(notNull, 1, numValues);
      while (numNulls > 0) {
        uint64_t pos = static_cast<uint64_t>(std::rand()) % numValues;
        if (notNull[pos]) {
          notNull[pos] = static_cast<char>(0);
          --numNulls;
        }
      }
    }

    for (uint64_t i = 0; i < numValues; ++i) {
      if (notNull == nullptr || notNull[i])
      {
        if (!random) {
          data[i] = start + delta * static_cast<int64_t>(i);
        } else {
          data[i] = std::rand();
        }
      }
    }
  }

  void decodeAndVerify(
                       const MemoryOutputStream& memStream,
                       int64_t * data,
                       uint64_t numValues,
                       const char* notNull,
                       bool isSinged) {
    RleDecoderV1 decoder(
      std::unique_ptr<SeekableInputStream>(
        new SeekableArrayInputStream(
                                    memStream.getData(),
                                    memStream.getLength())),
        isSinged);

    int64_t* decodedData = new int64_t[numValues];
    decoder.next(decodedData, numValues, notNull);

    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        EXPECT_EQ(data[i], decodedData[i]);
      }
    }

    delete [] decodedData;
  }

  TEST(RleEncoderV1, delta_increasing_sequance_unsigned) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block);

    RleEncoderV1 encoder(
      std::unique_ptr<BufferedOutputStream>(
              new BufferedOutputStream(*pool, &memStream, capacity, block)),
          false);

    int64_t* data = new int64_t[1024];
    generateData(1024, 0, 1, false, data);
    encoder.add(data, 1024, nullptr);
    encoder.flush();

    decodeAndVerify(memStream, data, 1024, nullptr, false);
    delete [] data;
  }

  TEST(RleEncoderV1, delta_increasing_sequance_unsigned_null) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block);

    RleEncoderV1 encoder(
      std::unique_ptr<BufferedOutputStream>(
              new BufferedOutputStream(*pool, &memStream, capacity, block)),
          false);

    char* notNull = new char[1024];
    int64_t* data = new int64_t[1024];
    generateData(1024, 0, 1, false, data, 100, notNull);
    encoder.add(data, 1024, notNull);
    encoder.flush();

    decodeAndVerify(memStream, data, 1024, notNull, false);
    delete [] data;
    delete [] notNull;
  }

  TEST(RleEncoderV1, delta_decreasing_sequance_unsigned) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block);

    RleEncoderV1 encoder(
      std::unique_ptr<BufferedOutputStream>(
              new BufferedOutputStream(*pool, &memStream, capacity, block)),
          false);

    int64_t* data = new int64_t[1024];
    generateData(1024, 5000, -3, false, data);
    encoder.add(data, 1024, nullptr);
    encoder.flush();

    decodeAndVerify(memStream, data, 1024, nullptr, false);
    delete [] data;
  }

  TEST(RleEncoderV1, delta_decreasing_sequance_signed) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block);

    RleEncoderV1 encoder(
      std::unique_ptr<BufferedOutputStream>(
              new BufferedOutputStream(*pool, &memStream, capacity, block)),
          true);

    int64_t* data = new int64_t[1024];
    generateData(1024, 100, -3, false, data);
    encoder.add(data, 1024, nullptr);
    encoder.flush();

    decodeAndVerify(memStream, data, 1024, nullptr, true);
    delete [] data;
  }

  TEST(RleEncoderV1, delta_decreasing_sequance_signed_null) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block);

    RleEncoderV1 encoder(
      std::unique_ptr<BufferedOutputStream>(
              new BufferedOutputStream(*pool, &memStream, capacity, block)),
          true);

    char* notNull = new char[1024];
    int64_t* data = new int64_t[1024];
    generateData(1024, 100, -3, false, data, 500, notNull);
    encoder.add(data, 1024, notNull);
    encoder.flush();

    decodeAndVerify(memStream, data, 1024, notNull, true);
    delete [] data;
    delete [] notNull;
  }

  TEST(RleEncoderV1, random_sequance_signed) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block);

    RleEncoderV1 encoder(
      std::unique_ptr<BufferedOutputStream>(
              new BufferedOutputStream(*pool, &memStream, capacity, block)),
          true);

    int64_t* data = new int64_t[1024];
    generateData(1024, 0, 0, true, data);
    encoder.add(data, 1024, nullptr);
    encoder.flush();

    decodeAndVerify(memStream, data, 1024, nullptr, true);
    delete [] data;
  }

  TEST(RleEncoderV1, all_null) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);
    MemoryPool * pool = getDefaultPool();

    uint64_t capacity = 500 * 1024;
    uint64_t block = 1024;
    BufferedOutputStream bufStream(*pool, &memStream, capacity, block);

    RleEncoderV1 encoder(
      std::unique_ptr<BufferedOutputStream>(
              new BufferedOutputStream(*pool, &memStream, capacity, block)),
          true);

    char* notNull = new char[1024];
    int64_t* data = new int64_t[1024];
    generateData(1024, 100, -3, false, data, 1024, notNull);
    encoder.add(data, 1024, notNull);
    encoder.flush();

    decodeAndVerify(memStream, data, 1024, notNull, true);
    delete [] data;
    delete [] notNull;
  }
}
