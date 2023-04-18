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
#include "RLEv2.hh"
#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

#ifdef __clang__
DIAGNOSTIC_IGNORE("-Wmissing-variable-declarations")
#endif

namespace orc {
  using ::testing::TestWithParam;
  using ::testing::Values;

  const int DEFAULT_MEM_STREAM_SIZE = 1024 * 1024;  // 1M
  const char finish = '#';
  std::string flags = "-\\|/";

  class RleV2BitUnpackAvx512Test : public TestWithParam<bool> {
    virtual void SetUp();

   protected:
    bool alignBitpacking;
    std::unique_ptr<RleEncoder> getEncoder(RleVersion version, MemoryOutputStream& memStream,
                                           bool isSigned);

    void runExampleTest(int64_t* inputData, uint64_t inputLength, unsigned char* expectedOutput,
                        uint64_t outputLength);

    void runTest(RleVersion version, uint64_t numValues, int64_t start, int64_t delta, bool random,
                 bool isSigned, uint8_t bitWidth, uint64_t blockSize = 0, uint64_t numNulls = 0);
  };

  void vectorDecodeAndVerify(RleVersion version, const MemoryOutputStream& memStream, int64_t* data,
                             uint64_t numValues, const char* notNull, uint64_t blockSize,
                             bool isSinged) {
    std::unique_ptr<RleDecoder> decoder =
        createRleDecoder(std::unique_ptr<SeekableArrayInputStream>(new SeekableArrayInputStream(
                             memStream.getData(), memStream.getLength(), blockSize)),
                         isSinged, version, *getDefaultPool(), getDefaultReaderMetrics());

    int64_t* decodedData = new int64_t[numValues];
    decoder->next(decodedData, numValues, notNull);

    for (uint64_t i = 0; i < numValues; ++i) {
      if (!notNull || notNull[i]) {
        EXPECT_EQ(data[i], decodedData[i]);
      }
    }

    delete[] decodedData;
  }

  void RleV2BitUnpackAvx512Test::SetUp() {
    alignBitpacking = GetParam();
  }

  void generateDataForBits(uint64_t numValues, int64_t start, int64_t delta, bool random,
                           int64_t* data, uint8_t bitWidth, uint64_t numNulls = 0,
                           char* notNull = nullptr) {
    int64_t max = pow(2, bitWidth);
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
      if (notNull == nullptr || notNull[i]) {
        if (!random) {
          data[i] = start + delta * static_cast<int64_t>(i);
        } else {
          data[i] = std::rand() % max;
        }
      }
    }
  }

  void printBar(const char* testName, int64_t offset, int64_t total) {
    int64_t n = offset * 100 / total;
    std::string progress(100, '.');
    for (int i = 0; i < n; i++) {
      progress[i] = finish;
    }

    std::string f, p;
    if (n == 100) {
      f = "OK";
      p = "100%";
    } else {
      f = flags[n % 4];
      p = std::to_string(n) + '%';
    }
    std::cout << std::unitbuf << testName << ":" << '[' << f << ']' << '[' << progress << ']' << '['
              << p << "]" << '\r';
    if (n >= 100) {
      std::cout << std::endl;
    }
  }

  std::unique_ptr<RleEncoder> RleV2BitUnpackAvx512Test::getEncoder(RleVersion version,
                                                                   MemoryOutputStream& memStream,
                                                                   bool isSigned) {
    MemoryPool* pool = getDefaultPool();

    return createRleEncoder(std::unique_ptr<BufferedOutputStream>(new BufferedOutputStream(
                                *pool, &memStream, 500 * 1024, 1024, nullptr)),
                            isSigned, version, *pool, alignBitpacking);
  }

  void RleV2BitUnpackAvx512Test::runTest(RleVersion version, uint64_t numValues, int64_t start,
                                         int64_t delta, bool random, bool isSigned,
                                         uint8_t bitWidth, uint64_t blockSize, uint64_t numNulls) {
    MemoryOutputStream memStream(DEFAULT_MEM_STREAM_SIZE);

    std::unique_ptr<RleEncoder> encoder = getEncoder(version, memStream, isSigned);

    char* notNull = numNulls == 0 ? nullptr : new char[numValues];
    int64_t* data = new int64_t[numValues];
    generateDataForBits(numValues, start, delta, random, data, bitWidth, numNulls, notNull);
    encoder->add(data, numValues, notNull);
    encoder->flush();

    vectorDecodeAndVerify(version, memStream, data, numValues, notNull, blockSize, isSigned);
    delete[] data;
    delete[] notNull;
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_1bit) {
    uint8_t bitWidth = 1;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("1bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("1bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_2bit) {
    uint8_t bitWidth = 2;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("2bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("2bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_3bit) {
    uint8_t bitWidth = 3;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("3bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("3bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_4bit) {
    uint8_t bitWidth = 4;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("4bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("4bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_5bit) {
    uint8_t bitWidth = 5;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("5bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("5bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_6bit) {
    uint8_t bitWidth = 6;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("6bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("6bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_7bit) {
    uint8_t bitWidth = 7;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("7bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("7bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_9bit) {
    uint8_t bitWidth = 9;

    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("9bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("9bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_10bit) {
    uint8_t bitWidth = 10;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("10bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("10bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_11bit) {
    uint8_t bitWidth = 11;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("11bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("11bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_12bit) {
    uint8_t bitWidth = 12;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("12bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("12bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_13bit) {
    uint8_t bitWidth = 13;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("13bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("13bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_14bit) {
    uint8_t bitWidth = 14;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("14bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("14bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_15bit) {
    uint8_t bitWidth = 15;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("15bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("15bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_16bit) {
    uint8_t bitWidth = 16;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("16bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("16bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_17bit) {
    uint8_t bitWidth = 17;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("17bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("17bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_18bit) {
    uint8_t bitWidth = 18;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("18bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("18bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_19bit) {
    uint8_t bitWidth = 19;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("19bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("19bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_20bit) {
    uint8_t bitWidth = 20;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("20bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("20bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_21bit) {
    uint8_t bitWidth = 21;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("21bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("21bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_22bit) {
    uint8_t bitWidth = 22;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("22bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("22bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_23bit) {
    uint8_t bitWidth = 23;
    runTest(RleVersion_2, 3277, 0, 0, true, false, bitWidth, 108);
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("23bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("23bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_24bit) {
    uint8_t bitWidth = 24;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("24bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("24bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_26bit) {
    uint8_t bitWidth = 26;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("26bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("26bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_28bit) {
    uint8_t bitWidth = 28;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("28bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("28bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_30bit) {
    uint8_t bitWidth = 30;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("30bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("30bit Test 2nd Part", blockSize, 10000);
    }
  }

  TEST_P(RleV2BitUnpackAvx512Test, RleV2_basic_vector_decode_32bit) {
    uint8_t bitWidth = 32;
    for (uint64_t blockSize = 1; blockSize <= 10000; blockSize++) {
      runTest(RleVersion_2, 10240, 0, 0, true, false, bitWidth, blockSize);
      printBar("32bit Test 1st Part", blockSize, 10000);
    }

    for (uint64_t blockSize = 1000; blockSize <= 10000; blockSize += 1000) {
      for (uint64_t dataSize = 1000; dataSize <= 70000; dataSize += 1000) {
        runTest(RleVersion_2, dataSize, 0, 0, true, false, bitWidth, blockSize);
      }
      printBar("32bit Test 2nd Part", blockSize, 10000);
    }
  }

  INSTANTIATE_TEST_SUITE_P(OrcTest, RleV2BitUnpackAvx512Test, Values(true, false));
}  // namespace orc
