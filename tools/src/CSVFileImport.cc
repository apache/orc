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

#include "orc/Exceptions.hh"
#include "orc/OrcFile.hh"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <sys/time.h>
#include <time.h>

#define DELIMITER ','

std::string extractColumn(std::string s, uint64_t colIndex) {
  uint64_t col = 0;
  size_t start = 0;
  size_t end = s.find(DELIMITER);
  while (col < colIndex && end != std::string::npos) {
    start = end + 1;
    end = s.find(DELIMITER, start);
    ++col;
  }
  return s.substr(start, end - start);
}

static const char* GetDate(void)
{
  static char buf[200];
  time_t t = time(NULL);
  struct tm* p = localtime(&t);
  strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", p);
  return buf;
}

void fillLongValues(const std::vector<std::string>& data,
                    orc::ColumnVectorBatch* batch,
                    uint64_t numValues,
                    uint64_t colIndex) {
  orc::LongVectorBatch* longBatch =
    dynamic_cast<orc::LongVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      longBatch->notNull[i] = 0;
      hasNull = true;
    } else {
      longBatch->data[i] = atoll(col.c_str());
    }
  }
  longBatch->hasNulls = hasNull;
  longBatch->numElements = numValues;
}

void fillStringValues(const std::vector<std::string>& data,
                      orc::ColumnVectorBatch* batch,
                      uint64_t numValues,
                      uint64_t colIndex,
                      orc::DataBuffer<char>& buffer,
                      uint64_t& offset) {
  orc::StringVectorBatch* stringBatch =
    dynamic_cast<orc::StringVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      stringBatch->notNull[i] = 0;
      hasNull = true;
    } else {
      memcpy(buffer.data() + offset,
             col.c_str(),
             col.size());
      stringBatch->data[i] = buffer.data() + offset;
      stringBatch->length[i] = static_cast<int64_t>(col.size());
      offset += col.size();
    }
  }
  stringBatch->hasNulls = hasNull;
  stringBatch->numElements = numValues;
}

void fillDoubleValues(const std::vector<std::string>& data,
                      orc::ColumnVectorBatch* batch,
                      uint64_t numValues,
                      uint64_t colIndex) {
  orc::DoubleVectorBatch* dblBatch =
    dynamic_cast<orc::DoubleVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      dblBatch->notNull[i] = 0;
      hasNull = true;
    } else {
      dblBatch->data[i] = atof(col.c_str());
    }
  }
  dblBatch->hasNulls = hasNull;
  dblBatch->numElements = numValues;
}

// parse fixed point decimal numbers
void fillDecimalValues(const std::vector<std::string>& data,
                       orc::ColumnVectorBatch* batch,
                       uint64_t numValues,
                       uint64_t colIndex,
                       size_t scale,
                       size_t precision) {


  orc::Decimal128VectorBatch* d128Batch = ORC_NULLPTR;
  orc::Decimal64VectorBatch* d64Batch = ORC_NULLPTR;
  if (precision <= 18) {
    d64Batch = dynamic_cast<orc::Decimal64VectorBatch*>(batch);
    d64Batch->scale = static_cast<int32_t>(scale);
  } else {
    d128Batch = dynamic_cast<orc::Decimal128VectorBatch*>(batch);
    d128Batch->scale = static_cast<int32_t>(scale);
  }
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      batch->notNull[i] = 0;
      hasNull = true;
    } else {
      size_t ptPos = col.find('.');
      size_t curScale = 0;
      std::string num = col;
      if (ptPos != std::string::npos) {
        curScale = col.length() - ptPos - 1;
        num = col.substr(0, ptPos) + col.substr(ptPos + 1);
      }
      orc::Int128 decimal(num);
      while (curScale != scale) {
        curScale++;
        decimal *= 10;
      }
      if (precision <= 18) {
        d64Batch->values[i] = decimal.toLong();
      } else {
        d128Batch->values[i] = decimal;
      }
    }
  }
  batch->hasNulls = hasNull;
  batch->numElements = numValues;
}

void fillBoolValues(const std::vector<std::string>& data,
                    orc::ColumnVectorBatch* batch,
                    uint64_t numValues,
                    uint64_t colIndex) {
  orc::LongVectorBatch* boolBatch =
    dynamic_cast<orc::LongVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      boolBatch->notNull[i] = 0;
      hasNull = true;
    } else {
      std::transform(col.begin(), col.end(), col.begin(), ::tolower);
      if (col == "true" || col == "t") {
        boolBatch->data[i] = true;
      } else {
        boolBatch->data[i] = false;
      }
    }
  }
  boolBatch->hasNulls = hasNull;
  boolBatch->numElements = numValues;
}

// parse date string from format YYYY-MM-dd
void fillDateValues(const std::vector<std::string>& data,
                    orc::ColumnVectorBatch* batch,
                    uint64_t numValues,
                    uint64_t colIndex) {
  orc::LongVectorBatch* longBatch =
    dynamic_cast<orc::LongVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      longBatch->notNull[i] = 0;
      hasNull = true;
    } else {
      struct tm tm;
      memset(&tm, 0, sizeof(struct tm));
      strptime(col.c_str(), "%Y-%m-%d", &tm);
      time_t t = mktime(&tm);
      time_t t1970 = 0;
      double seconds = difftime(t, t1970);
      int64_t days = static_cast<int64_t>(seconds / (60*60*24));
      longBatch->data[i] = days;
    }
  }
  longBatch->hasNulls = hasNull;
  longBatch->numElements = numValues;
}

// parse timestamp values in seconds
void fillTimestampValues(const std::vector<std::string>& data,
                         orc::ColumnVectorBatch* batch,
                         uint64_t numValues,
                         uint64_t colIndex) {
  orc::TimestampVectorBatch* tsBatch =
    dynamic_cast<orc::TimestampVectorBatch*>(batch);
  bool hasNull = false;
  for (uint64_t i = 0; i < numValues; ++i) {
    std::string col = extractColumn(data[i], colIndex);
    if (col.empty()) {
      tsBatch->notNull[i] = 0;
      hasNull = true;
    } else {
      tsBatch->data[i] = atoll(col.c_str());
      tsBatch->nanoseconds[i] = 0;
    }
  }
  tsBatch->hasNulls = hasNull;
  tsBatch->numElements = numValues;
}

void usage() {
  std::cout << "Usage: csv-import <input> <output> --schema=<file schema>"
            << "Import CSV file into an Orc file using the specified schema.\n";
}

int main(int argc, char* argv[]) {
  if (argc != 4) {
    std::cout << "Invalid number of arguments." << std::endl;
    usage();
    return 1;
  }

  std::string input = argv[1];
  std::string output = argv[2];
  std::string schema = argv[3];

  const std::string SCHEMA_PREFIX = "--schema=";
  ORC_UNIQUE_PTR<orc::Type> fileType = ORC_NULLPTR;
  if (schema.find(SCHEMA_PREFIX) != 0) {
    std::cout << "Cannot find " << SCHEMA_PREFIX << " argument." << std::endl;
    usage();
    return 1;
  } else {
    fileType = orc::Type::buildTypeFromString(schema.substr(SCHEMA_PREFIX.size()));
  }

  std::cout << GetDate() << "Start importing Orc file..." << std::endl;

  double totalElapsedTime = 0.0;
  double totalCPUTime = 0.0;

  orc::DataBuffer<char> buffer(*orc::getDefaultPool());
  buffer.resize(4 * 1024 * 1024);

  // set ORC writer options here
  uint64_t stripeSize = (36 << 20); // 36M
  uint64_t blockSize = 64 * 1024; // 64K
  uint64_t batchSize = 1024;
  orc::CompressionKind compression = orc::CompressionKind_ZLIB;

  orc::WriterOptions options;
  options.setStripeSize(stripeSize);
  options.setCompressionBlockSize(blockSize);
  options.setCompression(compression);

  ORC_UNIQUE_PTR<orc::OutputStream> outStream = orc::writeLocalFile(output);
  ORC_UNIQUE_PTR<orc::Writer> writer =
    orc::createWriter(*fileType, outStream.get(), options);
  ORC_UNIQUE_PTR<orc::ColumnVectorBatch> rowBatch =
    writer->createRowBatch(batchSize);

  bool eof = false;
  std::string line;
  std::vector<std::string> data;
  std::ifstream finput(input.c_str());
  while (!eof) {
    uint64_t numValues = 0;
    uint64_t bufferOffset = 0;

    data.clear();
    memset(rowBatch->notNull.data(), 1, batchSize);

    for (uint64_t i = 0; i < batchSize; ++i) {
      if (!std::getline(finput, line)) {
        eof = true;
        break;
      }
      data.push_back(line);
      ++numValues;
    }

    if (numValues != 0) {
      orc::StructVectorBatch* structBatch =
        dynamic_cast<orc::StructVectorBatch*>(rowBatch.get());
      structBatch->numElements = numValues;

      for (uint64_t i = 0; i < structBatch->fields.size(); ++i) {
        orc::TypeKind subTypeKind = fileType->getSubtype(i)->getKind();
        switch (subTypeKind) {
          case orc::BYTE:
          case orc::INT:
          case orc::SHORT:
          case orc::LONG:
            fillLongValues(data,
                           structBatch->fields[i],
                           numValues,
                           i);
            break;
          case orc::STRING:
          case orc::CHAR:
          case orc::VARCHAR:
          case orc::BINARY:
            fillStringValues(data,
                             structBatch->fields[i],
                             numValues,
                             i,
                             buffer,
                             bufferOffset);
            break;
          case orc::FLOAT:
          case orc::DOUBLE:
            fillDoubleValues(data,
                             structBatch->fields[i],
                             numValues,
                             i);
            break;
          case orc::DECIMAL:
            fillDecimalValues(data,
                              structBatch->fields[i],
                              numValues,
                              i,
                              fileType->getSubtype(i)->getScale(),
                              fileType->getSubtype(i)->getPrecision());
            break;
          case orc::BOOLEAN:
            fillBoolValues(data,
                           structBatch->fields[i],
                           numValues,
                           i);
            break;
          case orc::DATE:
            fillDateValues(data,
                           structBatch->fields[i],
                           numValues,
                           i);
            break;
          case orc::TIMESTAMP:
            fillTimestampValues(data,
                                structBatch->fields[i],
                                numValues,
                                i);
            break;
          case orc::STRUCT:
          case orc::LIST:
          case orc::MAP:
          case orc::UNION:
            throw std::runtime_error("Type is not supported yet.");
        }
      }

      struct timeval t_start, t_end;
      gettimeofday(&t_start, NULL);
      clock_t c_start = clock();
      writer->add(*rowBatch);
      totalCPUTime += (clock() - c_start);
      gettimeofday(&t_end, NULL);
      totalElapsedTime +=
        (static_cast<double>((t_end.tv_sec - t_start.tv_sec) * 1000000.0 +
                             t_end.tv_usec - t_start.tv_usec) / 1000000);
    }
  }

  struct timeval t_start, t_end;
  gettimeofday(&t_start, NULL);
  clock_t c_start = clock();
  writer->close();
  totalCPUTime += (clock() - c_start);
  gettimeofday(&t_end, NULL);
  totalElapsedTime +=
    (static_cast<double>((t_end.tv_sec - t_start.tv_sec) * 1000000.0 +
                         t_end.tv_usec - t_start.tv_usec) / 1000000);

  std::cout << GetDate() << "Finish importing Orc file." << std::endl;
  std::cout << GetDate() << "Total writer elasped time: "
            << totalElapsedTime << "s." << std::endl;
  std::cout << GetDate() << "Total writer CPU time: "
            << totalCPUTime / CLOCKS_PER_SEC << "s." << std::endl;
  return 0;
}
