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
#include "Exceptions.hh"

#include <string>
#include <memory>
#include <iostream>
#include <map>
#include <exception>

class TestMemoryPool: public orc::MemoryPool {
private:
  std::map<char*, uint64_t> blocks;
  uint64_t totalMemory;
  uint64_t maxMemory;

public:
  char* malloc(uint64_t size) override {
    char* p = static_cast<char*>(std::malloc(size));
    blocks[p] = size ;
    totalMemory += size;
    if (maxMemory < totalMemory) {
      maxMemory = totalMemory;
    }
    return p;
  }

  void free(char* p) override {
    std::free(p);
    totalMemory -= blocks[p] ;
    blocks.erase(p);
  }

  uint64_t getMaxMemory() {
    return maxMemory ;
  }

  TestMemoryPool(): totalMemory(0), maxMemory(0) {}
  ~TestMemoryPool();
};

TestMemoryPool::~TestMemoryPool() {}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: file-memory [--columns=column1,column2,...] "
        << "[--batch=rows_in_batch] <filename> \n";
    return 1;
  }

  const std::string COLUMNS_PREFIX = "--columns=";
  const std::string BATCH_PREFIX = "--batch=";
  char* filename = ORC_NULLPTR;

  // Default parameters
  std::list<int64_t> cols;
  uint32_t batchSize = 1000;

  // Read command-line options
  char *param, *value;
  for (int i = 1; i < argc; i++) {
    if ( (param = std::strstr(argv[i], COLUMNS_PREFIX.c_str())) ) {
      value = std::strtok(param+COLUMNS_PREFIX.length(), "," );
      while (value) {
        cols.push_back(std::atoi(value));
        value = std::strtok(nullptr, "," );
      }
    } else if ( (param=strstr(argv[i], BATCH_PREFIX.c_str())) ) {
      batchSize = static_cast<uint32_t>(std::atoi(param+BATCH_PREFIX.length()));
    } else {
      filename = argv[i];
    }
  }

  if (filename == ORC_NULLPTR) {
    std::cout << "Error: Filename not provided.\n";
    return 1;
  }

  orc::ReaderOptions opts;
  if (cols.size() > 0) {
    opts.include(cols);
  }
  std::unique_ptr<orc::MemoryPool> pool(new TestMemoryPool());
  opts.setMemoryPool(*(pool.get()));

  std::unique_ptr<orc::Reader> reader;
  try{
    reader = orc::createReader(orc::readLocalFile(std::string(filename)), opts);
  } catch (orc::ParseError e) {
    std::cout << "Error reading file " << filename << "! " << e.what() << "\n";
    return -1;
  }

  std::unique_ptr<orc::ColumnVectorBatch> batch =
      reader->createRowBatch(batchSize);
  uint64_t readerMemory = reader->getMemoryUse();
  uint64_t batchMemory = batch->getMemoryUsage();
  while (reader->next(*batch)) {}
  uint64_t actualMemory =
      static_cast<TestMemoryPool*>(pool.get())->getMaxMemory();
  std::cout << "Reader memory estimate: " << readerMemory
            << "\nBatch memory estimate:  " ;
  if (batch->hasVariableLength()) {
    std::cout << "Cannot estimate because reading ARRAY or MAP columns";
  } else {
    std::cout << batchMemory
              << "\nTotal memory estimate:  " << readerMemory + batchMemory;
  }
  std::cout << "\nActual max memory used: " << actualMemory << "\n";

  return 0;
}
