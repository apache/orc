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

#include <memory>
#include <string>
#include <iostream>
#include <string>

void printContents(const char* filename, const orc::ReaderOptions opts) {
  std::unique_ptr<orc::Reader> reader;
  reader = orc::createReader(orc::readLocalFile(std::string(filename)), opts);

  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1000);
  std::string line;
  const std::vector<bool> selectedColumns = reader->getSelectedColumns();
  std::unique_ptr<orc::ColumnPrinter> printer =
    createColumnPrinter(line, reader->getType(), &selectedColumns);

  while (reader->next(*batch)) {
    printer->reset(*batch);
    for(unsigned long i=0; i < batch->numElements; ++i) {
      line.clear();
      printer->printRow(i);
      line += "\n";
      const char* str = line.c_str();
      fwrite(str, 1, strlen(str), stdout);
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: file-contents [--columns=1,2,...] <filename>\n" ;
    return 1;
  }
  try {
    const std::string COLUMNS_PREFIX = "--columns=";
    std::list<int64_t> cols;
    char* filename = ORC_NULLPTR;

    // Read command-line options
    char *param, *value;
    for (int i = 1; i < argc; i++) {
      if ( (param = std::strstr(argv[i], COLUMNS_PREFIX.c_str())) ) {
        value = std::strtok(param+COLUMNS_PREFIX.length(), "," );
        while (value) {
          cols.push_back(std::atoi(value));
          value = std::strtok(nullptr, "," );
        }
      } else {
        filename = argv[i];
      }
    }
    orc::ReaderOptions opts;
    if (cols.size() > 0) {
      opts.include(cols);
    }
    if (filename != ORC_NULLPTR) {
      printContents(filename, opts);
    }
  } catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return 1;
  }
  return 0;
}
