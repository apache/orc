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

#include "Adaptor.hh"
#include "Exceptions.hh"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace orc {

  class FileInputStream : public InputStream {
  private:
    std::string filename ;
    int file;
    uint64_t totalLength;

  public:
    FileInputStream(std::string _filename) {
      filename = _filename ;
      file = open(filename.c_str(), O_RDONLY);
      if (file == -1) {
        throw ParseError("Can't open " + filename);
      }
      struct stat fileStat;
      if (fstat(file, &fileStat) == -1) {
        throw ParseError("Can't stat " + filename);
      }
      totalLength = static_cast<uint64_t>(fileStat.st_size);
    }

    ~FileInputStream();

    uint64_t getLength() const override {
      return totalLength;
    }

    uint64_t getNaturalReadSize() const override {
      return 128 * 1024;
    }

    void read(void* buf,
              uint64_t length,
              uint64_t offset) override {
      if (!buf) {
        throw ParseError("Buffer is null");
      }
      ssize_t bytesRead = pread(file, buf, length, static_cast<off_t>(offset));

      if (bytesRead == -1) {
        throw ParseError("Bad read of " + filename);
      }
      if (static_cast<uint64_t>(bytesRead) != length) {
        throw ParseError("Short read of " + filename);
      }
    }

    const std::string& getName() const override {
      return filename;
    }
  };

  FileInputStream::~FileInputStream() {
    close(file);
  }

  std::unique_ptr<InputStream> readLocalFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new FileInputStream(path));
  }
}

#ifndef HAS_STOLL

  #include <sstream>

  int64_t std::stoll(std::string str) {
    int64_t val = 0;
    stringstream ss ;
    ss << str ;
    ss >> val ;
    return val;
  }

#endif
