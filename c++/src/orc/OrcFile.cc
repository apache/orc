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

#include "orc/Adaptor.hh"
#include "orc/OrcFile.hh"
#include "Exceptions.hh"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace orc {

  Buffer::~Buffer() {
    // PASS
  }

  class HeapBuffer: public Buffer {
  private:
    char* start;
    uint64_t length;

  public:
    HeapBuffer(uint64_t size) {
      start = new char[size];
      length = size;
    }

    virtual ~HeapBuffer();

    virtual char *getStart() const override {
      return start;
    }

    virtual uint64_t getLength() const override {
      return length;
    }
  };

  HeapBuffer::~HeapBuffer() {
    delete[] start;
  }

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

    Buffer* read(uint64_t offset,
                 uint64_t length,
                 Buffer* buffer) override {
      if (buffer == nullptr) {
        buffer = new HeapBuffer(length);
      } else if (buffer->getLength() < length) {
        delete buffer;
        buffer = new HeapBuffer(length);
      }
      ssize_t bytesRead = pread(file, buffer->getStart(), length,
                                static_cast<off_t>(offset));
      if (bytesRead == -1) {
        throw ParseError("Bad read of " + filename);
      }
      if (static_cast<uint64_t>(bytesRead) != length) {
        throw ParseError("Short read of " + filename);
      }
      return buffer;
    }

    const std::string& getName() const override {
      return filename;
    }
  };

  FileInputStream::~FileInputStream() {
    close(file);
  }

  /**
   * A buffer for use with an memmapped file where the Buffer doesn't own
   * the memory that it references.
   */
  class MmapBuffer: public Buffer {
  private:
    char* start;
    uint64_t length;

  public:
    MmapBuffer(): start(nullptr), length(0) {
      // PASS
    }

    virtual ~MmapBuffer();

    void reset(char *_start, uint64_t _length) {
      start = _start;
      length = _length;
    }

    virtual char *getStart() const override {
      return start;
    }

    virtual uint64_t getLength() const override {
      return length;
    }
  };

  MmapBuffer::~MmapBuffer() {
    // PASS
  }

  /**
   * An InputStream implementation that uses memory mapping to read the
   * local file.
   */
  class MmapInputStream : public InputStream {
  private:
    std::string filename ;
    char* start;
    uint64_t totalLength;

  public:
    MmapInputStream(std::string _filename);
    ~MmapInputStream();

    uint64_t getLength() const override {
      return totalLength;
    }

    const std::string& getName() const override {
      return filename;
    }

    Buffer* read(uint64_t offset,
                 uint64_t length,
                 Buffer* buffer) override;
  };

  MmapInputStream::MmapInputStream(std::string _filename) {
    filename = _filename ;
    int file = open(filename.c_str(), O_RDONLY);
    if (file == -1) {
      throw ParseError("Can't open " + filename);
    }
    struct stat fileStat;
    if (fstat(file, &fileStat) == -1) {
      throw ParseError("Can't stat " + filename);
    }
    totalLength = static_cast<uint64_t>(fileStat.st_size);
    start = static_cast<char*>(mmap(nullptr, totalLength, PROT_READ,
                                    MAP_FILE|MAP_PRIVATE,
                                    file, 0LL));
    if (start == MAP_FAILED) {
      throw std::runtime_error("mmap failed " + filename + " " +
                               strerror(errno));
    }
    close(file);
  }

  MmapInputStream::~MmapInputStream() {
    int64_t result = munmap(reinterpret_cast<void*>(start), totalLength);
    if (result != 0) {
      throw std::runtime_error("Failed to unmap " + filename + " - " +
                               strerror(errno));
    }
  }

  Buffer* MmapInputStream::read(uint64_t offset,
                                uint64_t length,
                                Buffer* buffer) {
    if (buffer == nullptr) {
      buffer = new MmapBuffer();
    }
    if (offset + length > totalLength) {
      throw std::runtime_error("Read past end of file " + filename);
    }
    dynamic_cast<MmapBuffer*>(buffer)->reset(start + offset, length);
    return buffer;
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
