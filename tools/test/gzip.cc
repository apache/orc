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

#include "gzip.hh"
#include "Adaptor.hh"

#include <iostream>
#include <stdexcept>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wold-style-cast"
#endif

namespace orc {

  GzipTextReader::GzipTextReader(const std::string& filename) : filename_(filename) {
    file_ = fopen(filename_.c_str(), "rb");
    if (file_ == nullptr) {
      throw std::runtime_error("can't open " + filename_);
    }
    stream_.zalloc = nullptr;
    stream_.zfree = nullptr;
    stream_.opaque = nullptr;
    stream_.avail_in = 0;
    stream_.avail_out = 1;
    stream_.next_in = nullptr;
    int ret = inflateInit2(&stream_, 16 + MAX_WBITS);
    if (ret != Z_OK) {
      throw std::runtime_error("zlib failed initialization for " + filename_);
    }
    outPtr_ = nullptr;
    outEnd_ = nullptr;
    isDone_ = false;
  }

  bool GzipTextReader::nextBuffer() {
    // if we are done, return
    if (isDone_) {
      return false;
    }
    // if the last read is done, read more
    if (stream_.avail_in == 0 && stream_.avail_out != 0) {
      stream_.next_in = input_;
      stream_.avail_in = static_cast<unsigned>(fread(input_, 1, sizeof(input_), file_));
      if (ferror(file_)) {
        throw std::runtime_error("failure reading " + filename_);
      }
    }
    stream_.avail_out = sizeof(output_);
    stream_.next_out = output_;
    int ret = inflate(&stream_, Z_NO_FLUSH);
    switch (ret) {
      case Z_OK:
        break;
      case Z_STREAM_END:
        isDone_ = true;
        break;
      case Z_STREAM_ERROR:
        throw std::runtime_error("zlib stream problem");
      case Z_NEED_DICT:
      case Z_DATA_ERROR:
        throw std::runtime_error("zlib data problem");
      case Z_MEM_ERROR:
        throw std::runtime_error("zlib memory problem");
      case Z_BUF_ERROR:
        throw std::runtime_error("zlib buffer problem");
      default:
        throw std::runtime_error("zlib unknown problem");
    }
    outPtr_ = output_;
    outEnd_ = output_ + (sizeof(output_) - stream_.avail_out);
    return true;
  }

  bool GzipTextReader::nextLine(std::string& line) {
    bool result = false;
    line.clear();
    while (true) {
      if (outPtr_ == outEnd_) {
        if (!nextBuffer()) {
          return result;
        }
      }
      unsigned char ch = *(outPtr_++);
      if (ch == '\n') {
        return true;
      }
      line += static_cast<char>(ch);
    }
  }

  GzipTextReader::~GzipTextReader() {
    inflateEnd(&stream_);
    if (fclose(file_) != 0) {
      std::cerr << "can't close file " << filename_;
    }
  }
}  // namespace orc
