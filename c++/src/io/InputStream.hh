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

#ifndef ORC_INPUTSTREAM_HH
#define ORC_INPUTSTREAM_HH

#include "Adaptor.hh"
#include "orc/OrcFile.hh"
#include "wrap/zero-copy-stream-wrapper.h"
#include <openssl/evp.h>
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <fstream>
#include <iostream>
#include <list>
#include <sstream>
#include <vector>

namespace orc {

  void printBuffer(std::ostream& out, const char* buffer, uint64_t length);

  class PositionProvider {
   private:
    std::list<uint64_t>::const_iterator position_;

   public:
    PositionProvider(const std::list<uint64_t>& positions);
    uint64_t next();
    uint64_t current();
  };

  /**
   * A subclass of Google's ZeroCopyInputStream that supports seek.
   * By extending Google's class, we get the ability to pass it directly
   * to the protobuf readers.
   */
  class SeekableInputStream : public google::protobuf::io::ZeroCopyInputStream {
   public:
    ~SeekableInputStream() override;
    virtual void seek(PositionProvider& position) = 0;
    virtual std::string getName() const = 0;
  };

  /**
   * Create a seekable input stream based on a memory range.
   */
  class SeekableArrayInputStream : public SeekableInputStream {
   private:
    const char* data_;
    uint64_t length_;
    uint64_t position_;
    uint64_t blockSize_;

   public:
    SeekableArrayInputStream(const unsigned char* list, uint64_t length, uint64_t blockSize = 0);
    SeekableArrayInputStream(const char* list, uint64_t length, uint64_t blockSize = 0);
    virtual ~SeekableArrayInputStream() override;
    virtual bool Next(const void** data, int* size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual google::protobuf::int64 ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;
  };

  /**
   * Create a seekable input stream based on an input stream.
   */
  class SeekableFileInputStream : public SeekableInputStream {
   private:
    MemoryPool& pool_;
    InputStream* const input_;
    const uint64_t start_;
    const uint64_t length_;
    const uint64_t blockSize_;
    std::unique_ptr<DataBuffer<char> > buffer_;
    uint64_t position_;
    uint64_t pushBack_;

   public:
    SeekableFileInputStream(InputStream* input, uint64_t offset, uint64_t byteCount,
                            MemoryPool& pool, uint64_t blockSize = 0);
    virtual ~SeekableFileInputStream() override;

    virtual bool Next(const void** data, int* size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;
  };
  class DecryptionInputStream : public SeekableInputStream {
   public:
    DecryptionInputStream(std::unique_ptr<SeekableInputStream> input,std::vector<unsigned char> key,
                          std::vector<unsigned char> iv,const EVP_CIPHER* cipher,MemoryPool& pool);
    virtual ~DecryptionInputStream();

    virtual bool Next(const void** data, int* size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual google::protobuf::int64 ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;
    void changeIv(long offset);

   private:
    std::unique_ptr<SeekableInputStream> input_;
    std::vector<unsigned char> key_;
    std::vector<unsigned char> iv_;
    EVP_CIPHER_CTX* ctx_;
    const EVP_CIPHER* cipher;
    MemoryPool& pool;
    std::unique_ptr<DataBuffer<unsigned char>> inputBuffer_;
    std::unique_ptr<DataBuffer<unsigned char>> outputBuffer_;
  };
}  // namespace orc

#endif  // ORC_INPUTSTREAM_HH
