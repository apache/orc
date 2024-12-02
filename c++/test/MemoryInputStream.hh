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

#ifndef ORC_MEMORYINPUTSTREAM_HH
#define ORC_MEMORYINPUTSTREAM_HH

#include "io/InputStream.hh"
#include "orc/OrcFile.hh"

namespace orc {
  class MemoryInputStream : public InputStream {
   public:
    MemoryInputStream(const char* buffer, size_t size)
        : buffer_(buffer), size_(size), naturalReadSize_(1024), name_("MemoryInputStream") {}

    ~MemoryInputStream() override;

    virtual uint64_t getLength() const override {
      return size_;
    }

    virtual uint64_t getNaturalReadSize() const override {
      return naturalReadSize_;
    }

    virtual void read(void* buf, uint64_t length, uint64_t offset) override {
      memcpy(buf, buffer_ + offset, length);
    }

    std::future<void> readAsync(void* buf, uint64_t length, uint64_t offset) override {
      return std::async(std::launch::async,
                        [this, buf, length, offset] { this->read(buf, length, offset); });
    }

    virtual const std::string& getName() const override {
      return name_;
    }

    const char* getData() const {
      return buffer_;
    }

   private:
    const char* buffer_;
    uint64_t size_, naturalReadSize_;
    std::string name_;
  };
}  // namespace orc

#endif
