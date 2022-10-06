
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

#ifndef ORC_MEMORYPOOL_IMPL_HH
#define ORC_MEMORYPOOL_IMPL_HH

#include "orc/MemoryPool.hh"

#include <vector>

namespace orc {

  struct Block {
    char* data;
    uint64_t size;

    Block() : data(nullptr), size(0) {}
    Block(char* _data, uint64_t _size) : data(_data), size(_size) {}
    Block(const Block& block) = default;
    ~Block() {
      // PASS
    }
  };

  class BlockBuffer {
  private:
    MemoryPool& memoryPool;
    // current buffer size
    uint64_t currentSize;
    // maximal capacity (actual allocated memory)
    uint64_t currentCapacity;
    // unit for buffer expansion
    const uint64_t blockSize;
    // pointers to the start of each block
    std::vector<char*> blocks;

    // non-copy-constructible
    BlockBuffer(BlockBuffer& buffer) = delete;
    BlockBuffer& operator=(BlockBuffer& buffer) = delete;
    BlockBuffer(BlockBuffer&& buffer) = delete;
    BlockBuffer& operator=(BlockBuffer&& buffer) = delete;

  public:
    BlockBuffer(MemoryPool& pool, uint64_t blockSize);

    ~BlockBuffer();

    /**
     * Get the Block object
     */
    Block getBlock(uint64_t blockIndex);

    /**
     * Get the empty block or allocate a new block if the buffer is exhausted
     */
    Block getEmptyBlock();

    /**
     * Get the Block number
     */
    uint64_t getBlockNumber() const;

    uint64_t size() const {
      return currentSize;
    }

    uint64_t capacity() const {
      return currentCapacity;
    }

    void resize(uint64_t size);
    void reserve(uint64_t capacity);
  };
} // namespace

#endif
