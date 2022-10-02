
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
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestBlockBuffer, size_and_capacity) {
    MemoryPool* pool = getDefaultPool();
    BlockBuffer buffer(*pool, 1024);

    // block buffer will preallocate one block during initialization
    EXPECT_EQ(buffer.getBlockNumber(), 0);
    EXPECT_EQ(buffer.size(), 0);
    EXPECT_EQ(buffer.capacity(), 1024);

    buffer.reserve(128 * 1024);
    EXPECT_EQ(buffer.getBlockNumber(), 0);
    EXPECT_EQ(buffer.size(), 0);
    EXPECT_EQ(buffer.capacity(), 128 * 1024);

    // new size < old capacity
    buffer.resize(64 * 1024);
    EXPECT_EQ(buffer.getBlockNumber(), 64);
    EXPECT_EQ(buffer.size(), 64 * 1024);
    EXPECT_EQ(buffer.capacity(), 128 * 1024);

    // new size > old capacity
    buffer.resize(256 * 1024);
    EXPECT_EQ(buffer.getBlockNumber(), 256);
    EXPECT_EQ(buffer.size(), 256 * 1024);
    EXPECT_EQ(buffer.capacity(), 256 * 1024);

    // shrinkage of capacity
    buffer.shrink(64 * 1024);
    EXPECT_EQ(buffer.getBlockNumber(), 64);
    EXPECT_EQ(buffer.size(), 64 * 1024);
    EXPECT_EQ(buffer.capacity(), 64 * 1024);
  }

  TEST(TestBlockBuffer, get_block) {
    MemoryPool* pool = getDefaultPool();
    BlockBuffer buffer(*pool, 1024);

    buffer.resize(5000);
    EXPECT_EQ(buffer.getBlockNumber(), 5);
    for (int blockIdx = 0; blockIdx < buffer.getBlockNumber(); ++blockIdx) {
      Block block = buffer.getBlock(blockIdx);
      for (int j = 0; j < block.size; ++j) {
        block.data[j] = static_cast<char>('A' + (blockIdx + j) % 26);
      }
    }

    buffer.resize(10000);
    EXPECT_EQ(buffer.getBlockNumber(), 10);
    for (int blockIdx = 4; blockIdx < buffer.getBlockNumber(); ++blockIdx) {
      Block block = buffer.getBlock(blockIdx);
      for (int j = 0; j < block.size; ++j) {
        block.data[j] = static_cast<char>('a' + (blockIdx + j) % 26);
      }
    }

    // verify the block data
    for (int blockIdx = 0; blockIdx < buffer.getBlockNumber(); ++blockIdx) {
      Block block = buffer.getBlock(blockIdx);
      for (int j = 0; j < block.size; ++j) {
        if (blockIdx < 4) {
          EXPECT_EQ(block.data[j], 'A' + (blockIdx + j) % 26);
        } else {
          EXPECT_EQ(block.data[j], 'a' + (blockIdx + j) % 26);
        }
      }
    }
  }
}
