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

#pragma once

#include <orc/MemoryPool.hh>
#include <orc/OrcFile.hh>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <future>
#include <utility>
#include <vector>

namespace orc {
  class InputStream;

  struct CacheOptions {
    /// The maximum distance in bytes between two consecutive
    /// ranges; beyond this value, ranges are not combined
    uint64_t holeSizeLimit = 8192;

    /// The maximum size in bytes of a combined range; if
    /// combining two consecutive ranges would produce a range of a
    /// size greater than this, they are not combined
    uint64_t rangeSizeLimit = 32 * 1024 * 1024;
  };

  struct ReadRange {
    uint64_t offset;
    uint64_t length;

    ReadRange() = default;
    ReadRange(uint64_t offset, uint64_t length) : offset(offset), length(length) {}

    friend bool operator==(const ReadRange& left, const ReadRange& right) {
      return (left.offset == right.offset && left.length == right.length);
    }
    friend bool operator!=(const ReadRange& left, const ReadRange& right) {
      return !(left == right);
    }

    bool contains(const ReadRange& other) const {
      return (offset <= other.offset && offset + length >= other.offset + other.length);
    }
  };

  struct ReadRangeCombiner {
    const uint64_t holeSizeLimit;
    const uint64_t rangeSizeLimit;

    std::vector<ReadRange> coalesce(std::vector<ReadRange> ranges) const;
  };

  std::vector<ReadRange> coalesceReadRanges(std::vector<ReadRange> ranges, uint64_t holeSizeLimit,
                                            uint64_t rangeSizeLimit);
  struct RangeCacheEntry {
    using BufferPtr = InputStream::BufferPtr;

    ReadRange range;

    // The result may be get multiple times, so we use shared_future instead of std::future
    std::shared_future<BufferPtr> future;

    RangeCacheEntry() = default;
    RangeCacheEntry(const ReadRange& range, std::future<BufferPtr> future)
        : range(range), future(std::move(future).share()) {}

    friend bool operator<(const RangeCacheEntry& left, const RangeCacheEntry& right) {
      return left.range.offset < right.range.offset;
    }
  };

  /// A read cache designed to hide IO latencies when reading.
  class ReadRangeCache {
   public:
    using Buffer = InputStream::Buffer;
    using BufferPtr = InputStream::BufferPtr;

    struct BufferSlice {
      BufferSlice() : buffer(nullptr), offset(0), length(0) {}

      BufferSlice(BufferPtr buffer, uint64_t offset, uint64_t length)
          : buffer(std::move(buffer)), offset(offset), length(length) {}

      BufferPtr buffer;
      uint64_t offset;
      uint64_t length;
    };

    /// Construct a read cache with given options
    explicit ReadRangeCache(InputStream* stream, CacheOptions options, MemoryPool* memoryPool)
        : stream_(stream), options_(std::move(options)), memoryPool_(memoryPool) {}

    ~ReadRangeCache() = default;

    /// Cache the given ranges in the background.
    ///
    /// The caller must ensure that the ranges do not overlap with each other,
    /// nor with previously cached ranges.  Otherwise, behaviour will be undefined.
    void cache(std::vector<ReadRange> ranges);

    /// Read a range previously given to Cache().
    BufferSlice read(const ReadRange& range);

    /// Evict cache entries with its range before given boundary.
    void evictEntriesBefore(uint64_t boundary);

   private:
    std::vector<RangeCacheEntry> makeCacheEntries(const std::vector<ReadRange>& ranges);

    InputStream* stream_;
    CacheOptions options_;

    // Ordered by offset (so as to find a matching region by binary search)
    std::vector<RangeCacheEntry> entries_;

    MemoryPool* memoryPool_;
  };

}  // namespace orc
