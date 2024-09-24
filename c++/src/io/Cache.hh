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

#include <cassert>
#include <cstdint>
#include <future>
#include <memory>
#include <utility>
#include <vector>
#include <algorithm>

namespace orc {
  class InputStream;

  struct CacheOptions {
    /// The maximum distance in bytes between two consecutive
    /// ranges; beyond this value, ranges are not combined
    uint64_t hole_size_limit;

    /// The maximum size in bytes of a combined range; if
    /// combining two consecutive ranges would produce a range of a
    /// size greater than this, they are not combined
    uint64_t range_size_limit;
  };

  struct ReadRange {
    uint64_t offset;
    uint64_t length;

    ReadRange() = default;
    ReadRange(uint64_t _offset, uint64_t _length) : offset(_offset), length(_length) {}

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
    std::vector<ReadRange> coalesce(std::vector<ReadRange> ranges) const {
      if (ranges.empty()) {
        return ranges;
      }

      // Remove zero-sized ranges
      auto end = std::remove_if(ranges.begin(), ranges.end(),
                                [](const ReadRange& range) { return range.length == 0; });
      // Sort in position order
      std::sort(ranges.begin(), end,
                [](const ReadRange& a, const ReadRange& b) { return a.offset < b.offset; });

      // Remove ranges that overlap 100%
      end = std::unique(ranges.begin(), end, [](const ReadRange& left, const ReadRange& right) {
        return right.offset >= left.offset &&
               right.offset + right.length <= left.offset + left.length;
      });
      ranges.resize(end - ranges.begin());

      // Skip further processing if ranges is empty after removing zero-sized ranges.
      if (ranges.empty()) {
        return ranges;
      }

#ifndef NDEBUG
      for (size_t i = 0; i < ranges.size() - 1; ++i) {
        const auto& left = ranges[i];
        const auto& right = ranges[i + 1];
        assert(left.offset < right.offset);
        assert(left.offset + left.length < right.offset);
      }
#endif

      std::vector<ReadRange> coalesced;

      auto itr = ranges.begin();
      // Ensure ranges is not empty.
      assert(itr <= ranges.end());
      // Start of the current coalesced range and end (exclusive) of previous range.
      // Both are initialized with the start of first range which is a placeholder value.
      uint64_t coalesced_start = itr->offset;
      uint64_t prev_range_end = coalesced_start;

      for (; itr < ranges.end(); ++itr) {
        const uint64_t current_range_start = itr->offset;
        const uint64_t current_range_end = current_range_start + itr->length;
        // We don't expect to have 0 sized ranges.
        assert(current_range_start < current_range_end);

        // At this point, the coalesced range is [coalesced_start, prev_range_end).
        // Stop coalescing if:
        //   - coalesced range is too large, or
        //   - distance (hole/gap) between consecutive ranges is too large.
        if (current_range_end - coalesced_start > range_size_limit ||
            current_range_start - prev_range_end > hole_size_limit) {
          assert(coalesced_start <= prev_range_end);
          // Append the coalesced range only if coalesced range size > 0.
          if (prev_range_end > coalesced_start) {
            coalesced.push_back({coalesced_start, prev_range_end - coalesced_start});
          }
          // Start a new coalesced range.
          coalesced_start = current_range_start;
        }

        // Update the prev_range_end with the current range.
        prev_range_end = current_range_end;
      }

      // Append the coalesced range only if coalesced range size > 0.
      if (prev_range_end > coalesced_start) {
        coalesced.push_back({coalesced_start, prev_range_end - coalesced_start});
      }

      assert(coalesced.front().offset == ranges.front().offset);
      assert(coalesced.back().offset + coalesced.back().length ==
             ranges.back().offset + ranges.back().length);
      return coalesced;
    }

    const uint64_t hole_size_limit;
    const uint64_t range_size_limit;
  };

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
  ///
  /// This class takes multiple byte ranges that an application expects to read, and
  /// coalesces them into fewer, larger read requests, which benefits performance on some
  /// filesystems, particularly remote ones like Amazon S3. By default, it also issues
  /// these read requests in parallel up front.
  ///
  /// To use:
  /// 1. Cache() the ranges you expect to read in the future. Ideally, these ranges have
  ///    the exact offset and length that will later be read. The cache will combine those
  ///    ranges according to parameters (see constructor).
  ///
  ///    By default, the cache will also start fetching the combined ranges in parallel in
  ///    the background, unless CacheOptions.lazy is set.
  ///
  /// 2. Call WaitFor() to be notified when the given ranges have been read. If
  ///    CacheOptions.lazy is set, I/O will be triggered in the background here instead.
  ///    This can be done in parallel (e.g. if parsing a file, call WaitFor() for each
  ///    chunk of the file that can be parsed in parallel).
  ///
  /// 3. Call Read() to retrieve the actual data for the given ranges.
  ///    A synchronous application may skip WaitFor() and just call Read() - it will still
  ///    benefit from coalescing and parallel fetching.
  class ReadRangeCache {
   public:
    /// Construct a read cache with given options
    explicit ReadRangeCache(InputStream* _stream, CacheOptions _options, MemoryPool* _memoryPool)
        : stream(_stream), options(std::move(_options)), memoryPool(_memoryPool) {}

    ~ReadRangeCache() = default;

    /// Cache the given ranges in the background.
    ///
    /// The caller must ensure that the ranges do not overlap with each other,
    /// nor with previously cached ranges.  Otherwise, behaviour will be undefined.
    void cache(std::vector<ReadRange> ranges);

    /// Read a range previously given to Cache().
    InputStream::BufferSlice read(const ReadRange& range);

    /// Evict cache entries with its range before given boundary.
    void evictEntriesBefore(uint64_t boundary);

   private:
    std::vector<RangeCacheEntry> makeCacheEntries(const std::vector<ReadRange>& ranges) {
      std::vector<RangeCacheEntry> new_entries;
      new_entries.reserve(ranges.size());
      for (const auto& range : ranges) {
        new_entries.emplace_back(range, stream->readAsync(range.offset, range.length, *memoryPool));
      }
      return new_entries;
    }

    InputStream* stream;
    CacheOptions options;

    // Ordered by offset (so as to find a matching region by binary search)
    std::vector<RangeCacheEntry> entries;

    MemoryPool* memoryPool;
  };

}  // namespace orc
