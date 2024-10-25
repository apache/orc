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

#include <cassert>

#include "Cache.hh"

namespace orc {

  std::vector<ReadRange> ReadRangeCombiner::coalesce(std::vector<ReadRange> ranges) const {
    if (ranges.empty()) {
      return ranges;
    }

    // Remove zero-sized ranges
    auto end = std::remove_if(ranges.begin(), ranges.end(),
                              [](const ReadRange& range) { return range.length == 0; });
    // Sort in position order
    std::sort(ranges.begin(), end, [](const ReadRange& a, const ReadRange& b) {
      return a.offset != b.offset ? a.offset < b.offset : a.length > b.length;
    });

    // Remove ranges that overlap 100%
    std::vector<ReadRange> uniqueRanges;
    uniqueRanges.reserve(ranges.size());
    for (auto it = ranges.begin(); it != end; ++it) {
      if (uniqueRanges.empty() || !uniqueRanges.back().contains(*it)) {
        uniqueRanges.push_back(*it);
      }
    }
    ranges = std::move(uniqueRanges);

    // Skip further processing if ranges is empty after removing zero-sized ranges.
    if (ranges.empty()) {
      return ranges;
    }

#ifndef NDEBUG
    for (size_t i = 0; i < ranges.size() - 1; ++i) {
      const auto& left = ranges[i];
      const auto& right = ranges[i + 1];
      assert(left.offset < right.offset);
      assert(!left.contains(right));
    }
#endif

    std::vector<ReadRange> coalesced;

    auto itr = ranges.begin();
    // Ensure ranges is not empty.
    assert(itr <= ranges.end());
    // Start of the current coalesced range and end (exclusive) of previous range.
    // Both are initialized with the start of first range which is a placeholder value.
    uint64_t coalescedStart = itr->offset;
    uint64_t prevRangeEnd = coalescedStart;

    for (; itr < ranges.end(); ++itr) {
      const uint64_t currentRangeStart = itr->offset;
      const uint64_t currentRangeEnd = currentRangeStart + itr->length;
      // We don't expect to have 0 sized ranges.
      assert(currentRangeStart < currentRangeEnd);

      // At this point, the coalesced range is [coalesced_start, prev_range_end).
      // Stop coalescing if:
      //   - coalesced range is too large, or
      //   - distance (hole/gap) between consecutive ranges is too large.
      if ((currentRangeEnd - coalescedStart > rangeSizeLimit) ||
          (currentRangeStart > prevRangeEnd + holeSizeLimit)) {
        assert(coalescedStart <= prevRangeEnd);
        // Append the coalesced range only if coalesced range size > 0.
        if (prevRangeEnd > coalescedStart) {
          coalesced.push_back({coalescedStart, prevRangeEnd - coalescedStart});
        }
        // Start a new coalesced range.
        coalescedStart = currentRangeStart;
      }

      // Update the prev_range_end with the current range.
      prevRangeEnd = currentRangeEnd;
    }

    // Append the coalesced range only if coalesced range size > 0.
    if (prevRangeEnd > coalescedStart) {
      coalesced.push_back({coalescedStart, prevRangeEnd - coalescedStart});
    }

    assert(coalesced.front().offset == ranges.front().offset);
    assert(coalesced.back().offset + coalesced.back().length ==
           ranges.back().offset + ranges.back().length);
    return coalesced;
  }

  std::vector<ReadRange> coalesceReadRanges(std::vector<ReadRange> ranges, uint64_t holeSizeLimit,
                                            uint64_t rangeSizeLimit) {
    assert(rangeSizeLimit > holeSizeLimit);

    ReadRangeCombiner combiner{holeSizeLimit, rangeSizeLimit};
    return combiner.coalesce(std::move(ranges));
  }

  void ReadRangeCache::cache(std::vector<ReadRange> ranges) {
    ranges = coalesceReadRanges(std::move(ranges), options_.holeSizeLimit, options_.rangeSizeLimit);

    std::vector<RangeCacheEntry> newEntries = makeCacheEntries(ranges);
    // Add new entries, themselves ordered by offset
    if (entries_.size() > 0) {
      std::vector<RangeCacheEntry> merged(entries_.size() + newEntries.size());
      std::merge(entries_.begin(), entries_.end(), newEntries.begin(), newEntries.end(),
                 merged.begin());
      entries_ = std::move(merged);
    } else {
      entries_ = std::move(newEntries);
    }
  }

  InputStream::BufferSlice ReadRangeCache::read(const ReadRange& range) {
    if (range.length == 0) {
      return {std::make_shared<InputStream::Buffer>(*memoryPool_, 0), 0, 0};
    }

    const auto it = std::lower_bound(entries_.begin(), entries_.end(), range,
                                     [](const RangeCacheEntry& entry, const ReadRange& range) {
                                       return entry.range.offset + entry.range.length <
                                              range.offset + range.length;
                                     });

    if (it == entries_.end() || !it->range.contains(range)) {
      return {};
    }

    auto buffer = it->future.get();
    return InputStream::BufferSlice{buffer, range.offset - it->range.offset, range.length};
  }

  void ReadRangeCache::evictEntriesBefore(uint64_t boundary) {
    auto it = std::lower_bound(entries_.begin(), entries_.end(), boundary,
                               [](const RangeCacheEntry& entry, uint64_t offset) {
                                 return entry.range.offset + entry.range.length < offset;
                               });
    entries_.erase(entries_.begin(), it);
  }

    std::vector<RangeCacheEntry> ReadRangeCache::makeCacheEntries(const std::vector<ReadRange>& ranges) {
      std::vector<RangeCacheEntry> newEntries;
      newEntries.reserve(ranges.size());
      for (const auto& range : ranges) {
        newEntries.emplace_back(range,
                                 stream_->readAsync(range.offset, range.length, *memoryPool_));
      }
      return newEntries;
    }

}  // namespace orc
