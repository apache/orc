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

  std::vector<ReadRange> coalesceReadRanges(std::vector<ReadRange> ranges, uint64_t holeSizeLimit,
                                            uint64_t rangeSizeLimit) {
    assert(rangeSizeLimit > holeSizeLimit);

    ReadRangeCombiner combiner{holeSizeLimit, rangeSizeLimit};
    return combiner.coalesce(std::move(ranges));
  }

  void ReadRangeCache::cache(std::vector<ReadRange> ranges) {
    ranges =
        coalesceReadRanges(std::move(ranges), options_.holeSizeLimit, options_.rangeSizeLimit);

    std::vector<RangeCacheEntry> new_entries = makeCacheEntries(ranges);
    // Add new entries, themselves ordered by offset
    if (entries_.size() > 0) {
      std::vector<RangeCacheEntry> merged(entries_.size() + new_entries.size());
      std::merge(entries_.begin(), entries_.end(), new_entries.begin(), new_entries.end(),
                 merged.begin());
      entries_ = std::move(merged);
    } else {
      entries_ = std::move(new_entries);
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
}  // namespace orc
