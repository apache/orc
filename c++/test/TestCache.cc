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

#include <cstring>

#include "MemoryInputStream.hh"
#include "io/Cache.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestReadRangeCombiner, testBasics) {
    ReadRangeCombiner combinator{0, 100};
    /// Ranges with partial overlap and identical offsets
    std::vector<ReadRange> ranges{{0, 15}, {5, 11}, {5, 15}};
    std::vector<ReadRange> result = combinator.coalesce(std::move(ranges));
    std::vector<ReadRange> expect{{0, 20}};
    ASSERT_EQ(result, expect);
  }

  TEST(TestCoalesceReadRanges, testBasics) {
    auto check = [](std::vector<ReadRange> ranges, std::vector<ReadRange> expected) -> void {
      const uint64_t holeSizeLimit = 9;
      const uint64_t rangeSizeLimit = 99;
      auto coalesced = ReadRangeCombiner::coalesceReadRanges(ranges, holeSizeLimit, rangeSizeLimit);
      ASSERT_EQ(coalesced, expected);
    };

    check({}, {});
    // Zero sized range that ends up in empty list
    check({{110, 0}}, {});
    // Combination on 1 zero sized range and 1 non-zero sized range
    check({{110, 10}, {120, 0}}, {{110, 10}});
    // 1 non-zero sized range
    check({{110, 10}}, {{110, 10}});
    // No holes + unordered ranges
    check({{130, 10}, {110, 10}, {120, 10}}, {{110, 30}});
    // No holes
    check({{110, 10}, {120, 10}, {130, 10}}, {{110, 30}});
    // Small holes only
    check({{110, 11}, {130, 11}, {150, 11}}, {{110, 51}});
    // Large holes
    check({{110, 10}, {130, 10}}, {{110, 10}, {130, 10}});
    check({{110, 11}, {130, 11}, {150, 10}, {170, 11}, {190, 11}}, {{110, 50}, {170, 31}});

    // With zero-sized ranges
    check({{110, 11}, {130, 0}, {130, 11}, {145, 0}, {150, 11}, {200, 0}}, {{110, 51}});

    // No holes but large ranges
    check({{110, 100}, {210, 100}}, {{110, 100}, {210, 100}});
    // Small holes and large range in the middle (*)
    check({{110, 10}, {120, 11}, {140, 100}, {240, 11}, {260, 11}},
          {{110, 21}, {140, 100}, {240, 31}});
    // Mid-size ranges that would turn large after coalescing
    check({{100, 50}, {150, 50}}, {{100, 50}, {150, 50}});
    check({{100, 30}, {130, 30}, {160, 30}, {190, 30}, {220, 30}}, {{100, 90}, {190, 60}});

    // Same as (*) but unsorted
    check({{140, 100}, {120, 11}, {240, 11}, {110, 10}, {260, 11}},
          {{110, 21}, {140, 100}, {240, 31}});

    // Completely overlapping ranges should be eliminated
    check({{20, 5}, {20, 5}, {21, 2}}, {{20, 5}});
  }

  TEST(TestReadRangeCache, testBasics) {
    std::string data = "abcdefghijklmnopqrstuvwxyz";

    CacheOptions options;
    options.holeSizeLimit = 2;
    options.rangeSizeLimit = 10;

    auto file = std::make_shared<MemoryInputStream>(data.data(), data.size());
    ReadRangeCache cache(file.get(), options, getDefaultPool());

    cache.cache({{1, 2}, {3, 2}, {8, 2}, {20, 2}, {25, 0}});
    cache.cache({{10, 4}, {14, 0}, {15, 4}});

    auto assert_slice_equal = [](const BufferSlice& slice, const std::string& expected) {
      ASSERT_TRUE(slice.buffer);
      ASSERT_EQ(expected, std::string_view(slice.buffer->data() + slice.offset, slice.length));
    };

    BufferSlice slice;

    slice = cache.read({20, 2});
    assert_slice_equal(slice, "uv");

    slice = cache.read({1, 2});
    assert_slice_equal(slice, "bc");

    slice = cache.read({3, 2});
    assert_slice_equal(slice, "de");

    slice = cache.read({8, 2});
    assert_slice_equal(slice, "ij");

    slice = cache.read({10, 4});
    assert_slice_equal(slice, "klmn");

    slice = cache.read({15, 4});
    assert_slice_equal(slice, "pqrs");

    // Zero-sized
    slice = cache.read({14, 0});
    assert_slice_equal(slice, "");
    slice = cache.read({25, 0});
    assert_slice_equal(slice, "");

    // Non-cached ranges
    ASSERT_FALSE(cache.read({20, 3}).buffer);
    ASSERT_FALSE(cache.read({19, 3}).buffer);
    ASSERT_FALSE(cache.read({0, 3}).buffer);
    ASSERT_FALSE(cache.read({25, 2}).buffer);

    // Release cache entries before 10. After that cache entries would be: {10, 9}, {20, 2}
    cache.evictEntriesBefore(15);
    ASSERT_FALSE(cache.read({1, 2}).buffer);
    ASSERT_FALSE(cache.read({8, 2}).buffer);
    slice = cache.read({10, 4});
    assert_slice_equal(slice, "klmn");
    slice = cache.read({20, 2});
    assert_slice_equal(slice, "uv");
  }
}  // namespace orc
