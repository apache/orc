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

#include "Utils.hh"
#include "wrap/gtest-wrapper.h"

#include <limits>

namespace orc {

  TEST(Utils, testAddWithOverflow) {
    uint64_t unsignedResult = 0;
    EXPECT_FALSE(
        addWithOverflow(static_cast<uint64_t>(1), static_cast<uint64_t>(2), &unsignedResult));
    EXPECT_EQ(3, unsignedResult);
    EXPECT_TRUE(addWithOverflow((std::numeric_limits<uint64_t>::max)(), static_cast<uint64_t>(1),
                                &unsignedResult));

    int64_t signedResult = 0;
    EXPECT_FALSE(addWithOverflow(static_cast<int64_t>(-2), static_cast<int64_t>(1), &signedResult));
    EXPECT_EQ(-1, signedResult);
    EXPECT_TRUE(addWithOverflow((std::numeric_limits<int64_t>::max)(), static_cast<int64_t>(1),
                                &signedResult));
    EXPECT_TRUE(addWithOverflow((std::numeric_limits<int64_t>::min)(), static_cast<int64_t>(-1),
                                &signedResult));
  }

  TEST(Utils, testMultiplyWithOverflow) {
    uint64_t unsignedResult = 0;
    EXPECT_FALSE(
        multiplyWithOverflow(static_cast<uint64_t>(6), static_cast<uint64_t>(7), &unsignedResult));
    EXPECT_EQ(42, unsignedResult);
    EXPECT_TRUE(multiplyWithOverflow((std::numeric_limits<uint64_t>::max)(),
                                     static_cast<uint64_t>(2), &unsignedResult));

    int64_t signedResult = 0;
    EXPECT_FALSE(
        multiplyWithOverflow(static_cast<int64_t>(-6), static_cast<int64_t>(7), &signedResult));
    EXPECT_EQ(-42, signedResult);
    EXPECT_TRUE(multiplyWithOverflow((std::numeric_limits<int64_t>::max)(), static_cast<int64_t>(2),
                                     &signedResult));
    EXPECT_TRUE(multiplyWithOverflow((std::numeric_limits<int64_t>::min)(),
                                     static_cast<int64_t>(-1), &signedResult));
  }

}  // namespace orc
