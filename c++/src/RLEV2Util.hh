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

#ifndef ORC_RLEV2UTIL_HH
#define ORC_RLEV2UTIL_HH

#include "RLEv2.hh"

namespace orc {
  inline uint32_t decodeBitWidth(uint32_t n) {
    if (n <= FixedBitSizes::TWENTYFOUR) {
      return n + 1;
    } else if (n == FixedBitSizes::TWENTYSIX) {
      return 26;
    } else if (n == FixedBitSizes::TWENTYEIGHT) {
      return 28;
    } else if (n == FixedBitSizes::THIRTY) {
      return 30;
    } else if (n == FixedBitSizes::THIRTYTWO) {
      return 32;
    } else if (n == FixedBitSizes::FORTY) {
      return 40;
    } else if (n == FixedBitSizes::FORTYEIGHT) {
      return 48;
    } else if (n == FixedBitSizes::FIFTYSIX) {
      return 56;
    } else {
      return 64;
    }
  }

  inline uint32_t getClosestFixedBits(uint32_t n) {
    if (n == 0) {
      return 1;
    }

    if (n >= 1 && n <= 24) {
      return n;
    } else if (n <= 26) {
      return 26;
    } else if (n <= 28) {
      return 28;
    } else if (n <= 30) {
      return 30;
    } else if (n <= 32) {
      return 32;
    } else if (n <= 40) {
      return 40;
    } else if (n <= 48) {
      return 48;
    } else if (n <= 56) {
      return 56;
    } else {
      return 64;
    }
  }

  inline uint32_t getClosestAlignedFixedBits(uint32_t n) {
    if (n == 0 ||  n == 1) {
      return 1;
    } else if (n <= 2) {
      return 2;
    } else if (n <= 4) {
      return 4;
    } else if (n <= 8) {
      return 8;
    } else if (n <= 16) {
      return 16;
    } else if (n <= 24) {
      return 24;
    } else if (n <= 32) {
      return 32;
    } else if (n <= 40) {
      return 40;
    } else if (n <= 48) {
      return 48;
    } else if (n <= 56) {
      return 56;
    } else {
      return 64;
    }
  }

  inline uint32_t encodeBitWidth(uint32_t n) {
    n = getClosestFixedBits(n);

    if (n >= 1 && n <= 24) {
      return n - 1;
    } else if (n <= 26) {
      return FixedBitSizes::TWENTYSIX;
    } else if (n <= 28) {
      return FixedBitSizes::TWENTYEIGHT;
    } else if (n <= 30) {
      return FixedBitSizes::THIRTY;
    } else if (n <= 32) {
      return FixedBitSizes::THIRTYTWO;
    } else if (n <= 40) {
      return FixedBitSizes::FORTY;
    } else if (n <= 48) {
      return FixedBitSizes::FORTYEIGHT;
    } else if (n <= 56) {
      return FixedBitSizes::FIFTYSIX;
    } else {
      return FixedBitSizes::SIXTYFOUR;
    }
  }

  inline uint32_t findClosestNumBits(int64_t value) {
    if (value < 0) {
      return getClosestFixedBits(64);
    }

    uint32_t count = 0;
    while (value != 0) {
      count++;
      value = value >> 1;
    }
    return getClosestFixedBits(count);
  }

  inline bool isSafeSubtract(long left, long right) {
    return ((left ^ right) >= 0) | ((left ^ (left - right)) >= 0);
  }

  inline uint32_t RleEncoderV2::getOpCode(EncodingType encoding) {
    return static_cast<uint32_t >(encoding << 6);
  }
}

#endif //ORC_RLEV2UTIL_HH
