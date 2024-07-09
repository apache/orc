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

#include "orc/Int128.hh"

#include "OrcTest.hh"
#include "wrap/gtest-wrapper.h"

#include <iostream>

namespace orc {

  TEST(Int128, simpleTest) {
    Int128 x = 12;
    Int128 y = 13;
    x += y;
    EXPECT_EQ(25, x.toLong());
    EXPECT_EQ("0x00000000000000000000000000000019", x.toHexString());
    y -= 1;
    EXPECT_EQ("0x0000000000000000000000000000000c", y.toHexString());
    EXPECT_EQ(12, y.toLong());
    EXPECT_EQ(0, y.getHighBits());
    EXPECT_EQ(12, y.getLowBits());
    y -= 20;
    EXPECT_EQ("0xfffffffffffffffffffffffffffffff8", y.toHexString());
    EXPECT_EQ(-8, y.toLong());
    EXPECT_EQ(-1, y.getHighBits());
    EXPECT_EQ(static_cast<uint64_t>(-8), y.getLowBits());
    Int128 z;
    EXPECT_EQ(0, z.toLong());
  }

  TEST(Int128, testNegate) {
    Int128 n = -1000000000000;
    EXPECT_EQ("0xffffffffffffffffffffff172b5af000", n.toHexString());
    n.negate();
    EXPECT_EQ(1000000000000, n.toLong());
    n.abs();
    EXPECT_EQ(1000000000000, n.toLong());
    n.negate();
    EXPECT_EQ(-1000000000000, n.toLong());
    n.abs();
    EXPECT_EQ(1000000000000, n.toLong());

    Int128 big(0x12345678, 0x9abcdef0);
    EXPECT_EQ("0x0000000012345678000000009abcdef0", big.toHexString());
    EXPECT_EQ(305419896, big.getHighBits());
    EXPECT_EQ(2596069104, big.getLowBits());
    big.negate();
    EXPECT_EQ("0xffffffffedcba987ffffffff65432110", big.toHexString());
    EXPECT_EQ(0xffffffffedcba987, big.getHighBits());
    EXPECT_EQ(0xffffffff65432110, big.getLowBits());
    big.negate();
    EXPECT_EQ("0x0000000012345678000000009abcdef0", big.toHexString());
    big.invert();
    EXPECT_EQ("0xffffffffedcba987ffffffff6543210f", big.toHexString());
    big.invert();
    EXPECT_EQ("0x0000000012345678000000009abcdef0", big.toHexString());
  }

  TEST(Int128, testPlus) {
    Int128 n(0x1000, 0xfffffffffffffff0);
    EXPECT_EQ("0x0000000000001000fffffffffffffff0", n.toHexString());
    n += 0x20;
    EXPECT_EQ("0x00000000000010010000000000000010", n.toHexString());
    n -= 0x20;
    EXPECT_EQ("0x0000000000001000fffffffffffffff0", n.toHexString());
    n += Int128(2, 3);
    EXPECT_EQ("0x0000000000001002fffffffffffffff3", n.toHexString());

    Int128 x(static_cast<int64_t>(0xffffffffffffff00), 0x200);
    EXPECT_EQ("0xffffffffffffff000000000000000200", x.toHexString());
    x -= 0x300;
    EXPECT_EQ("0xfffffffffffffeffffffffffffffff00", x.toHexString());
    x -= 0x100;
    EXPECT_EQ("0xfffffffffffffefffffffffffffffe00", x.toHexString());
    x += 0x400;
    EXPECT_EQ("0xffffffffffffff000000000000000200", x.toHexString());
    x -= Int128(1, 2);
    EXPECT_EQ("0xfffffffffffffeff00000000000001fe", x.toHexString());
  }

  TEST(Int128, testLogic) {
    Int128 n = Int128(0x00000000100000002, 0x0000000400000008);
    n |= Int128(0x0000001000000020, 0x0000004000000080);
    EXPECT_EQ("0x00000011000000220000004400000088", n.toHexString());
    n = Int128(0x0000111100002222, 0x0000333300004444);
    n &= Int128(0x0000f00000000f00, 0x000000f00000000f);
    EXPECT_EQ("0x00001000000002000000003000000004", n.toHexString());
  }

  TEST(Int128, testShift) {
    Int128 n(0x123456789abcdef0, 0xfedcba9876543210);
    EXPECT_EQ("0x123456789abcdef0fedcba9876543210", n.toHexString());
    n <<= 0;
    EXPECT_EQ("0x123456789abcdef0fedcba9876543210", n.toHexString());
    n <<= 4;
    EXPECT_EQ("0x23456789abcdef0fedcba98765432100", n.toHexString());
    n <<= 8;
    EXPECT_EQ("0x456789abcdef0fedcba9876543210000", n.toHexString());
    n += 0x99;
    EXPECT_EQ("0x456789abcdef0fedcba9876543210099", n.toHexString());
    n <<= 64;
    EXPECT_EQ("0xcba98765432100990000000000000000", n.toHexString());
    n += 0x312;
    EXPECT_EQ("0xcba98765432100990000000000000312", n.toHexString());
    n <<= 120;
    EXPECT_EQ("0x12000000000000000000000000000000", n.toHexString());
    n += 0x411;
    EXPECT_EQ("0x12000000000000000000000000000411", n.toHexString());
    n <<= 128;
    EXPECT_EQ(0, n.toLong());

    n = Int128(0x123456789abcdef0, 0xfedcba9876543210);
    EXPECT_EQ("0x123456789abcdef0fedcba9876543210", n.toHexString());
    n >>= 0;
    EXPECT_EQ("0x123456789abcdef0fedcba9876543210", n.toHexString());
    n >>= 4;
    EXPECT_EQ("0x0123456789abcdef0fedcba987654321", n.toHexString());
    n >>= 8;
    EXPECT_EQ("0x000123456789abcdef0fedcba9876543", n.toHexString());
    n += Int128(0x2400000000000000, 0x0);
    EXPECT_EQ("0x240123456789abcdef0fedcba9876543", n.toHexString());
    n >>= 64;
    EXPECT_EQ("0x0000000000000000240123456789abcd", n.toHexString());
    n += Int128(0x2400000000000000, 0x0);
    EXPECT_EQ("0x2400000000000000240123456789abcd", n.toHexString());
    n >>= 129;
    EXPECT_EQ(0, n.toLong());
    n = Int128(static_cast<int64_t>(0xfedcba0987654321), 0x1234567890abcdef);
    EXPECT_EQ("0xfedcba09876543211234567890abcdef", n.toHexString());
    n >>= 64;
    EXPECT_EQ("0xfffffffffffffffffedcba0987654321", n.toHexString());
    n = Int128(static_cast<int64_t>(0xfedcba0987654321), 0x1234567890abcdef);
    n >>= 129;
    EXPECT_EQ("0xffffffffffffffffffffffffffffffff", n.toHexString());
    n = Int128(-1, 0xffffffffffffffff);
    n >>= 4;
    EXPECT_EQ("0x0fffffffffffffffffffffffffffffff", n.toHexString());
    n = Int128(-0x100, 0xffffffffffffffff);
    n >>= 68;
    EXPECT_EQ("0xfffffffffffffffffffffffffffffff0", n.toHexString());
  }

  TEST(Int128, testCompare) {
    Int128 x = 123;
    EXPECT_EQ(Int128(123), x);
    EXPECT_EQ(true, x == 123);
    EXPECT_EQ(true, !(x == 124));
    EXPECT_EQ(true, !(x == -124));
    EXPECT_EQ(true, !(x == Int128(2, 123)));
    EXPECT_EQ(true, !(x != 123));
    EXPECT_EQ(true, x != -123);
    EXPECT_EQ(true, x != 124);
    EXPECT_EQ(true, x != Int128(-1, 123));
    x = Int128(0x123, 0x456);
    EXPECT_EQ(true, !(x < Int128(0x123, 0x455)));
    EXPECT_EQ(true, !(x < Int128(0x123, 0x456)));
    EXPECT_EQ(true, x < Int128(0x123, 0x457));
    EXPECT_EQ(true, !(x < Int128(0x122, 0x456)));
    EXPECT_EQ(true, x < Int128(0x124, 0x456));

    EXPECT_EQ(true, !(x <= Int128(0x123, 0x455)));
    EXPECT_EQ(true, x <= Int128(0x123, 0x456));
    EXPECT_EQ(true, x <= Int128(0x123, 0x457));
    EXPECT_EQ(true, !(x <= Int128(0x122, 0x456)));
    EXPECT_EQ(true, x <= Int128(0x124, 0x456));

    EXPECT_EQ(true, x > Int128(0x123, 0x455));
    EXPECT_EQ(true, !(x > Int128(0x123, 0x456)));
    EXPECT_EQ(true, !(x > Int128(0x123, 0x457)));
    EXPECT_EQ(true, x > Int128(0x122, 0x456));
    EXPECT_EQ(true, !(x > Int128(0x124, 0x456)));

    EXPECT_EQ(true, x >= Int128(0x123, 0x455));
    EXPECT_EQ(true, x >= Int128(0x123, 0x456));
    EXPECT_EQ(true, !(x >= Int128(0x123, 0x457)));
    EXPECT_EQ(true, x >= Int128(0x122, 0x456));
    EXPECT_EQ(true, !(x >= Int128(0x124, 0x456)));

    EXPECT_EQ(true, Int128(-3) < Int128(-2));
    EXPECT_EQ(true, Int128(-3) < Int128(0));
    EXPECT_EQ(true, Int128(-3) < Int128(3));
    EXPECT_EQ(true, Int128(0) < Int128(5));
    EXPECT_EQ(true, Int128::minimumValue() < 0);
    EXPECT_EQ(true, Int128(0) < Int128::maximumValue());
    EXPECT_EQ(true, Int128::minimumValue() < Int128::maximumValue());
  }

  TEST(Int128, testHash) {
    EXPECT_EQ(0, Int128().hash());
    EXPECT_EQ(0x123, Int128(0x123).hash());
    EXPECT_EQ(0xc3c3c3c3, Int128(0x0101010102020202, 0x4040404080808080).hash());
    EXPECT_EQ(0x122, Int128(-0x123).hash());
    EXPECT_EQ(0x12345678, Int128(0x1234567800000000, 0x0).hash());
    EXPECT_EQ(0x12345678, Int128(0x12345678, 0x0).hash());
    EXPECT_EQ(0x12345678, Int128(0x0, 0x1234567800000000).hash());
    EXPECT_EQ(0x12345678, Int128(0x0, 0x12345678).hash());
  }

  TEST(Int128, testFitsInLong) {
    EXPECT_EQ(true, Int128(0x0, 0x7fffffffffffffff).fitsInLong());
    EXPECT_EQ(true, !Int128(0x0, 0x8000000000000000).fitsInLong());
    EXPECT_EQ(true, !Int128(-1, 0x7fffffffffffffff).fitsInLong());
    EXPECT_EQ(true, Int128(-1, 0x8000000000000000).fitsInLong());
    EXPECT_EQ(true, !Int128(1, 0x8000000000000000).fitsInLong());
    EXPECT_EQ(true, !Int128(1, 0x7fffffffffffffff).fitsInLong());
    EXPECT_EQ(true, !Int128(-2, 0x8000000000000000).fitsInLong());
    EXPECT_EQ(true, !Int128(-2, 0x7fffffffffffffff).fitsInLong());

    EXPECT_EQ(0x7fffffffffffffff, Int128(0x0, 0x7fffffffffffffff).toLong());
    EXPECT_THROW(Int128(1, 1).toLong(), std::runtime_error);
    EXPECT_EQ(0x8000000000000000, Int128(-1, 0x8000000000000000).toLong());
  }

  TEST(Int128, testMultiply) {
    Int128 x = 2;
    x *= 3;
    EXPECT_EQ(6, x.toLong());
    x *= -4;
    EXPECT_EQ(-24, x.toLong());
    x *= 5;
    EXPECT_EQ(-120, x.toLong());
    x *= -7;
    EXPECT_EQ(840, x.toLong());
    x = Int128(0x0123456776543210, 0x1111222233334444);
    x *= 2;
    EXPECT_EQ(0x02468aceeca86420, x.getHighBits());
    EXPECT_EQ(0x2222444466668888, x.getLowBits());

    x = Int128(0x0534AB4C, 0x59D109ADF9892FCA);
    x *= Int128(0, 0x9033b8c7a);
    EXPECT_EQ("0x2eead9afd0c6e0e929c18da753113e44", x.toHexString());
  }

  TEST(Int128, testMultiplyInt) {
    Int128 x = 2;
    x *= 1;
    EXPECT_EQ(2, x.toLong());
    x *= 2;
    EXPECT_EQ(4, x.toLong());

    x = 5;
    x *= 6432346;
    EXPECT_EQ(6432346 * 5, x.toLong());

    x = (1LL << 62) + (3LL << 34) + 3LL;
    x *= 96;
    EXPECT_EQ("0x00000000000000180000048000000120", x.toHexString());

    x = 1;
    x <<= 126;
    EXPECT_EQ("0x40000000000000000000000000000000", x.toHexString());
    x *= 2;
    EXPECT_EQ("0x80000000000000000000000000000000", x.toHexString());
    x *= 2;
    EXPECT_EQ("0x00000000000000000000000000000000", x.toHexString());
  }

  TEST(Int128, testFillInArray) {
    Int128 x(0x123456789abcdef0, 0x23456789abcdef01);
    uint32_t array[4];
    bool wasNegative;
    EXPECT_EQ(4, x.fillInArray(array, wasNegative));
    EXPECT_EQ(true, !wasNegative);
    EXPECT_EQ(0x12345678, array[0]);
    EXPECT_EQ(0x9abcdef0, array[1]);
    EXPECT_EQ(0x23456789, array[2]);
    EXPECT_EQ(0xabcdef01, array[3]);

    x = 0;
    EXPECT_EQ(0, x.fillInArray(array, wasNegative));
    EXPECT_EQ(true, !wasNegative);

    x = 1;
    EXPECT_EQ(1, x.fillInArray(array, wasNegative));
    EXPECT_EQ(true, !wasNegative);
    EXPECT_EQ(1, array[0]);

    x = -12345;
    EXPECT_EQ(1, x.fillInArray(array, wasNegative));
    EXPECT_EQ(true, wasNegative);
    EXPECT_EQ(12345, array[0]);

    x = 0x80000000;
    EXPECT_EQ(1, x.fillInArray(array, wasNegative));
    EXPECT_EQ(true, !wasNegative);
    EXPECT_EQ(0x80000000, array[0]);

    x = Int128(0, 0x8000000000000000);
    EXPECT_EQ(2, x.fillInArray(array, wasNegative));
    EXPECT_EQ(true, !wasNegative);
    EXPECT_EQ(0x80000000, array[0]);
    EXPECT_EQ(0x0, array[1]);

    x = Int128(0x80000000, 0x123456789abcdef0);
    EXPECT_EQ(3, x.fillInArray(array, wasNegative));
    EXPECT_EQ(true, !wasNegative);
    EXPECT_EQ(0x80000000, array[0]);
    EXPECT_EQ(0x12345678, array[1]);
    EXPECT_EQ(0x9abcdef0, array[2]);
  }

  int64_t fls(uint32_t x);

  TEST(Int128, testFindLastSet) {
    EXPECT_EQ(0, fls(0));
    EXPECT_EQ(1, fls(1));
    EXPECT_EQ(8, fls(0xff));
    EXPECT_EQ(9, fls(0x100));
    EXPECT_EQ(29, fls(0x12345678));
    EXPECT_EQ(31, fls(0x40000000));
    EXPECT_EQ(32, fls(0x80000000));
  }

  void shiftArrayLeft(uint32_t* array, int64_t length, int64_t bits);

  TEST(Int128, testShiftArrayLeft) {
    uint32_t array[5];
    // make sure nothing blows up
    array[0] = 0x12345678;
    shiftArrayLeft(0, 0, 30);
    EXPECT_EQ(0x12345678, array[0]);

    array[0] = 0x12345678;
    shiftArrayLeft(array, 1, 0);
    EXPECT_EQ(0x12345678, array[0]);

    array[0] = 0x12345678;
    array[1] = 0x9abcdef0;
    shiftArrayLeft(array, 1, 3);
    EXPECT_EQ(0x91a2b3c0, array[0]);
    EXPECT_EQ(0x9abcdef0, array[1]);

    array[0] = 0x12345678;
    array[1] = 0x9abcdeff;
    array[2] = 0xfedcba98;
    array[3] = 0x76543210;
    shiftArrayLeft(array, 4, 4);
    EXPECT_EQ(0x23456789, array[0]);
    EXPECT_EQ(0xabcdefff, array[1]);
    EXPECT_EQ(0xedcba987, array[2]);
    EXPECT_EQ(0x65432100, array[3]);

    array[0] = 0;
    array[1] = 0x12345678;
    array[2] = 0x9abcdeff;
    array[3] = 0xfedcba98;
    array[4] = 0x76543210;
    shiftArrayLeft(array, 5, 8);
    EXPECT_EQ(0x00000012, array[0]);
    EXPECT_EQ(0x3456789a, array[1]);
    EXPECT_EQ(0xbcdefffe, array[2]);
    EXPECT_EQ(0xdcba9876, array[3]);
    EXPECT_EQ(0x54321000, array[4]);
  }

  void shiftArrayRight(uint32_t* array, int64_t length, int64_t bits);

  TEST(Int128, testShiftArrayRight) {
    uint32_t array[4];
    // make sure nothing blows up
    array[0] = 0x12345678;
    shiftArrayRight(0, 0, 30);
    EXPECT_EQ(0x12345678, array[0]);

    array[0] = 0x12345678;
    array[1] = 0x9abcdef0;
    shiftArrayRight(array, 1, 3);
    EXPECT_EQ(0x2468acf, array[0]);
    EXPECT_EQ(0x9abcdef0, array[1]);

    array[0] = 0x12345678;
    array[1] = 0x9abcdeff;
    array[2] = 0xfedcba98;
    array[3] = 0x76543210;
    shiftArrayRight(array, 4, 4);
    EXPECT_EQ(0x01234567, array[0]);
    EXPECT_EQ(0x89abcdef, array[1]);
    EXPECT_EQ(0xffedcba9, array[2]);
    EXPECT_EQ(0x87654321, array[3]);
  }

  void fixDivisionSigns(Int128& result, Int128& remainder, bool dividendWasNegative,
                        bool divisorWasNegative);

  TEST(Int128, testFixDivisionSigns) {
    Int128 x = 123;
    Int128 y = 456;
    fixDivisionSigns(x, y, false, false);
    EXPECT_EQ(123, x.toLong());
    EXPECT_EQ(456, y.toLong());

    x = 123;
    y = 456;
    fixDivisionSigns(x, y, false, true);
    EXPECT_EQ(-123, x.toLong());
    EXPECT_EQ(456, y.toLong());

    x = 123;
    y = 456;
    fixDivisionSigns(x, y, true, false);
    EXPECT_EQ(-123, x.toLong());
    EXPECT_EQ(-456, y.toLong());

    x = 123;
    y = 456;
    fixDivisionSigns(x, y, true, true);
    EXPECT_EQ(123, x.toLong());
    EXPECT_EQ(-456, y.toLong());
  }

  void buildFromArray(Int128& value, uint32_t* array, int64_t length);

  TEST(Int128, testBuildFromArray) {
    Int128 result;
    uint32_t array[5] = {0x12345678, 0x9abcdef0, 0xfedcba98, 0x76543210, 0};

    buildFromArray(result, array, 0);
    EXPECT_EQ(0, result.toLong());

    buildFromArray(result, array, 1);
    EXPECT_EQ(0x12345678, result.toLong());

    buildFromArray(result, array, 2);
    EXPECT_EQ(0x123456789abcdef0, result.toLong());

    buildFromArray(result, array, 3);
    EXPECT_EQ("0x00000000123456789abcdef0fedcba98", result.toHexString());

    buildFromArray(result, array, 4);
    EXPECT_EQ("0x123456789abcdef0fedcba9876543210", result.toHexString());

    EXPECT_THROW(buildFromArray(result, array, 5), std::logic_error);
  }

  Int128 singleDivide(uint32_t* dividend, int64_t dividendLength, uint32_t divisor,
                      Int128& remainder, bool dividendWasNegative, bool divisorWasNegative);

  TEST(Int128, testSingleDivide) {
    Int128 remainder;
    uint32_t dividend[4];

    dividend[0] = 23;
    Int128 result = singleDivide(dividend, 1, 5, remainder, true, false);
    EXPECT_EQ(-4, result.toLong());
    EXPECT_EQ(-3, remainder.toLong());

    dividend[0] = 0x100;
    dividend[1] = 0x120;
    dividend[2] = 0x140;
    dividend[3] = 0x160;
    result = singleDivide(dividend, 4, 0x20, remainder, false, false);
    EXPECT_EQ("0x00000008000000090000000a0000000b", result.toHexString());
    EXPECT_EQ(0, remainder.toLong());

    dividend[0] = 0x101;
    dividend[1] = 0x122;
    dividend[2] = 0x143;
    dividend[3] = 0x164;
    result = singleDivide(dividend, 4, 0x20, remainder, false, false);
    EXPECT_EQ("0x00000008080000091000000a1800000b", result.toHexString());
    EXPECT_EQ(4, remainder.toLong());

    dividend[0] = 0x12345678;
    dividend[1] = 0x9abcdeff;
    dividend[2] = 0xfedcba09;
    dividend[3] = 0x87654321;
    result = singleDivide(dividend, 4, 123, remainder, false, false);
    EXPECT_EQ("0x0025e390971c97aaaaa84c7077bc23ed", result.toHexString());
    EXPECT_EQ(0x42, remainder.toLong());
  }

  TEST(Int128, testDivide) {
    Int128 dividend;
    Int128 result;
    Int128 remainder;

    dividend = 0x12345678;
    result = dividend.divide(0x123456789abcdef0, remainder);
    EXPECT_EQ(0, result.toLong());
    EXPECT_EQ(0x12345678, remainder.toLong());

    EXPECT_THROW(dividend.divide(0, remainder), std::runtime_error);

    dividend = Int128(0x123456789abcdeff, 0xfedcba0987654321);
    result = dividend.divide(123, remainder);
    EXPECT_EQ("0x0025e390971c97aaaaa84c7077bc23ed", result.toHexString());
    EXPECT_EQ(0x42, remainder.toLong());

    dividend = Int128(0x111111112fffffff, 0xeeeeeeeedddddddd);
    result = dividend.divide(0x1111111123456789, remainder);
    EXPECT_EQ("0x000000000000000100000000beeeeef7", result.toHexString());
    EXPECT_EQ("0x0000000000000000037d3b3d60479aae", remainder.toHexString());

    dividend = 1234234662345;
    result = dividend.divide(642337, remainder);
    EXPECT_EQ(1921475, result.toLong());
    EXPECT_EQ(175270, remainder.toLong());

    dividend = Int128(0x42395ADC0534AB4C, 0x59D109ADF9892FCA);
    result = dividend.divide(0x1234F09DC19A, remainder);
    EXPECT_EQ("0x000000000003a327c1348bccd2f06c27", result.toHexString());
    EXPECT_EQ("0x000000000000000000000cacef73b954", remainder.toHexString());

    dividend = Int128(0xfffffffffffffff, 0xf000000000000000);
    result = dividend.divide(Int128(0, 0x1000000000000000), remainder);
    EXPECT_EQ("0x0000000000000000ffffffffffffffff", result.toHexString());
    EXPECT_EQ(0, remainder.toLong());

    dividend = Int128(0x4000000000000000, 0);
    result = dividend.divide(Int128(0, 0x400000007fffffff), remainder);
    EXPECT_EQ("0x0000000000000000fffffffe00000007", result.toHexString());
    EXPECT_EQ("0x00000000000000003ffffffa80000007", remainder.toHexString());
  }

  TEST(Int128, testToString) {
    Int128 num = Int128(0x123456789abcdef0, 0xfedcba0987654321);
    EXPECT_EQ("24197857203266734881846307133640229665", num.toString());

    num = Int128(0, 0xab54a98ceb1f0ad2);
    EXPECT_EQ("12345678901234567890", num.toString());

    num = 12345678;
    EXPECT_EQ("12345678", num.toString());

    num = -1234;
    EXPECT_EQ("-1234", num.toString());

    num = Int128(0x13f20d9c2, 0xfff89d38e1c70cb1);
    EXPECT_EQ("98765432109876543210987654321", num.toString());
    num.negate();
    EXPECT_EQ("-98765432109876543210987654321", num.toString());

    num = Int128("10000000000000000000000000000000000000");
    EXPECT_EQ("10000000000000000000000000000000000000", num.toString());

    num = Int128("-1234");
    EXPECT_EQ("-1234", num.toString());

    num = Int128("-12345678901122334455667788990011122233");
    EXPECT_EQ("-12345678901122334455667788990011122233", num.toString());

    num = Int128::maximumValue();
    EXPECT_EQ("170141183460469231731687303715884105727", num.toString());
    num = Int128::minimumValue();
    EXPECT_EQ("-170141183460469231731687303715884105728", num.toString());
  }

  TEST(Int128, testToDecimalString) {
    Int128 num = Int128("98765432109876543210987654321098765432");
    EXPECT_EQ("98765432109876543210987654321098765432", num.toDecimalString(0));
    EXPECT_EQ("987654321098765432109876543210987.65432", num.toDecimalString(5));
    num.negate();
    EXPECT_EQ("-98765432109876543210987654321098765432", num.toDecimalString(0));
    EXPECT_EQ("-987654321098765432109876543210987.65432", num.toDecimalString(5));
    num = 123;
    EXPECT_EQ("12.3", num.toDecimalString(1));
    EXPECT_EQ("0.123", num.toDecimalString(3));
    EXPECT_EQ("0.0123", num.toDecimalString(4));
    EXPECT_EQ("0.00123", num.toDecimalString(5));

    num = -123;
    EXPECT_EQ("-123", num.toDecimalString(0));
    EXPECT_EQ("-12.3", num.toDecimalString(1));
    EXPECT_EQ("-0.123", num.toDecimalString(3));
    EXPECT_EQ("-0.0123", num.toDecimalString(4));
    EXPECT_EQ("-0.00123", num.toDecimalString(5));
  }

  TEST(Int128, testInt128Scale) {
    Int128 num = Int128(10);
    bool overflow = false;

    num = scaleUpInt128ByPowerOfTen(num, 0, overflow);
    EXPECT_FALSE(overflow);
    EXPECT_EQ(Int128(10), num);

    num = scaleUpInt128ByPowerOfTen(num, 5, overflow);
    EXPECT_FALSE(overflow);
    EXPECT_EQ(Int128(1000000), num);

    num = scaleUpInt128ByPowerOfTen(num, 5, overflow);
    EXPECT_FALSE(overflow);
    EXPECT_EQ(Int128(100000000000l), num);

    num = scaleUpInt128ByPowerOfTen(num, 20, overflow);
    EXPECT_FALSE(overflow);
    EXPECT_EQ(Int128("10000000000000000000000000000000"), num);

    scaleUpInt128ByPowerOfTen(num, 10, overflow);
    EXPECT_TRUE(overflow);

    scaleUpInt128ByPowerOfTen(Int128::maximumValue(), 0, overflow);
    EXPECT_FALSE(overflow);

    scaleUpInt128ByPowerOfTen(Int128::maximumValue(), 1, overflow);
    EXPECT_TRUE(overflow);

    num = scaleDownInt128ByPowerOfTen(Int128(10001), 0);
    EXPECT_EQ(Int128(10001), num);

    num = scaleDownInt128ByPowerOfTen(Int128(10001), 2);
    EXPECT_EQ(Int128(100), num);

    num = scaleDownInt128ByPowerOfTen(Int128(10000), 5);
    EXPECT_EQ(Int128(0), num);
  }

  TEST(Int128, testToDecimalStringTrimTrailingZeros) {
    EXPECT_TRUE(Int128(0).toDecimalString(0, false) == "0");
    EXPECT_TRUE(Int128(0).toDecimalString(0, true) == "0");

    EXPECT_TRUE(Int128(0).toDecimalString(4, false) == "0.0000");
    EXPECT_TRUE(Int128(0).toDecimalString(4, true) == "0");

    EXPECT_TRUE(Int128(12340000).toDecimalString(3, false) == "12340.000");
    EXPECT_TRUE(Int128(12340000).toDecimalString(3, true) == "12340");

    EXPECT_TRUE(Int128(12340000).toDecimalString(10, false) == "0.0012340000");
    EXPECT_TRUE(Int128(12340000).toDecimalString(10, true) == "0.001234");

    EXPECT_TRUE(Int128(12340000).toDecimalString(8, false) == "0.12340000");
    EXPECT_TRUE(Int128(12340000).toDecimalString(8, true) == "0.1234");

    EXPECT_TRUE(Int128(-12340000).toDecimalString(3, false) == "-12340.000");
    EXPECT_TRUE(Int128(-12340000).toDecimalString(3, true) == "-12340");

    EXPECT_TRUE(Int128(-12340000).toDecimalString(10, false) == "-0.0012340000");
    EXPECT_TRUE(Int128(-12340000).toDecimalString(10, true) == "-0.001234");

    EXPECT_TRUE(Int128(-12340000).toDecimalString(8, false) == "-0.12340000");
    EXPECT_TRUE(Int128(-12340000).toDecimalString(8, true) == "-0.1234");
  }

  TEST(Int128, testConvertDecimalToDifferentPrecisionScale) {
    // Test convert decimal to different precision/scale
    Int128 num = Int128(1234567890);

    int fromScale = 5;
    int toPrecision = 9;
    int toScale = 5;
    auto pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, true) << pair.second.toString();  // overflow

    fromScale = 5;
    toPrecision = 9;
    toScale = 4;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(123456789));

    fromScale = 5;
    toPrecision = 9;
    toScale = 3;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12345679)) << pair.second.toString();

    fromScale = 5;
    toPrecision = 10;
    toScale = 0;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12346));

    fromScale = 5;
    toPrecision = 10;
    toScale = 2;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(1234568)) << pair.second.toString();

    fromScale = 5;
    toPrecision = 10;
    toScale = 5;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(1234567890)) << pair.second.toString();

    fromScale = 5;
    toPrecision = 10;
    toScale = 6;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);

    fromScale = 5;
    toPrecision = 11;
    toScale = 0;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12346));

    fromScale = 5;
    toPrecision = 11;
    toScale = 3;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12345679)) << pair.second.toString();

    fromScale = 5;
    toPrecision = 11;
    toScale = 6;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12345678900)) << pair.second.toString();

    fromScale = 5;
    toPrecision = 11;
    toScale = 7;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromScale = 5;
    toPrecision = 12;
    toScale = 5;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(1234567890)) << pair.second.toString();

    fromScale = 5;
    toPrecision = 12;
    toScale = 6;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12345678900)) << pair.second.toString();

    fromScale = 5;
    toPrecision = 12;
    toScale = 8;
    pair = convertDecimal(num, fromScale, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromScale = 5;
    toPrecision = 39;
    toScale = 5;
    EXPECT_THROW(convertDecimal(num, fromScale, toPrecision, toScale), std::invalid_argument);

    fromScale = 5;
    toPrecision = 0;
    toScale = 5;
    EXPECT_THROW(convertDecimal(num, fromScale, toPrecision, toScale), std::invalid_argument);

    fromScale = 39;
    toPrecision = 9;
    toScale = -1;
    EXPECT_THROW(convertDecimal(num, fromScale, toPrecision, toScale), std::invalid_argument);

    fromScale = 0;
    toPrecision = 9;
    toScale = 10;
    EXPECT_THROW(convertDecimal(num, fromScale, toPrecision, toScale), std::invalid_argument);

    fromScale = -1;
    toPrecision = 9;
    toScale = 0;
    EXPECT_THROW(convertDecimal(num, fromScale, toPrecision, toScale), std::invalid_argument);

    fromScale = 40;
    toPrecision = 9;
    toScale = 0;
    EXPECT_THROW(convertDecimal(num, fromScale, toPrecision, toScale), std::invalid_argument);

    fromScale = -40;
    toPrecision = 9;
    toScale = 0;
    EXPECT_THROW(convertDecimal(num, fromScale, toPrecision, toScale), std::invalid_argument);
  }

  TEST(Int128, testConvertDecimaFromDouble) {
    double fromDouble = 12345.13579;
    int toPrecision = 4;
    int toScale = 2;
    auto pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    toPrecision = 5;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12345)) << pair.second.toString();

    toPrecision = 5;
    toScale = 1;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    toPrecision = 6;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12345)) << pair.second.toString();

    toPrecision = 6;
    toScale = 1;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(123451)) << pair.second.toString();

    toPrecision = 6;
    toScale = 2;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    toPrecision = 8;
    toScale = 3;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(12345136)) << pair.second.toString();

    fromDouble = -12345.13579;

    toPrecision = 4;
    toScale = 2;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    toPrecision = 5;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(-12345)) << pair.second.toString();

    toPrecision = 5;
    toScale = 1;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    toPrecision = 6;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(-12345)) << pair.second.toString();

    toPrecision = 6;
    toScale = 1;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(-123451)) << pair.second.toString();

    toPrecision = 6;
    toScale = 2;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    toPrecision = 8;
    toScale = 3;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second, Int128(-12345136)) << pair.second.toString();

    fromDouble = pow(10, 37);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), std::to_string(fromDouble).substr(0, 37))
        << pair.second.toString();

    fromDouble = -pow(10, 37);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), std::to_string(fromDouble).substr(0, 38))
        << pair.second.toString();

    fromDouble = -std::ldexp(1.0, 126);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), std::to_string(fromDouble).substr(0, 39))
        << pair.second.toString();

    fromDouble = -std::ldexp(1.0, 127);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromDouble = -std::ldexp(1.0, 127);
    toPrecision = 39;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromDouble = std::ldexp(1.0, 127);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromDouble = std::ldexp(1.0, 127);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromDouble = std::ldexp(1.0, 126);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), std::to_string(fromDouble).substr(0, 38))
        << pair.second.toString();

    fromDouble = 9988776655443322880.0;
    toPrecision = 38;
    toScale = 6;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "9988776655443322880000000") << pair.second.toString();

    toScale = 10;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "99887766554433228800000000000") << pair.second.toString();

    fromDouble = -9988776655443322880.0;
    toPrecision = 38;
    toScale = 6;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "-9988776655443322880000000") << pair.second.toString();

    // 1e19
    fromDouble = 1e19 + 0.5;
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), std::to_string(fromDouble).substr(0, 20))
        << pair.second.toString();

    toPrecision = 38;
    toScale = 3;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "10000000000000000000000") << pair.second.toString();

    // small than 1<<127 but overflow
    fromDouble = 1.5e38;
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    // -2^55
    fromDouble = -std::ldexp(1.0, 55) + 0.5;
    toScale = 3;
    toPrecision = 38;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "-36028797018963968000") << pair.second.toString();

    // -2^50 - 0.5
    fromDouble = -std::ldexp(1.0, 50) - 0.5;
    toScale = 3;
    toPrecision = 38;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "-1125899906842624500") << pair.second.toString();

    fromDouble = std::nan("1");
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromDouble = std::numeric_limits<double>::infinity();
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromDouble = -std::numeric_limits<double>::infinity();
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromDouble = +0.0;
    toPrecision = 38;
    toScale = 5;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "0") << pair.second.toString();

    fromDouble = -0.0;
    toPrecision = 38;
    toScale = 5;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "0") << pair.second.toString();

    fromDouble = 998244353.998244;
    toPrecision = 15;
    toScale = 6;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(), "998244353998244")
        << pair.second.toString();

    toScale = 5;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(), "99824435399824")
        << pair.second.toString();

    toScale = 2;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(), "99824435400")
        << pair.second.toString();

    toScale = 1;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(), "9982443540")
        << pair.second.toString();

    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(), "998244354")
        << pair.second.toString();

    toPrecision = 14;
    toScale = 6;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromDouble = -998244353.998244;
    toPrecision = 15;
    toScale = 6;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(),
              "-998244353998244")
        << pair.second.toString();

    toScale = 2;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(), "-99824435400")
        << pair.second.toString();

    toScale = 1;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(), "-9982443540")
        << pair.second.toString();

    toScale = 0;
    pair = convertDecimal(fromDouble, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromDouble, toPrecision, toScale).second.toString(), "-998244354")
        << pair.second.toString();
  }

  // Test float to decimal conversion like double to decimal conversion.
  TEST(Int128, testConvertDecimalFromFloat) {
    std::pair<bool, Int128> pair;
    float fromFloat;
    int32_t toPrecision;
    int32_t toScale;

    fromFloat = +0.0;
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "0") << pair.second.toString();

    fromFloat = -0.0;
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "0") << pair.second.toString();

    fromFloat = std::numeric_limits<float>::infinity();
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromFloat = -std::numeric_limits<float>::infinity();
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    fromFloat = std::nanf("1");
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    // 2^126
    fromFloat = std::ldexp(1.0f, 126);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "85070591730234615865843651857942052864")
        << pair.second.toString();

    // 2^127
    fromFloat = std::ldexp(1.0f, 127);
    toPrecision = 38;
    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, true);  // overflow

    // 2^70 + 2^69
    fromFloat = std::ldexp(1.0f, 70) + std::ldexp(1.0f, 69);
    toPrecision = 38;
    toScale = 3;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "1770887431076116955136000") << pair.second.toString();

    fromFloat = std::ldexp(1.0f, 70) + std::ldexp(1.0f, 60);
    toPrecision = 38;
    toScale = 3;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "1181744542222018150400000") << pair.second.toString();

    fromFloat = -(std::ldexp(1.0f, 70) + std::ldexp(1.0f, 50));
    toPrecision = 38;
    toScale = 3;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "-1180592746617318146048000") << pair.second.toString();

    fromFloat = std::ldexp(1.0f, 70) - std::ldexp(1.0f, 60);
    toPrecision = 38;
    toScale = 3;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(pair.second.toString(), "1179438699212804456448000") << pair.second.toString();

    fromFloat = 9.998244f;
    toPrecision = 15;
    toScale = 6;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromFloat, toPrecision, toScale).second.toString(), "9998244")
        << pair.second.toString();

    toScale = 2;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromFloat, toPrecision, toScale).second.toString(), "1000")
        << pair.second.toString();

    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromFloat, toPrecision, toScale).second.toString(), "10")
        << pair.second.toString();

    fromFloat = -9.998244f;
    toPrecision = 15;
    toScale = 6;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromFloat, toPrecision, toScale).second.toString(), "-9998244")
        << pair.second.toString();

    toScale = 2;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromFloat, toPrecision, toScale).second.toString(), "-1000")
        << pair.second.toString();

    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromFloat, toPrecision, toScale).second.toString(), "-10")
        << pair.second.toString();

    toPrecision = 1;
    toScale = 0;
    pair = convertDecimal(fromFloat, toPrecision, toScale);
    EXPECT_EQ(pair.first, false);  // no overflow
    EXPECT_EQ(convertDecimal(fromFloat, toPrecision, toScale).second.toString(), "-10")
        << pair.second.toString();
  }

  TEST(Int128, testConvertToDouble) {
    // fit in long
    EXPECT_DOUBLE_EQ(Int128("0").toDouble(), 0);
    EXPECT_DOUBLE_EQ(Int128("1").toDouble(), 1);
    EXPECT_DOUBLE_EQ(Int128("-1").toDouble(), -1);
    EXPECT_DOUBLE_EQ(Int128("-123").toDouble(), -123);
    EXPECT_DOUBLE_EQ(Int128("123").toDouble(), 123);
    EXPECT_DOUBLE_EQ(Int128("-123456").toDouble(), -123456);
    EXPECT_DOUBLE_EQ(Int128("123456").toDouble(), 123456);
    EXPECT_DOUBLE_EQ(Int128("-123456789").toDouble(), -123456789);
    EXPECT_DOUBLE_EQ(Int128("123456789").toDouble(), 123456789);
    EXPECT_DOUBLE_EQ(Int128("-123456789012").toDouble(), -123456789012.0);
    EXPECT_DOUBLE_EQ(Int128("123456789012").toDouble(), 123456789012.0);
    EXPECT_DOUBLE_EQ(Int128("-123456789012345").toDouble(), -123456789012345.0);
    EXPECT_DOUBLE_EQ(Int128("123456789012345").toDouble(), 123456789012345.0);
    EXPECT_DOUBLE_EQ(Int128("-123456789012345678").toDouble(), -123456789012345678.0);
    EXPECT_DOUBLE_EQ(Int128("123456789012345678").toDouble(), 123456789012345678.0);
    EXPECT_DOUBLE_EQ(Int128("-9223372036854775808").toDouble(), -9223372036854775808.0);
    EXPECT_DOUBLE_EQ(Int128("9223372036854775807").toDouble(), 9223372036854775807.0);
    // Not fit in long
    EXPECT_DOUBLE_EQ(Int128("100000000000000000000").toDouble(), 1e20);
    EXPECT_DOUBLE_EQ(Int128("-100000000000000000000").toDouble(), -1e20);
    EXPECT_DOUBLE_EQ(Int128("-12345678901234567890").toDouble(), -12345678901234567890.0);
    EXPECT_DOUBLE_EQ(Int128("-123456789012345678901").toDouble(), -123456789012345678901.0);
    EXPECT_DOUBLE_EQ(Int128("123456789012345678901").toDouble(), 123456789012345678901.0);
    EXPECT_DOUBLE_EQ(Int128("-1234567890123456789012").toDouble(), -1234567890123456789012.0);
    EXPECT_DOUBLE_EQ(Int128("1234567890123456789012").toDouble(), 1234567890123456789012.0);
    EXPECT_DOUBLE_EQ(Int128("-12345678901234567890123").toDouble(), -12345678901234567890123.0);
    EXPECT_DOUBLE_EQ(Int128("12345678901234567890123").toDouble(), 12345678901234567890123.0);
    EXPECT_DOUBLE_EQ(Int128("12345678901234567890123456789").toDouble(),
                     12345678901234567890123456789.0);
    EXPECT_DOUBLE_EQ(Int128("-12345678901234567890123456789").toDouble(),
                     -12345678901234567890123456789.0);
  }

}  // namespace orc
