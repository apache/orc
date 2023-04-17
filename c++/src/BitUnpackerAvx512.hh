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

#ifndef ORC_BIT_UNPACKER_AVX512_HH
#define ORC_BIT_UNPACKER_AVX512_HH

// Mingw-w64 defines strcasecmp in string.h
#if defined(_WIN32) && !defined(strcasecmp)
#include <string.h>
#define strcasecmp stricmp
#else
#include <strings.h>
#endif

#include <immintrin.h>
#include <cstdint>
#include <vector>

namespace orc {
#define ORC_VECTOR_BITS_2_BYTE(x) \
  (((x) + 7u) >> 3u) /**< Convert a number of bits to a number of bytes */
#define ORC_VECTOR_ONE_64U (1ULL)
#define ORC_VECTOR_MAX_16U 0xFFFF     /**< Max value for uint16_t */
#define ORC_VECTOR_MAX_32U 0xFFFFFFFF /**< Max value for uint32_t */
#define ORC_VECTOR_BYTE_WIDTH 8u      /**< Byte width in bits */
#define ORC_VECTOR_WORD_WIDTH 16u     /**< Word width in bits */
#define ORC_VECTOR_DWORD_WIDTH 32u    /**< Dword width in bits */
#define ORC_VECTOR_QWORD_WIDTH 64u    /**< Qword width in bits */
#define ORC_VECTOR_BIT_MASK(x) \
  ((ORC_VECTOR_ONE_64U << (x)) - 1u) /**< Bit mask below bit position */

#define ORC_VECTOR_BITS_2_WORD(x) \
  (((x) + 15u) >> 4u) /**< Convert a number of bits to a number of words */
#define ORC_VECTOR_BITS_2_DWORD(x) \
  (((x) + 31u) >> 5u) /**< Convert a number of bits to a number of double words */

  // ------------------------------------ 3u -----------------------------------------
  static const uint8_t shuffleIdxTable3u_0[64] = {
      1u, 0u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 4u, 3u, 5u, 4u, 6u, 5u, 1u, 0u, 1u, 0u, 2u, 1u,
      3u, 2u, 4u, 3u, 4u, 3u, 5u, 4u, 6u, 5u, 1u, 0u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 4u, 3u,
      5u, 4u, 6u, 5u, 1u, 0u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 4u, 3u, 5u, 4u, 6u, 5u};
  static const uint8_t shuffleIdxTable3u_1[64] = {
      0u, 0u, 1u, 0u, 2u, 1u, 3u, 2u, 3u, 2u, 4u, 3u, 5u, 4u, 6u, 5u, 0u, 0u, 1u, 0u, 2u, 1u,
      3u, 2u, 3u, 2u, 4u, 3u, 5u, 4u, 6u, 5u, 0u, 0u, 1u, 0u, 2u, 1u, 3u, 2u, 3u, 2u, 4u, 3u,
      5u, 4u, 6u, 5u, 0u, 0u, 1u, 0u, 2u, 1u, 3u, 2u, 3u, 2u, 4u, 3u, 5u, 4u, 6u, 5u};
  static const uint16_t shiftTable3u_0[32] = {13u, 7u,  9u,  11u, 13u, 7u,  9u,  11u, 13u, 7u,  9u,
                                              11u, 13u, 7u,  9u,  11u, 13u, 7u,  9u,  11u, 13u, 7u,
                                              9u,  11u, 13u, 7u,  9u,  11u, 13u, 7u,  9u,  11u};
  static const uint16_t shiftTable3u_1[32] = {6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u,
                                              0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u,
                                              2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u};
  static const uint16_t permutexIdxTable3u[32] = {
      0u, 1u, 2u, 0x0, 0x0, 0x0, 0x0, 0x0, 3u, 4u,  5u,  0x0, 0x0, 0x0, 0x0, 0x0,
      6u, 7u, 8u, 0x0, 0x0, 0x0, 0x0, 0x0, 9u, 10u, 11u, 0x0, 0x0, 0x0, 0x0, 0x0};

  // ------------------------------------ 5u -----------------------------------------
  static const uint8_t shuffleIdxTable5u_0[64] = {
      1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u, 8u, 7u, 9u, 8u, 1u, 0u, 2u, 1u, 3u, 2u,
      4u, 3u, 6u, 5u, 7u, 6u, 8u, 7u, 9u, 8u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u,
      8u, 7u, 9u, 8u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u, 8u, 7u, 9u, 8u};
  static const uint8_t shuffleIdxTable5u_1[64] = {
      1u, 0u, 2u,  1u, 3u, 2u, 5u, 4u, 6u,  5u, 7u, 6u, 8u, 7u, 10u, 9u, 1u, 0u, 2u,  1u, 3u, 2u,
      5u, 4u, 6u,  5u, 7u, 6u, 8u, 7u, 10u, 9u, 1u, 0u, 2u, 1u, 3u,  2u, 5u, 4u, 6u,  5u, 7u, 6u,
      8u, 7u, 10u, 9u, 1u, 0u, 2u, 1u, 3u,  2u, 5u, 4u, 6u, 5u, 7u,  6u, 8u, 7u, 10u, 9u};
  static const uint16_t shiftTable5u_0[32] = {11u, 9u,  7u,  5u, 11u, 9u,  7u,  5u, 11u, 9u,  7u,
                                              5u,  11u, 9u,  7u, 5u,  11u, 9u,  7u, 5u,  11u, 9u,
                                              7u,  5u,  11u, 9u, 7u,  5u,  11u, 9u, 7u,  5u};
  static const uint16_t shiftTable5u_1[32] = {2u, 4u, 6u, 0u, 2u, 4u, 6u, 0u, 2u, 4u, 6u,
                                              0u, 2u, 4u, 6u, 0u, 2u, 4u, 6u, 0u, 2u, 4u,
                                              6u, 0u, 2u, 4u, 6u, 0u, 2u, 4u, 6u, 0u};
  static const uint16_t permutexIdxTable5u[32] = {
      0u,  1u,  2u,  3u,  4u,  0x0, 0x0, 0x0, 5u,  6u,  7u,  8u,  9u,  0x0, 0x0, 0x0,
      10u, 11u, 12u, 13u, 14u, 0x0, 0x0, 0x0, 15u, 16u, 17u, 18u, 19u, 0x0, 0x0, 0x0};

  // ------------------------------------ 6u -----------------------------------------
  static const uint8_t shuffleIdxTable6u_0[64] = {
      1u, 0u, 2u, 1u, 4u, 3u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 11u, 10u,
      1u, 0u, 2u, 1u, 4u, 3u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 11u, 10u,
      1u, 0u, 2u, 1u, 4u, 3u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 11u, 10u,
      1u, 0u, 2u, 1u, 4u, 3u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 11u, 10u};
  static const uint8_t shuffleIdxTable6u_1[64] = {
      1u, 0u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u, 9u, 8u, 10u, 9u, 12u, 11u,
      1u, 0u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u, 9u, 8u, 10u, 9u, 12u, 11u,
      1u, 0u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u, 9u, 8u, 10u, 9u, 12u, 11u,
      1u, 0u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u, 9u, 8u, 10u, 9u, 12u, 11u};
  static const uint16_t shiftTable6u_0[32] = {10u, 6u,  10u, 6u,  10u, 6u,  10u, 6u,  10u, 6u,  10u,
                                              6u,  10u, 6u,  10u, 6u,  10u, 6u,  10u, 6u,  10u, 6u,
                                              10u, 6u,  10u, 6u,  10u, 6u,  10u, 6u,  10u, 6u};
  static const uint16_t shiftTable6u_1[32] = {4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u,
                                              0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u,
                                              4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u};
  static const uint32_t permutexIdxTable6u[16] = {0u, 1u, 2u, 0x0, 3u, 4u,  5u,  0x0,
                                                  6u, 7u, 8u, 0x0, 9u, 10u, 11u, 0x0};

  // ------------------------------------ 7u -----------------------------------------
  static const uint8_t shuffleIdxTable7u_0[64] = {
      1u, 0u, 2u, 1u, 4u, 3u, 6u, 5u, 8u, 7u, 9u, 8u, 11u, 10u, 13u, 12u,
      1u, 0u, 2u, 1u, 4u, 3u, 6u, 5u, 8u, 7u, 9u, 8u, 11u, 10u, 13u, 12u,
      1u, 0u, 2u, 1u, 4u, 3u, 6u, 5u, 8u, 7u, 9u, 8u, 11u, 10u, 13u, 12u,
      1u, 0u, 2u, 1u, 4u, 3u, 6u, 5u, 8u, 7u, 9u, 8u, 11u, 10u, 13u, 12u};
  static const uint8_t shuffleIdxTable7u_1[64] = {
      1u, 0u, 3u, 2u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 12u, 11u, 14u, 13u,
      1u, 0u, 3u, 2u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 12u, 11u, 14u, 13u,
      1u, 0u, 3u, 2u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 12u, 11u, 14u, 13u,
      1u, 0u, 3u, 2u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 12u, 11u, 14u, 13u};
  static const uint16_t shiftTable7u_0[32] = {9u, 3u, 5u, 7u, 9u, 3u, 5u, 7u, 9u, 3u, 5u,
                                              7u, 9u, 3u, 5u, 7u, 9u, 3u, 5u, 7u, 9u, 3u,
                                              5u, 7u, 9u, 3u, 5u, 7u, 9u, 3u, 5u, 7u};
  static const uint16_t shiftTable7u_1[32] = {6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u,
                                              0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u,
                                              2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u};
  static const uint16_t permutexIdxTable7u[32] = {
      0u,  1u,  2u,  3u,  4u,  5u,  6u,  0x0, 7u,  8u,  9u,  10u, 11u, 12u, 13u, 0x0,
      14u, 15u, 16u, 17u, 18u, 19u, 20u, 0x0, 21u, 22u, 23u, 24u, 25u, 26u, 27u, 0x0};

  // ------------------------------------ 9u -----------------------------------------
  static const uint16_t permutexIdxTable9u_0[32] = {
      0u, 1u,  1u,  2u,  2u,  3u,  3u,  4u,  4u,  5u,  5u,  6u,  6u,  7u,  7u,  8u,
      9u, 10u, 10u, 11u, 11u, 12u, 12u, 13u, 13u, 14u, 14u, 15u, 15u, 16u, 16u, 17u};
  static const uint16_t permutexIdxTable9u_1[32] = {
      0u, 1u,  1u,  2u,  2u,  3u,  3u,  4u,  5u,  6u,  6u,  7u,  7u,  8u,  8u,  9u,
      9u, 10u, 10u, 11u, 11u, 12u, 12u, 13u, 14u, 15u, 15u, 16u, 16u, 17u, 17u, 18u};
  static const uint32_t shiftTable9u_0[16] = {0u, 2u, 4u, 6u, 8u, 10u, 12u, 14u,
                                              0u, 2u, 4u, 6u, 8u, 10u, 12u, 14u};
  static const uint32_t shiftTable9u_1[16] = {7u, 5u, 3u, 1u, 15u, 13u, 11u, 9u,
                                              7u, 5u, 3u, 1u, 15u, 13u, 11u, 9u};

  static const uint8_t shuffleIdxTable9u_0[64] = {
      1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 5u, 4u, 6u, 5u, 7u, 6u, 8u, 7u, 1u, 0u, 2u, 1u, 3u, 2u,
      4u, 3u, 5u, 4u, 6u, 5u, 7u, 6u, 8u, 7u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 5u, 4u, 6u, 5u,
      7u, 6u, 8u, 7u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 5u, 4u, 6u, 5u, 7u, 6u, 8u, 7u};
  static const uint16_t shiftTable9u_2[32] = {7u, 6u, 5u, 4u, 3u, 2u, 1u, 0u, 7u, 6u, 5u,
                                              4u, 3u, 2u, 1u, 0u, 7u, 6u, 5u, 4u, 3u, 2u,
                                              1u, 0u, 7u, 6u, 5u, 4u, 3u, 2u, 1u, 0u};
  static const uint64_t gatherIdxTable9u[8] = {0u, 8u, 9u, 17u, 18u, 26u, 27u, 35u};

  // ------------------------------------ 10u -----------------------------------------
  static const uint8_t shuffleIdxTable10u_0[64] = {
      1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u, 8u, 7u, 9u, 8u, 1u, 0u, 2u, 1u, 3u, 2u,
      4u, 3u, 6u, 5u, 7u, 6u, 8u, 7u, 9u, 8u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u,
      8u, 7u, 9u, 8u, 1u, 0u, 2u, 1u, 3u, 2u, 4u, 3u, 6u, 5u, 7u, 6u, 8u, 7u, 9u, 8u};
  static const uint16_t shiftTable10u[32] = {6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u,
                                             0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u,
                                             2u, 0u, 6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u};
  static const uint16_t permutexIdxTable10u[32] = {
      0u,  1u,  2u,  3u,  4u,  0x0, 0x0, 0x0, 5u,  6u,  7u,  8u,  9u,  0x0, 0x0, 0x0,
      10u, 11u, 12u, 13u, 14u, 0x0, 0x0, 0x0, 15u, 16u, 17u, 18u, 19u, 0x0, 0x0, 0x0};

  // ------------------------------------ 11u -----------------------------------------
  static const uint16_t permutexIdxTable11u_0[32] = {
      0u,  1u,  1u,  2u,  2u,  3u,  4u,  5u,  5u,  6u,  6u,  7u,  8u,  9u,  9u,  10u,
      11u, 12u, 12u, 13u, 13u, 14u, 15u, 16u, 16u, 17u, 17u, 18u, 19u, 20u, 20u, 21u};
  static const uint16_t permutexIdxTable11u_1[32] = {
      0u,  1u,  2u,  3u,  3u,  4u,  4u,  5u,  6u,  7u,  7u,  8u,  8u,  9u,  10u, 11u,
      11u, 12u, 13u, 14u, 14u, 15u, 15u, 16u, 17u, 18u, 18u, 19u, 19u, 20u, 21u, 22u};
  static const uint32_t shiftTable11u_0[16] = {0u, 6u, 12u, 2u, 8u, 14u, 4u, 10u,
                                               0u, 6u, 12u, 2u, 8u, 14u, 4u, 10u};
  static const uint32_t shiftTable11u_1[16] = {5u, 15u, 9u, 3u, 13u, 7u, 1u, 11u,
                                               5u, 15u, 9u, 3u, 13u, 7u, 1u, 11u};

  static const uint8_t shuffleIdxTable11u_0[64] = {
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u,
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u,
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u,
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u};
  static const uint8_t shuffleIdxTable11u_1[64] = {
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 0u, 8u, 7u, 6u, 0u, 11u, 10u, 9u, 0u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 0u, 8u, 7u, 6u, 0u, 11u, 10u, 9u, 0u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 0u, 8u, 7u, 6u, 0u, 11u, 10u, 9u, 0u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 0u, 8u, 7u, 6u, 0u, 11u, 10u, 9u, 0u};
  static const uint32_t shiftTable11u_2[16] = {21u, 15u, 17u, 19u, 21u, 15u, 17u, 19u,
                                               21u, 15u, 17u, 19u, 21u, 15u, 17u, 19u};
  static const uint32_t shiftTable11u_3[16] = {6u, 4u, 10u, 8u, 6u, 4u, 10u, 8u,
                                               6u, 4u, 10u, 8u, 6u, 4u, 10u, 8u};
  static const uint64_t gatherIdxTable11u[8] = {0u, 8u, 11u, 19u, 22u, 30u, 33u, 41u};

  // ------------------------------------ 12u -----------------------------------------
  static const uint8_t shuffleIdxTable12u_0[64] = {
      1u, 0u, 2u, 1u, 4u, 3u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 11u, 10u,
      1u, 0u, 2u, 1u, 4u, 3u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 11u, 10u,
      1u, 0u, 2u, 1u, 4u, 3u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 11u, 10u,
      1u, 0u, 2u, 1u, 4u, 3u, 5u, 4u, 7u, 6u, 8u, 7u, 10u, 9u, 11u, 10u};
  static const uint16_t shiftTable12u[32] = {4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u,
                                             0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u,
                                             4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u};
  static const uint32_t permutexIdxTable12u[16] = {0u, 1u, 2u, 0x0, 3u, 4u,  5u,  0x0,
                                                   6u, 7u, 8u, 0x0, 9u, 10u, 11u, 0x0};

  // ------------------------------------ 13u -----------------------------------------
  static const uint16_t permutexIdxTable13u_0[32] = {
      0u,  1u,  1u,  2u,  3u,  4u,  4u,  5u,  6u,  7u,  8u,  9u,  9u,  10u, 11u, 12u,
      13u, 14u, 14u, 15u, 16u, 17u, 17u, 18u, 19u, 20u, 21u, 22u, 22u, 23u, 24u, 25u};
  static const uint16_t permutexIdxTable13u_1[32] = {
      0u,  1u,  2u,  3u,  4u,  5u,  5u,  6u,  7u,  8u,  8u,  9u,  10u, 11u, 12u, 13u,
      13u, 14u, 15u, 16u, 17u, 18u, 18u, 19u, 20u, 21u, 21u, 22u, 23u, 24u, 25u, 26u};
  static const uint32_t shiftTable13u_0[16] = {0u, 10u, 4u, 14u, 8u, 2u, 12u, 6u,
                                               0u, 10u, 4u, 14u, 8u, 2u, 12u, 6u};
  static const uint32_t shiftTable13u_1[16] = {3u, 9u, 15u, 5u, 11u, 1u, 7u, 13u,
                                               3u, 9u, 15u, 5u, 11u, 1u, 7u, 13u};

  static const uint8_t shuffleIdxTable13u_0[64] = {
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u};
  static const uint8_t shuffleIdxTable13u_1[64] = {
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 0u, 10u, 9u, 8u, 0u, 13u, 12u, 11u, 0u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 0u, 10u, 9u, 8u, 0u, 13u, 12u, 11u, 0u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 0u, 10u, 9u, 8u, 0u, 13u, 12u, 11u, 0u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 0u, 10u, 9u, 8u, 0u, 13u, 12u, 11u, 0u};
  static const uint32_t shiftTable13u_2[16] = {19u, 17u, 15u, 13u, 19u, 17u, 15u, 13u,
                                               19u, 17u, 15u, 13u, 19u, 17u, 15u, 13u};
  static const uint32_t shiftTable13u_3[16] = {10u, 12u, 6u, 8u, 10u, 12u, 6u, 8u,
                                               10u, 12u, 6u, 8u, 10u, 12u, 6u, 8u};
  static const uint64_t gatherIdxTable13u[8] = {0u, 8u, 13u, 21u, 26u, 34u, 39u, 47u};

  // ------------------------------------ 14u -----------------------------------------
  static const uint8_t shuffleIdxTable14u_0[64] = {
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 13u, 12u, 11u, 10u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 13u, 12u, 11u, 10u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 13u, 12u, 11u, 10u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 13u, 12u, 11u, 10u};
  static const uint8_t shuffleIdxTable14u_1[64] = {
      3u, 2u, 1u, 0u, 7u, 6u, 5u, 0u, 10u, 9u, 8u, 0u, 14u, 13u, 12u, 0u,
      3u, 2u, 1u, 0u, 7u, 6u, 5u, 0u, 10u, 9u, 8u, 0u, 14u, 13u, 12u, 0u,
      3u, 2u, 1u, 0u, 7u, 6u, 5u, 0u, 10u, 9u, 8u, 0u, 14u, 13u, 12u, 0u,
      3u, 2u, 1u, 0u, 7u, 6u, 5u, 0u, 10u, 9u, 8u, 0u, 14u, 13u, 12u, 0u};
  static const uint32_t shiftTable14u_0[16] = {18u, 14u, 18u, 14u, 18u, 14u, 18u, 14u,
                                               18u, 14u, 18u, 14u, 18u, 14u, 18u, 14u};
  static const uint32_t shiftTable14u_1[16] = {12u, 8u, 12u, 8u, 12u, 8u, 12u, 8u,
                                               12u, 8u, 12u, 8u, 12u, 8u, 12u, 8u};
  static const uint16_t permutexIdxTable14u[32] = {
      0u,  1u,  2u,  3u,  4u,  5u,  6u,  0x0, 7u,  8u,  9u,  10u, 11u, 12u, 13u, 0x0,
      14u, 15u, 16u, 17u, 18u, 19u, 20u, 0x0, 21u, 22u, 23u, 24u, 25u, 26u, 27u, 0x0};

  // ------------------------------------ 15u -----------------------------------------
  static const uint16_t permutexIdxTable15u_0[32] = {
      0u,  1u,  1u,  2u,  3u,  4u,  5u,  6u,  7u,  8u,  9u,  10u, 11u, 12u, 13u, 14u,
      15u, 16u, 16u, 17u, 18u, 19u, 20u, 21u, 22u, 23u, 24u, 25u, 26u, 27u, 28u, 29u};
  static const uint16_t permutexIdxTable15u_1[32] = {
      0u,  1u,  2u,  3u,  4u,  5u,  6u,  7u,  8u,  9u,  10u, 11u, 12u, 13u, 14u, 15u,
      15u, 16u, 17u, 18u, 19u, 20u, 21u, 22u, 23u, 24u, 25u, 26u, 27u, 28u, 29u, 30u};
  static const uint32_t shiftTable15u_0[16] = {0u, 14u, 12u, 10u, 8u, 6u, 4u, 2u,
                                               0u, 14u, 12u, 10u, 8u, 6u, 4u, 2u};
  static const uint32_t shiftTable15u_1[16] = {1u, 3u, 5u, 7u, 9u, 11u, 13u, 15u,
                                               1u, 3u, 5u, 7u, 9u, 11u, 13u, 15u};

  static const uint8_t shuffleIdxTable15u_0[64] = {
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 14u, 13u, 12u, 11u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 14u, 13u, 12u, 11u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 14u, 13u, 12u, 11u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 14u, 13u, 12u, 11u};
  static const uint8_t shuffleIdxTable15u_1[64] = {
      3u, 2u, 1u, 0u, 7u, 6u, 5u, 0u, 11u, 10u, 9u, 0u, 15u, 14u, 13u, 0u,
      3u, 2u, 1u, 0u, 7u, 6u, 5u, 0u, 11u, 10u, 9u, 0u, 15u, 14u, 13u, 0u,
      3u, 2u, 1u, 0u, 7u, 6u, 5u, 0u, 11u, 10u, 9u, 0u, 15u, 14u, 13u, 0u,
      3u, 2u, 1u, 0u, 7u, 6u, 5u, 0u, 11u, 10u, 9u, 0u, 15u, 14u, 13u, 0u};
  static const uint32_t shiftTable15u_2[16] = {17u, 11u, 13u, 15u, 17u, 11u, 13u, 15u,
                                               17u, 11u, 13u, 15u, 17u, 11u, 13u, 15u};
  static const uint32_t shiftTable15u_3[16] = {14u, 12u, 10u, 8u, 14u, 12u, 10u, 8u,
                                               14u, 12u, 10u, 8u, 14u, 12u, 10u, 8u};
  static const uint64_t gatherIdxTable15u[8] = {0u, 8u, 15u, 23u, 30u, 38u, 45u, 53u};

  // ------------------------------------ 17u -----------------------------------------
  static const uint32_t permutexIdxTable17u_0[16] = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                     4u, 5u, 5u, 6u, 6u, 7u, 7u, 8u};
  static const uint32_t permutexIdxTable17u_1[16] = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                     4u, 5u, 5u, 6u, 6u, 7u, 7u, 8u};
  static const uint64_t shiftTable17u_0[8] = {0u, 2u, 4u, 6u, 8u, 10u, 12u, 14u};
  static const uint64_t shiftTable17u_1[8] = {15u, 13u, 11u, 9u, 7u, 5u, 3u, 1u};

  static const uint8_t shuffleIdxTable17u_0[64] = {
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 7u, 6u, 5u, 4u, 9u, 8u, 7u, 6u, 3u, 2u, 1u, 0u, 5u, 4u,
      3u, 2u, 7u, 6u, 5u, 4u, 9u, 8u, 7u, 6u, 3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 7u, 6u, 5u, 4u,
      9u, 8u, 7u, 6u, 3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 7u, 6u, 5u, 4u, 9u, 8u, 7u, 6u};
  static const uint32_t shiftTable17u_2[16] = {15u, 14u, 13u, 12u, 11u, 10u, 9u, 8u,
                                               15u, 14u, 13u, 12u, 11u, 10u, 9u, 8u};
  static const uint64_t gatherIdxTable17u[8] = {0u, 8u, 8u, 16u, 17u, 25u, 25u, 33u};

  // ------------------------------------ 18u -----------------------------------------
  static const uint32_t permutexIdxTable18u_0[16] = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                     4u, 5u, 5u, 6u, 6u, 7u, 7u, 8u};
  static const uint32_t permutexIdxTable18u_1[16] = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                     5u, 6u, 6u, 7u, 7u, 8u, 8u, 9u};
  static const uint64_t shiftTable18u_0[8] = {0u, 4u, 8u, 12u, 16u, 20u, 24u, 28u};
  static const uint64_t shiftTable18u_1[8] = {14u, 10u, 6u, 2u, 30u, 26u, 22u, 18u};

  static const uint8_t shuffleIdxTable18u_0[64] = {
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 7u, 6u, 5u, 4u, 9u, 8u, 7u, 6u, 3u, 2u, 1u, 0u, 5u, 4u,
      3u, 2u, 7u, 6u, 5u, 4u, 9u, 8u, 7u, 6u, 3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 7u, 6u, 5u, 4u,
      9u, 8u, 7u, 6u, 3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 7u, 6u, 5u, 4u, 9u, 8u, 7u, 6u};
  static const uint32_t shiftTable18u_2[16] = {14u, 12u, 10u, 8u, 14u, 12u, 10u, 8u,
                                               14u, 12u, 10u, 8u, 14u, 12u, 10u, 8u};
  static const uint64_t gatherIdxTable18u[8] = {0u, 8u, 9u, 17u, 18u, 26u, 27u, 35u};

  // ------------------------------------ 19u -----------------------------------------
  static const uint32_t permutexIdxTable19u_0[16] = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                     4u, 5u, 5u, 6u, 7u, 8u, 8u, 9u};
  static const uint32_t permutexIdxTable19u_1[16] = {0u, 1u, 1u, 2u, 2u, 3u, 4u, 5u,
                                                     5u, 6u, 6u, 7u, 7u, 8u, 8u, 9u};
  static const uint64_t shiftTable19u_0[8] = {0u, 6u, 12u, 18u, 24u, 30u, 4u, 10u};
  static const uint64_t shiftTable19u_1[8] = {13u, 7u, 1u, 27u, 21u, 15u, 9u, 3u};

  static const uint8_t shuffleIdxTable19u_0[64] = {
      3u,  2u, 1u, 0u, 5u, 4u, 3u,  2u, 7u, 6u, 5u, 4u, 10u, 9u, 8u, 7u, 3u,  2u, 1u, 0u, 5u, 4u,
      3u,  2u, 8u, 7u, 6u, 5u, 10u, 9u, 8u, 7u, 3u, 2u, 1u,  0u, 5u, 4u, 3u,  2u, 7u, 6u, 5u, 4u,
      10u, 9u, 8u, 7u, 3u, 2u, 1u,  0u, 5u, 4u, 3u, 2u, 8u,  7u, 6u, 5u, 10u, 9u, 8u, 7u};
  static const uint32_t shiftTable19u_2[16] = {13u, 10u, 7u, 12u, 9u, 6u, 11u, 8u,
                                               13u, 10u, 7u, 12u, 9u, 6u, 11u, 8u};
  static const uint64_t gatherIdxTable19u[8] = {0u, 8u, 9u, 17u, 19u, 27u, 28u, 36u};

  // ------------------------------------ 20u -----------------------------------------
  static const uint8_t shuffleIdxTable20u_0[64] = {
      3u,  2u, 1u, 0u, 5u, 4u, 3u,  2u, 8u, 7u, 6u, 5u, 10u, 9u, 8u, 7u, 3u,  2u, 1u, 0u, 5u, 4u,
      3u,  2u, 8u, 7u, 6u, 5u, 10u, 9u, 8u, 7u, 3u, 2u, 1u,  0u, 5u, 4u, 3u,  2u, 8u, 7u, 6u, 5u,
      10u, 9u, 8u, 7u, 3u, 2u, 1u,  0u, 5u, 4u, 3u, 2u, 8u,  7u, 6u, 5u, 10u, 9u, 8u, 7u};
  static const uint32_t shiftTable20u[16] = {12u, 8u, 12u, 8u, 12u, 8u, 12u, 8u,
                                             12u, 8u, 12u, 8u, 12u, 8u, 12u, 8u};
  static const uint16_t permutexIdxTable20u[32] = {
      0u,  1u,  2u,  3u,  4u,  0x0, 0x0, 0x0, 5u,  6u,  7u,  8u,  9u,  0x0, 0x0, 0x0,
      10u, 11u, 12u, 13u, 14u, 0x0, 0x0, 0x0, 15u, 16u, 17u, 18u, 19u, 0x0, 0x0, 0x0};

  // ------------------------------------ 21u -----------------------------------------
  static const uint32_t permutexIdxTable21u_0[16] = {0u, 1u, 1u, 2u, 2u, 3u, 3u, 4u,
                                                     5u, 6u, 6u, 7u, 7u, 8u, 9u, 10u};
  static const uint32_t permutexIdxTable21u_1[16] = {0u, 1u, 1u, 2u, 3u, 4u, 4u, 5u,
                                                     5u, 6u, 7u, 8u, 8u, 9u, 9u, 10u};
  static const uint64_t shiftTable21u_0[8] = {0u, 10u, 20u, 30u, 8u, 18u, 28u, 6u};
  static const uint64_t shiftTable21u_1[8] = {11u, 1u, 23u, 13u, 3u, 25u, 15u, 5u};

  static const uint8_t shuffleIdxTable21u_0[64] = {
      3u,  2u, 1u, 0u, 5u, 4u, 3u,  2u,  8u, 7u, 6u, 5u, 10u, 9u, 8u, 7u, 3u,  2u,  1u, 0u, 6u, 5u,
      4u,  3u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u, 3u, 2u, 1u,  0u, 5u, 4u, 3u,  2u,  8u, 7u, 6u, 5u,
      10u, 9u, 8u, 7u, 3u, 2u, 1u,  0u,  6u, 5u, 4u, 3u, 8u,  7u, 6u, 5u, 11u, 10u, 9u, 8u};
  static const uint32_t shiftTable21u_2[16] = {11u, 6u, 9u, 4u, 7u, 10u, 5u, 8u,
                                               11u, 6u, 9u, 4u, 7u, 10u, 5u, 8u};
  static const uint64_t gatherIdxTable21u[8] = {0u, 8u, 10u, 18u, 21u, 29u, 31u, 39u};

  // ------------------------------------ 22u -----------------------------------------
  static const uint32_t permutexIdxTable22u_0[16] = {0u, 1u, 1u, 2u, 2u, 3u, 4u, 5u,
                                                     5u, 6u, 6u, 7u, 8u, 9u, 9u, 10u};
  static const uint32_t permutexIdxTable22u_1[16] = {0u, 1u, 2u, 3u, 3u, 4u, 4u,  5u,
                                                     6u, 7u, 7u, 8u, 8u, 9u, 10u, 11u};
  static const uint64_t shiftTable22u_0[8] = {0u, 12u, 24u, 4u, 16u, 28u, 8u, 20u};
  static const uint64_t shiftTable22u_1[8] = {10u, 30u, 18u, 6u, 26u, 14u, 2u, 22u};

  static const uint8_t shuffleIdxTable22u_0[64] = {
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u,
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u,
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u,
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u, 8u};
  static const uint32_t shiftTable22u_2[16] = {10u, 4u, 6u, 8u, 10u, 4u, 6u, 8u,
                                               10u, 4u, 6u, 8u, 10u, 4u, 6u, 8u};
  static const uint64_t gatherIdxTable22u[8] = {0u, 8u, 11u, 19u, 22u, 30u, 33u, 41u};

  // ------------------------------------ 23u -----------------------------------------
  static const uint32_t permutexIdxTable23u_0[16] = {0u, 1u, 1u, 2u, 2u, 3u, 4u,  5u,
                                                     5u, 6u, 7u, 8u, 8u, 9u, 10u, 11u};
  static const uint32_t permutexIdxTable23u_1[16] = {0u, 1u, 2u, 3u, 3u, 4u,  5u,  6u,
                                                     6u, 7u, 7u, 8u, 9u, 10u, 10u, 11u};
  static const uint64_t shiftTable23u_0[8] = {0u, 14u, 28u, 10u, 24u, 6u, 20u, 2u};
  static const uint64_t shiftTable23u_1[8] = {9u, 27u, 13u, 31u, 17u, 3u, 21u, 7u};

  static const uint8_t shuffleIdxTable23u_0[64] = {
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u,  8u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u,
      3u, 2u, 1u, 0u, 5u, 4u, 3u, 2u, 8u, 7u, 6u, 5u, 11u, 10u, 9u,  8u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u};
  static const uint32_t shiftTable23u_2[16] = {9u, 2u, 3u, 4u, 5u, 6u, 7u, 8u,
                                               9u, 2u, 3u, 4u, 5u, 6u, 7u, 8u};
  static const uint64_t gatherIdxTable23u[8] = {0u, 8u, 11u, 19u, 23u, 31u, 34u, 42u};

  // ------------------------------------ 24u -----------------------------------------
  static const uint8_t shuffleIdxTable24u_0[64] = {
      2u, 1u, 0u, 0xFF, 5u, 4u, 3u, 0xFF, 8u, 7u, 6u, 0xFF, 11u, 10u, 9u, 0xFF,
      2u, 1u, 0u, 0xFF, 5u, 4u, 3u, 0xFF, 8u, 7u, 6u, 0xFF, 11u, 10u, 9u, 0xFF,
      2u, 1u, 0u, 0xFF, 5u, 4u, 3u, 0xFF, 8u, 7u, 6u, 0xFF, 11u, 10u, 9u, 0xFF,
      2u, 1u, 0u, 0xFF, 5u, 4u, 3u, 0xFF, 8u, 7u, 6u, 0xFF, 11u, 10u, 9u, 0xFF};
  static const uint32_t permutexIdxTable24u[16] = {0u, 1u, 2u, 0x0, 3u, 4u,  5u,  0x0,
                                                   6u, 7u, 8u, 0x0, 9u, 10u, 11u, 0x0};

  // ------------------------------------ 26u -----------------------------------------
  static const uint32_t permutexIdxTable26u_0[16] = {0u, 1u, 1u, 2u, 3u, 4u,  4u,  5u,
                                                     6u, 7u, 8u, 9u, 9u, 10u, 11u, 12u};
  static const uint32_t permutexIdxTable26u_1[16] = {0u, 1u, 2u, 3u, 4u,  5u,  5u,  6u,
                                                     7u, 8u, 8u, 9u, 10u, 11u, 12u, 13u};
  static const uint64_t shiftTable26u_0[8] = {0u, 20u, 8u, 28u, 16u, 4u, 24u, 12u};
  static const uint64_t shiftTable26u_1[8] = {6u, 18u, 30u, 10u, 22u, 2u, 14u, 26u};

  static const uint8_t shuffleIdxTable26u_0[64] = {
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 9u, 8u, 7u, 6u, 12u, 11u, 10u, 9u};
  static const uint32_t shiftTable26u_2[16] = {6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u,
                                               6u, 4u, 2u, 0u, 6u, 4u, 2u, 0u};
  static const uint64_t gatherIdxTable26u[8] = {0u, 8u, 13u, 21u, 26u, 34u, 39u, 47u};

  // ------------------------------------ 28u -----------------------------------------
  static const uint8_t shuffleIdxTable28u_0[64] = {
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 13u, 12u, 11u, 10u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 13u, 12u, 11u, 10u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 13u, 12u, 11u, 10u,
      3u, 2u, 1u, 0u, 6u, 5u, 4u, 3u, 10u, 9u, 8u, 7u, 13u, 12u, 11u, 10u};
  static const uint32_t shiftTable28u[16] = {4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u,
                                             4u, 0u, 4u, 0u, 4u, 0u, 4u, 0u};
  static const uint16_t permutexIdxTable28u[32] = {
      0u,  1u,  2u,  3u,  4u,  5u,  6u,  0x0, 7u,  8u,  9u,  10u, 11u, 12u, 13u, 0x0,
      14u, 15u, 16u, 17u, 18u, 19u, 20u, 0x0, 21u, 22u, 23u, 24u, 25u, 26u, 27u, 0x0};

  // ------------------------------------ 30u -----------------------------------------
  static const uint32_t permutexIdxTable30u_0[16] = {0u, 1u, 1u, 2u,  3u,  4u,  5u,  6u,
                                                     7u, 8u, 9u, 10u, 11u, 12u, 13u, 14u};
  static const uint32_t permutexIdxTable30u_1[16] = {0u, 1u, 2u,  3u,  4u,  5u,  6u,  7u,
                                                     8u, 9u, 10u, 11u, 12u, 13u, 14u, 15u};
  static const uint64_t shiftTable30u_0[8] = {0u, 28u, 24u, 20u, 16u, 12u, 8u, 4u};
  static const uint64_t shiftTable30u_1[8] = {2u, 6u, 10u, 14u, 18u, 22u, 26u, 30u};

  static const uint8_t shuffleIdxTable30u_0[64] = {
      0u, 0u, 0u, 4u, 3u, 2u, 1u, 0u, 0u, 0u, 0u, 11u, 10u, 9u, 8u, 7u,
      0u, 0u, 0u, 4u, 3u, 2u, 1u, 0u, 0u, 0u, 0u, 11u, 10u, 9u, 8u, 7u,
      0u, 0u, 0u, 4u, 3u, 2u, 1u, 0u, 0u, 0u, 0u, 11u, 10u, 9u, 8u, 7u,
      0u, 0u, 0u, 4u, 3u, 2u, 1u, 0u, 0u, 0u, 0u, 11u, 10u, 9u, 8u, 7u};
  static const uint8_t shuffleIdxTable30u_1[64] = {
      7u, 6u, 5u, 4u, 3u, 0u, 0u, 0u, 15u, 14u, 13u, 12u, 11u, 0u, 0u, 0u,
      7u, 6u, 5u, 4u, 3u, 0u, 0u, 0u, 15u, 14u, 13u, 12u, 11u, 0u, 0u, 0u,
      7u, 6u, 5u, 4u, 3u, 0u, 0u, 0u, 15u, 14u, 13u, 12u, 11u, 0u, 0u, 0u,
      7u, 6u, 5u, 4u, 3u, 0u, 0u, 0u, 15u, 14u, 13u, 12u, 11u, 0u, 0u, 0u};
  static const uint64_t shiftTable30u_2[8] = {34u, 30u, 34u, 30u, 34u, 30u, 34u, 30u};
  static const uint64_t shiftTable30u_3[8] = {28u, 24u, 28u, 24u, 28u, 24u, 28u, 24u};
  static const uint64_t gatherIdxTable30u[8] = {0u, 8u, 15u, 23u, 30u, 38u, 45u, 53u};

  static const uint64_t nibbleReverseTable[8] = {
      0x0E060A020C040800, 0x0F070B030D050901, 0x0E060A020C040800, 0x0F070B030D050901,
      0x0E060A020C040800, 0x0F070B030D050901, 0x0E060A020C040800, 0x0F070B030D050901};

  static const uint64_t reverseMaskTable1u[8] = {
      0x0001020304050607, 0x08090A0B0C0D0E0F, 0x1011121314151617, 0x18191A1B1C1D1E1F,
      0x2021222324252627, 0x28292A2B2C2D2E2F, 0x3031323334353637, 0x38393A3B3C3D3E3F};

  static const uint64_t reverseMaskTable16u[8] = {
      0x0607040502030001, 0x0E0F0C0D0A0B0809, 0x1617141512131011, 0x1E1F1C1D1A1B1819,
      0x2627242522232021, 0x2E2F2C2D2A2B2829, 0x3637343532333031, 0x3E3F3C3D3A3B3839};

  static const uint64_t reverseMaskTable32u[8] = {
      0x0405060700010203, 0x0C0D0E0F08090A0B, 0x1415161710111213, 0x1C1D1E1F18191A1B,
      0x2425262720212223, 0x2C2D2E2F28292A2B, 0x3435363730313233, 0x3C3D3E3F38393A3B};

  inline uint32_t getAlign(uint32_t startBit, uint32_t base, uint32_t bitSize) {
    uint32_t remnant = bitSize - startBit;
    uint32_t retValue = 0xFFFFFFFF;
    for (uint32_t i = 0u; i < bitSize; ++i) {
      uint32_t testValue = (i * base) % bitSize;
      if (testValue == remnant) {
        retValue = i;
        break;
      }
    }
    return retValue;
  }

  inline uint64_t moveByteLen(uint64_t numBits) {
    uint64_t result = numBits / ORC_VECTOR_BYTE_WIDTH;
    if (numBits % ORC_VECTOR_BYTE_WIDTH != 0) ++result;
    return result;
  }
}  // namespace orc

#endif
