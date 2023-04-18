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

#include "BpackingAvx512.hh"
#include "BitUnpackerAvx512.hh"
#include "CpuInfoUtil.hh"
#include "RLEv2.hh"

namespace orc {
  UnpackAvx512::UnpackAvx512(RleDecoderV2* dec) : decoder(dec), unpackDefault(UnpackDefault(dec)) {
    // PASS
  }

  UnpackAvx512::~UnpackAvx512() {
    // PASS
  }

  template <bool hasBitOffset>
  inline void UnpackAvx512::alignHeaderBoundary(const uint32_t bitWidth, const uint32_t bitMaxSize,
                                                uint64_t& startBit, uint64_t& bufMoveByteLen,
                                                uint64_t& bufRestByteLen,
                                                uint64_t& remainingNumElements,
                                                uint64_t& tailBitLen, uint32_t& backupByteLen,
                                                uint64_t& numElements, bool& resetBuf,
                                                const uint8_t*& srcPtr, int64_t*& dstPtr) {
    uint64_t numBits = remainingNumElements * bitWidth;
    if (hasBitOffset && startBit != 0) {
      numBits += startBit - ORC_VECTOR_BYTE_WIDTH;
    }
    bufMoveByteLen += moveByteLen(numBits);

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = remainingNumElements;
      resetBuf = false;
      remainingNumElements = 0;
    } else {
      uint64_t leadingBits = 0;
      if (hasBitOffset && startBit != 0) leadingBits = ORC_VECTOR_BYTE_WIDTH - startBit;
      uint64_t bufRestBitLen = bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + leadingBits;
      numElements = bufRestBitLen / bitWidth;
      remainingNumElements -= numElements;
      tailBitLen = fmod(bufRestBitLen, bitWidth);
      resetBuf = true;
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (hasBitOffset && startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, bitMaxSize);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= moveByteLen(align * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
        bufRestByteLen = decoder->bufLength();
        dstPtr += align;
        numElements -= align;
      }
    }
  }

  template <bool hasBitOffset>
  inline void UnpackAvx512::alignTailerBoundary(const uint32_t bitWidth, const uint32_t specialBit,
                                                uint64_t& startBit, uint64_t& bufMoveByteLen,
                                                uint64_t& bufRestByteLen,
                                                uint64_t& remainingNumElements,
                                                uint32_t& backupByteLen, uint64_t& numElements,
                                                bool& resetBuf, const uint8_t*& srcPtr,
                                                int64_t*& dstPtr) {
    if (numElements > 0) {
      uint64_t numBits = numElements * bitWidth;
      if (hasBitOffset && startBit != 0) {
        numBits += startBit - ORC_VECTOR_BYTE_WIDTH;
      }
      bufMoveByteLen -= moveByteLen(numBits);
      if (hasBitOffset) {
        plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      } else {
        switch (specialBit) {
          case 16:
            unpackDefault.unrolledUnpack16(dstPtr, 0, numElements);
            break;
          case 24:
            unpackDefault.unrolledUnpack24(dstPtr, 0, numElements);
            break;
          case 32:
            unpackDefault.unrolledUnpack32(dstPtr, 0, numElements);
            break;
          default:
            break;
        }
      }
      srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
      dstPtr += numElements;
      bufRestByteLen = decoder->bufLength();
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      decoder->resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    decoder->resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    if (backupByteLen != 0) {
      if (hasBitOffset) {
        plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      } else {
        switch (specialBit) {
          case 16:
            unpackDefault.unrolledUnpack16(dstPtr, 0, 1);
            break;
          case 24:
            unpackDefault.unrolledUnpack24(dstPtr, 0, 1);
            break;
          case 32:
            unpackDefault.unrolledUnpack32(dstPtr, 0, 1);
            break;
          default:
            break;
        }
      }
      dstPtr++;
      backupByteLen = 0;
      remainingNumElements--;
    }

    bufRestByteLen = decoder->bufLength();
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
  }

  void UnpackAvx512::vectorUnpack1(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 1;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_8Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
        uint8_t* simdPtr = reinterpret_cast<uint8_t*>(vectorBuf);
        __m512i reverseMask1u = _mm512_loadu_si512(reverseMaskTable1u);

        while (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
          uint64_t src_64 = *reinterpret_cast<uint64_t*>(const_cast<uint8_t*>(srcPtr));
          // convert mask to 512-bit register. 0 --> 0x00, 1 --> 0xFF
          __m512i srcmm = _mm512_movm_epi8(src_64);
          // make 0x00 --> 0x00, 0xFF --> 0x01
          srcmm = _mm512_abs_epi8(srcmm);
          srcmm = _mm512_shuffle_epi8(srcmm, reverseMask1u);
          _mm512_storeu_si512(simdPtr, srcmm);

          srcPtr += 8 * bitWidth;
          decoder->resetBufferStart(8 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 8 * bitWidth;
          numElements -= VECTOR_UNPACK_8BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_8BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_8BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack2(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 2;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_8Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
        uint8_t* simdPtr = reinterpret_cast<uint8_t*>(vectorBuf);
        __mmask64 readMask = ORC_VECTOR_MAX_16U;         // first 16 bytes (64 elements)
        __m512i parse_mask = _mm512_set1_epi16(0x0303);  // 2 times 1 then (8 - 2) times 0
        while (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
          __m512i srcmm3 = _mm512_maskz_loadu_epi8(readMask, srcPtr);
          __m512i srcmm0, srcmm1, srcmm2, tmpmm;

          srcmm2 = _mm512_srli_epi16(srcmm3, 2);
          srcmm1 = _mm512_srli_epi16(srcmm3, 4);
          srcmm0 = _mm512_srli_epi16(srcmm3, 6);

          // turn 2 bitWidth into 8 by zeroing 3 of each 4 elements.
          // move them into their places
          // srcmm0: a e i m 0 0 0 0 0 0 0 0 0 0 0 0
          // srcmm1: b f j n 0 0 0 0 0 0 0 0 0 0 0 0
          tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);        // ab ef 00 00 00 00 00 00
          srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);       // ij mn 00 00 00 00 00 00
          srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x00);  // ab ef ab ef ij mn ij mn

          // srcmm2: c g k o 0 0 0 0 0 0 0 0 0 0 0 0
          // srcmm3: d h l p 0 0 0 0 0 0 0 0 0 0 0 0
          tmpmm = _mm512_unpacklo_epi8(srcmm2, srcmm3);        // cd gh 00 00 00 00 00 00
          srcmm1 = _mm512_unpackhi_epi8(srcmm2, srcmm3);       // kl op 00 00 00 00 00 00
          srcmm1 = _mm512_shuffle_i64x2(tmpmm, srcmm1, 0x00);  // cd gh cd gh kl op kl op

          tmpmm = _mm512_unpacklo_epi16(srcmm0, srcmm1);        // abcd abcd ijkl ijkl
          srcmm0 = _mm512_unpackhi_epi16(srcmm0, srcmm1);       // efgh efgh mnop mnop
          srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x88);   // abcd ijkl efgh mnop
          srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8);  // abcd efgh ijkl mnop

          srcmm0 = _mm512_and_si512(srcmm0, parse_mask);

          _mm512_storeu_si512(simdPtr, srcmm0);

          srcPtr += 8 * bitWidth;
          decoder->resetBufferStart(8 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 8 * bitWidth;
          numElements -= VECTOR_UNPACK_8BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_8BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_8BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack3(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 3;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_8Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
        uint8_t* simdPtr = reinterpret_cast<uint8_t*>(vectorBuf);
        __mmask64 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_BYTE(bitWidth * 64));
        __m512i parseMask = _mm512_set1_epi8(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable3u);

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable3u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable3u_1);

        __m512i shiftMaskPtr[2];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable3u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable3u_1);

        while (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi8(readMask, srcPtr);
          srcmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi16(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi16(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 8 * bitWidth;
          decoder->resetBufferStart(8 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 8 * bitWidth;
          numElements -= VECTOR_UNPACK_8BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_8BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_8BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack4(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 4;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_8Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
        uint8_t* simdPtr = reinterpret_cast<uint8_t*>(vectorBuf);
        __mmask64 readMask = ORC_VECTOR_MAX_32U;        // first 32 bytes (64 elements)
        __m512i parseMask = _mm512_set1_epi16(0x0F0F);  // 4 times 1 then (8 - 4) times 0
        while (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
          __m512i srcmm0, srcmm1, tmpmm;

          srcmm1 = _mm512_maskz_loadu_epi8(readMask, srcPtr);
          srcmm0 = _mm512_srli_epi16(srcmm1, 4);

          // move elements into their places
          // srcmm0: a c e g 0 0 0 0
          // srcmm1: b d f h 0 0 0 0
          tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);         // ab ef 00 00
          srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);        // cd gh 00 00
          srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x44);   // ab ef cd gh
          srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8);  // ab cd ef gh

          // turn 4 bitWidth into 8 by zeroing 4 of each 8 bits.
          srcmm0 = _mm512_and_si512(srcmm0, parseMask);

          _mm512_storeu_si512(simdPtr, srcmm0);

          srcPtr += 8 * bitWidth;
          decoder->resetBufferStart(8 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 8 * bitWidth;
          numElements -= VECTOR_UNPACK_8BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_8BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_8BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack5(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 5;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_8Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
        uint8_t* simdPtr = reinterpret_cast<uint8_t*>(vectorBuf);
        __mmask64 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_BYTE(bitWidth * 64));
        __m512i parseMask = _mm512_set1_epi8(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable5u);

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable5u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable5u_1);

        __m512i shiftMaskPtr[2];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable5u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable5u_1);

        while (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi8(readMask, srcPtr);
          srcmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi16(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi16(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 8 * bitWidth;
          decoder->resetBufferStart(8 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 8 * bitWidth;
          numElements -= VECTOR_UNPACK_8BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_8BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_8BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack6(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 6;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_8Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
        uint8_t* simdPtr = reinterpret_cast<uint8_t*>(vectorBuf);
        __mmask64 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_BYTE(bitWidth * 64));
        __m512i parseMask = _mm512_set1_epi8(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable6u);

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable6u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable6u_1);

        __m512i shiftMaskPtr[2];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable6u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable6u_1);

        while (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi8(readMask, srcPtr);
          srcmm = _mm512_permutexvar_epi32(permutexIdx, srcmm);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi16(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi16(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 8 * bitWidth;
          decoder->resetBufferStart(8 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 8 * bitWidth;
          numElements -= VECTOR_UNPACK_8BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_8BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_8BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack7(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 7;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_8Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
        uint8_t* simdPtr = reinterpret_cast<uint8_t*>(vectorBuf);
        __mmask64 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_BYTE(bitWidth * 64));
        __m512i parseMask = _mm512_set1_epi8(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable7u);

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable7u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable7u_1);

        __m512i shiftMaskPtr[2];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable7u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable7u_1);

        while (numElements >= VECTOR_UNPACK_8BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi8(readMask, srcPtr);
          srcmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi16(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi16(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi8(zmm[0], 0xAAAAAAAAAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 8 * bitWidth;
          decoder->resetBufferStart(8 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 8 * bitWidth;
          numElements -= VECTOR_UNPACK_8BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_8BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_8BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack9(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 9;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_16Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
        uint16_t* simdPtr = reinterpret_cast<uint16_t*>(vectorBuf);
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
        __m512i parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask16u = _mm512_loadu_si512(reverseMaskTable16u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable9u_0);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable9u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable9u_1);

        __m512i shiftMaskPtr[3];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable9u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable9u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable9u_2);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable9u);

        while (numElements >= 2 * VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi16(zmm[0], shiftMaskPtr[2]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
        if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi16(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi16(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi32(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi16(zmm[0], 0xAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi16(zmm[0], 7);

          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask16u);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack10(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 10;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_16Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
        uint16_t* simdPtr = reinterpret_cast<uint16_t*>(vectorBuf);
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
        __m512i parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable10u_0);
        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable10u);
        __m512i shiftMask = _mm512_loadu_si512(shiftTable10u);

        while (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm;

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          zmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);
          zmm = _mm512_shuffle_epi8(zmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm = _mm512_srlv_epi16(zmm, shiftMask);
          zmm = _mm512_and_si512(zmm, parseMask0);

          _mm512_storeu_si512(simdPtr, zmm);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack11(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 11;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_16Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
        uint16_t* simdPtr = reinterpret_cast<uint16_t*>(vectorBuf);
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
        __m512i parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverse_mask_16u = _mm512_loadu_si512(reverseMaskTable16u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable11u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable11u_1);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable11u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable11u_1);

        __m512i shiftMaskPtr[4];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable11u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable11u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable11u_2);
        shiftMaskPtr[3] = _mm512_loadu_si512(shiftTable11u_3);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable11u);

        while (numElements >= 2 * VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[1] = _mm512_sllv_epi32(zmm[1], shiftMaskPtr[3]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi16(zmm[0], 0xAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
        if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4u);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi16(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi16(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi32(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi16(zmm[0], 0xAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi16(zmm[0], 5);

          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverse_mask_16u);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack12(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 12;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_16Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
        uint16_t* simdPtr = reinterpret_cast<uint16_t*>(vectorBuf);
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
        __m512i parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable12u_0);
        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable12u);
        __m512i shiftMask = _mm512_loadu_si512(shiftTable12u);

        while (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm;

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          zmm = _mm512_permutexvar_epi32(permutexIdx, srcmm);
          zmm = _mm512_shuffle_epi8(zmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm = _mm512_srlv_epi16(zmm, shiftMask);
          zmm = _mm512_and_si512(zmm, parseMask0);

          _mm512_storeu_si512(simdPtr, zmm);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack13(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 13;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_16Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
        uint16_t* simdPtr = reinterpret_cast<uint16_t*>(vectorBuf);
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
        __m512i parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverse_mask_16u = _mm512_loadu_si512(reverseMaskTable16u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable13u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable13u_1);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable13u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable13u_1);

        __m512i shiftMaskPtr[4];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable13u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable13u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable13u_2);
        shiftMaskPtr[3] = _mm512_loadu_si512(shiftTable13u_3);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable13u);

        while (numElements >= 2 * VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[1] = _mm512_sllv_epi32(zmm[1], shiftMaskPtr[3]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi16(zmm[0], 0xAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
        if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi16(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi16(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi32(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi16(zmm[0], 0xAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi16(zmm[0], 3);

          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverse_mask_16u);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack14(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 14;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_16Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
        uint16_t* simdPtr = reinterpret_cast<uint16_t*>(vectorBuf);
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
        __m512i parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable14u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable14u_1);

        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable14u);

        __m512i shiftMaskPtr[2];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable14u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable14u_1);

        while (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);
          srcmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi32(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi16(zmm[0], 0xAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack15(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 15;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_16Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
        uint16_t* simdPtr = reinterpret_cast<uint16_t*>(vectorBuf);
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
        __m512i parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask16u = _mm512_loadu_si512(reverseMaskTable16u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable15u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable15u_1);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable15u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable15u_1);

        __m512i shiftMaskPtr[4];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable15u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable15u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable15u_2);
        shiftMaskPtr[3] = _mm512_loadu_si512(shiftTable15u_3);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable15u);

        while (numElements >= 2 * VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[1] = _mm512_sllv_epi32(zmm[1], shiftMaskPtr[3]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi16(zmm[0], 0xAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
        if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi16(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi16(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi32(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi16(zmm[0], 0xAAAAAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi16(zmm[0], 1);

          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask16u);

          _mm512_storeu_si512(simdPtr, zmm[0]);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack16(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 16;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = len;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    int64_t* dstPtr = data + offset;
    bool resetBuf = false;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;
    uint64_t startBit = 0;

    while (len > 0) {
      alignHeaderBoundary<false>(bitWidth, UNPACK_16Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                 bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                 resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
        uint16_t* simdPtr = reinterpret_cast<uint16_t*>(vectorBuf);
        __m512i reverse_mask_16u = _mm512_loadu_si512(reverseMaskTable16u);
        while (numElements >= VECTOR_UNPACK_16BIT_MAX_NUM) {
          __m512i srcmm = _mm512_loadu_si512(srcPtr);
          srcmm = _mm512_shuffle_epi8(srcmm, reverse_mask_16u);
          _mm512_storeu_si512(simdPtr, srcmm);

          srcPtr += 4 * bitWidth;
          decoder->resetBufferStart(4 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 4 * bitWidth;
          numElements -= VECTOR_UNPACK_16BIT_MAX_NUM;
          std::copy(simdPtr, simdPtr + VECTOR_UNPACK_16BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_16BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<false>(bitWidth, 16, startBit, bufMoveByteLen, bufRestByteLen, len,
                                 backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack17(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 17;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(bitWidth);
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable17u_0);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable17u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable17u_1);

        __m512i shiftMaskPtr[3];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable17u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable17u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable17u_2);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable17u);

        while (numElements >= 2 * VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1u);

          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }

        if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi32(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi32(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi32(zmm[0], 15);
          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask32u);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack18(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 18;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable18u_0);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable18u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable18u_1);

        __m512i shiftMaskPtr[3];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable18u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable18u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable18u_2);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable18u);

        while (numElements >= 2 * VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }

        if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi32(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi32(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi32(zmm[0], 14);
          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask32u);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack19(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 19;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(bitWidth);
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable19u_0);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable19u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable19u_1);

        __m512i shiftMaskPtr[3];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable19u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable19u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable19u_2);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable19u);

        while (numElements >= 2 * VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }

        if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi32(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi32(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi32(zmm[0], 13);
          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask32u);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack20(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 20;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable20u_0);
        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable20u);
        __m512i shiftMask = _mm512_loadu_si512(shiftTable20u);

        while (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm;

          srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

          zmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);
          zmm = _mm512_shuffle_epi8(zmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm = _mm512_srlv_epi32(zmm, shiftMask);
          zmm = _mm512_and_si512(zmm, parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack21(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 21;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(bitWidth);
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable21u_0);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable21u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable21u_1);

        __m512i shiftMaskPtr[3];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable21u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable21u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable21u_2);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable21u);

        while (numElements >= 2 * VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }

        if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi32(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi32(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi32(zmm[0], 11);
          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask32u);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack22(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 22;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable22u_0);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable22u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable22u_1);

        __m512i shiftMaskPtr[3];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable22u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable22u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable22u_2);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable22u);

        while (numElements >= 2 * VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }

        if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi32(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi32(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi32(zmm[0], 10);
          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask32u);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack23(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 23;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;

    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask32 readMask = ORC_VECTOR_BIT_MASK(bitWidth);
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable23u_0);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable23u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable23u_1);

        __m512i shiftMaskPtr[3];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable23u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable23u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable23u_2);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable23u);

        while (numElements >= 2 * VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }

        if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi32(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi32(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi32(zmm[0], 9);
          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask32u);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack24(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 24;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;
    uint64_t startBit = 0;

    while (len > 0) {
      alignHeaderBoundary<false>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                 bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                 resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));

        __m512i shuffleIdx = _mm512_loadu_si512(shuffleIdxTable24u_0);
        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable24u);

        while (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm;

          srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

          zmm = _mm512_permutexvar_epi32(permutexIdx, srcmm);
          zmm = _mm512_shuffle_epi8(zmm, shuffleIdx);

          _mm512_storeu_si512(vectorBuf, zmm);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<false>(bitWidth, 24, startBit, bufMoveByteLen, bufRestByteLen, len,
                                 backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack26(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 26;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable26u_0);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable26u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable26u_1);

        __m512i shiftMaskPtr[3];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable26u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable26u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable26u_2);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable26u);

        while (numElements >= 2 * VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }

        if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi32(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi32(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi32(zmm[0], 6);
          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask32u);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack28(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 28;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));

        __m512i shuffleIdxPtr = _mm512_loadu_si512(shuffleIdxTable28u_0);
        __m512i permutexIdx = _mm512_loadu_si512(permutexIdxTable28u);
        __m512i shiftMask = _mm512_loadu_si512(shiftTable28u);

        while (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm;

          srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

          zmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);
          zmm = _mm512_shuffle_epi8(zmm, shuffleIdxPtr);

          // shifting elements so they start from the start of the word
          zmm = _mm512_srlv_epi32(zmm, shiftMask);
          zmm = _mm512_and_si512(zmm, parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack30(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 30;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t startBit = 0;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;

    while (len > 0) {
      alignHeaderBoundary<true>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
        __m512i parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
        __m512i nibbleReversemm = _mm512_loadu_si512(nibbleReverseTable);
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        __m512i maskmm = _mm512_set1_epi8(0x0F);

        __m512i shuffleIdxPtr[2];
        shuffleIdxPtr[0] = _mm512_loadu_si512(shuffleIdxTable30u_0);
        shuffleIdxPtr[1] = _mm512_loadu_si512(shuffleIdxTable30u_1);

        __m512i permutexIdxPtr[2];
        permutexIdxPtr[0] = _mm512_loadu_si512(permutexIdxTable30u_0);
        permutexIdxPtr[1] = _mm512_loadu_si512(permutexIdxTable30u_1);

        __m512i shiftMaskPtr[4];
        shiftMaskPtr[0] = _mm512_loadu_si512(shiftTable30u_0);
        shiftMaskPtr[1] = _mm512_loadu_si512(shiftTable30u_1);
        shiftMaskPtr[2] = _mm512_loadu_si512(shiftTable30u_2);
        shiftMaskPtr[3] = _mm512_loadu_si512(shiftTable30u_3);

        __m512i gatherIdxmm = _mm512_loadu_si512(gatherIdxTable30u);

        while (numElements >= 2 * VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1u);

          // shuffling so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[0]);
          zmm[1] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr[1]);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[2]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[3]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
        if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm, zmm[2];

          srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

          __m512i lowNibblemm = _mm512_and_si512(srcmm, maskmm);
          __m512i highNibblemm = _mm512_srli_epi16(srcmm, 4);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4u);

          srcmm = _mm512_or_si512(lowNibblemm, highNibblemm);

          // permuting so in zmm[0] will be elements with even indexes and in zmm[1] - with odd ones
          zmm[0] = _mm512_permutexvar_epi32(permutexIdxPtr[0], srcmm);
          zmm[1] = _mm512_permutexvar_epi32(permutexIdxPtr[1], srcmm);

          // shifting elements so they start from the start of the word
          zmm[0] = _mm512_srlv_epi64(zmm[0], shiftMaskPtr[0]);
          zmm[1] = _mm512_sllv_epi64(zmm[1], shiftMaskPtr[1]);

          // gathering even and odd elements together
          zmm[0] = _mm512_mask_mov_epi32(zmm[0], 0xAAAA, zmm[1]);
          zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

          zmm[0] = _mm512_slli_epi32(zmm[0], 2u);
          lowNibblemm = _mm512_and_si512(zmm[0], maskmm);
          highNibblemm = _mm512_srli_epi16(zmm[0], 4u);
          highNibblemm = _mm512_and_si512(highNibblemm, maskmm);

          lowNibblemm = _mm512_shuffle_epi8(nibbleReversemm, lowNibblemm);
          highNibblemm = _mm512_shuffle_epi8(nibbleReversemm, highNibblemm);
          lowNibblemm = _mm512_slli_epi16(lowNibblemm, 4u);

          zmm[0] = _mm512_or_si512(lowNibblemm, highNibblemm);
          zmm[0] = _mm512_shuffle_epi8(zmm[0], reverseMask32u);

          _mm512_storeu_si512(vectorBuf, zmm[0]);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<true>(bitWidth, 0, startBit, bufMoveByteLen, bufRestByteLen, len,
                                backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::vectorUnpack32(int64_t* data, uint64_t offset, uint64_t len) {
    uint32_t bitWidth = 32;
    const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
    uint64_t numElements = 0;
    int64_t* dstPtr = data + offset;
    uint64_t bufMoveByteLen = 0;
    uint64_t bufRestByteLen = decoder->bufLength();
    bool resetBuf = false;
    uint64_t tailBitLen = 0;
    uint32_t backupByteLen = 0;
    uint64_t startBit = 0;

    while (len > 0) {
      alignHeaderBoundary<false>(bitWidth, UNPACK_32Bit_MAX_SIZE, startBit, bufMoveByteLen,
                                 bufRestByteLen, len, tailBitLen, backupByteLen, numElements,
                                 resetBuf, srcPtr, dstPtr);

      if (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
        __m512i reverseMask32u = _mm512_loadu_si512(reverseMaskTable32u);
        while (numElements >= VECTOR_UNPACK_32BIT_MAX_NUM) {
          __m512i srcmm = _mm512_loadu_si512(srcPtr);
          srcmm = _mm512_shuffle_epi8(srcmm, reverseMask32u);
          _mm512_storeu_si512(vectorBuf, srcmm);

          srcPtr += 2 * bitWidth;
          decoder->resetBufferStart(2 * bitWidth, false, 0);
          bufRestByteLen = decoder->bufLength();
          bufMoveByteLen -= 2 * bitWidth;
          numElements -= VECTOR_UNPACK_32BIT_MAX_NUM;
          std::copy(vectorBuf, vectorBuf + VECTOR_UNPACK_32BIT_MAX_NUM, dstPtr);
          dstPtr += VECTOR_UNPACK_32BIT_MAX_NUM;
        }
      }

      alignTailerBoundary<false>(bitWidth, 32, startBit, bufMoveByteLen, bufRestByteLen, len,
                                 backupByteLen, numElements, resetBuf, srcPtr, dstPtr);
    }
  }

  void UnpackAvx512::plainUnpackLongs(int64_t* data, uint64_t offset, uint64_t len, uint64_t fbs,
                                      uint64_t& startBit) {
    for (uint64_t i = offset; i < (offset + len); i++) {
      uint64_t result = 0;
      uint64_t bitsLeftToRead = fbs;
      while (bitsLeftToRead > decoder->getBitsLeft()) {
        result <<= decoder->getBitsLeft();
        result |= decoder->getCurByte() & ((1 << decoder->getBitsLeft()) - 1);
        bitsLeftToRead -= decoder->getBitsLeft();
        decoder->setCurByte(decoder->readByte());
        decoder->setBitsLeft(8);
      }

      // handle the left over bits
      if (bitsLeftToRead > 0) {
        result <<= bitsLeftToRead;
        decoder->setBitsLeft(decoder->getBitsLeft() - static_cast<uint32_t>(bitsLeftToRead));
        result |= (decoder->getCurByte() >> decoder->getBitsLeft()) & ((1 << bitsLeftToRead) - 1);
      }
      data[i] = static_cast<int64_t>(result);
      startBit = decoder->getBitsLeft() == 0 ? 0 : (8 - decoder->getBitsLeft());
    }
  }

  void BitUnpackAVX512::readLongs(RleDecoderV2* decoder, int64_t* data, uint64_t offset,
                                  uint64_t len, uint64_t fbs) {
    UnpackAvx512 unpackAvx512(decoder);
    UnpackDefault unpackDefault(decoder);
    uint64_t startBit = 0;
    static const auto cpu_info = CpuInfo::getInstance();
    if (cpu_info->isSupported(CpuInfo::AVX512)) {
      switch (fbs) {
        case 1:
          unpackAvx512.vectorUnpack1(data, offset, len);
          break;
        case 2:
          unpackAvx512.vectorUnpack2(data, offset, len);
          break;
        case 3:
          unpackAvx512.vectorUnpack3(data, offset, len);
          break;
        case 4:
          unpackAvx512.vectorUnpack4(data, offset, len);
          break;
        case 5:
          unpackAvx512.vectorUnpack5(data, offset, len);
          break;
        case 6:
          unpackAvx512.vectorUnpack6(data, offset, len);
          break;
        case 7:
          unpackAvx512.vectorUnpack7(data, offset, len);
          break;
        case 8:
          unpackDefault.unrolledUnpack8(data, offset, len);
          break;
        case 9:
          unpackAvx512.vectorUnpack9(data, offset, len);
          break;
        case 10:
          unpackAvx512.vectorUnpack10(data, offset, len);
          break;
        case 11:
          unpackAvx512.vectorUnpack11(data, offset, len);
          break;
        case 12:
          unpackAvx512.vectorUnpack12(data, offset, len);
          break;
        case 13:
          unpackAvx512.vectorUnpack13(data, offset, len);
          break;
        case 14:
          unpackAvx512.vectorUnpack14(data, offset, len);
          break;
        case 15:
          unpackAvx512.vectorUnpack15(data, offset, len);
          break;
        case 16:
          unpackAvx512.vectorUnpack16(data, offset, len);
          break;
        case 17:
          unpackAvx512.vectorUnpack17(data, offset, len);
          break;
        case 18:
          unpackAvx512.vectorUnpack18(data, offset, len);
          break;
        case 19:
          unpackAvx512.vectorUnpack19(data, offset, len);
          break;
        case 20:
          unpackAvx512.vectorUnpack20(data, offset, len);
          break;
        case 21:
          unpackAvx512.vectorUnpack21(data, offset, len);
          break;
        case 22:
          unpackAvx512.vectorUnpack22(data, offset, len);
          break;
        case 23:
          unpackAvx512.vectorUnpack23(data, offset, len);
          break;
        case 24:
          unpackAvx512.vectorUnpack24(data, offset, len);
          break;
        case 26:
          unpackAvx512.vectorUnpack26(data, offset, len);
          break;
        case 28:
          unpackAvx512.vectorUnpack28(data, offset, len);
          break;
        case 30:
          unpackAvx512.vectorUnpack30(data, offset, len);
          break;
        case 32:
          unpackAvx512.vectorUnpack32(data, offset, len);
          break;
        case 40:
          unpackDefault.unrolledUnpack40(data, offset, len);
          break;
        case 48:
          unpackDefault.unrolledUnpack48(data, offset, len);
          break;
        case 56:
          unpackDefault.unrolledUnpack56(data, offset, len);
          break;
        case 64:
          unpackDefault.unrolledUnpack64(data, offset, len);
          break;
        default:
          // Fallback to the default implementation for deprecated bit size.
          unpackAvx512.plainUnpackLongs(data, offset, len, fbs, startBit);
          break;
      }
    } else {
      switch (fbs) {
        case 4:
          unpackDefault.unrolledUnpack4(data, offset, len);
          break;
        case 8:
          unpackDefault.unrolledUnpack8(data, offset, len);
          break;
        case 16:
          unpackDefault.unrolledUnpack16(data, offset, len);
          break;
        case 24:
          unpackDefault.unrolledUnpack24(data, offset, len);
          break;
        case 32:
          unpackDefault.unrolledUnpack32(data, offset, len);
          break;
        case 40:
          unpackDefault.unrolledUnpack40(data, offset, len);
          break;
        case 48:
          unpackDefault.unrolledUnpack48(data, offset, len);
          break;
        case 56:
          unpackDefault.unrolledUnpack56(data, offset, len);
          break;
        case 64:
          unpackDefault.unrolledUnpack64(data, offset, len);
          break;
        default:
          // Fallback to the default implementation for deprecated bit size.
          unpackDefault.plainUnpackLongs(data, offset, len, fbs);
          break;
      }
    }
  }
}  // namespace orc
