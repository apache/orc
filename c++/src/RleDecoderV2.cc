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

#include "Adaptor.hh"
#include "Compression.hh"
#include "RLEv2.hh"
#include "RLEV2Util.hh"
#include "VectorDecoder.hh"
#include "DetectPlatform.hh"
#include "Utils.hh"

namespace orc {
void RleDecoderV2::resetBufferStart(uint64_t len, bool resetBuf, uint32_t backupByteLen) {
  uint64_t restLen = bufferEnd - bufferStart;
  int bufferLength = 0;
  const void* bufferPointer = nullptr;

  if (backupByteLen != 0) {
    inputStream->BackUp(backupByteLen);
  }

  if (len >= restLen && resetBuf == true) {
    if (!inputStream->Next(&bufferPointer, &bufferLength)) {
      throw ParseError("bad read in RleDecoderV2::resetBufferStart");
    }
  }

  if (bufferPointer == nullptr) {
    bufferStart += len;
  } else {
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }
}

unsigned char RleDecoderV2::readByte() {
  SCOPED_MINUS_STOPWATCH(metrics, DecodingLatencyUs);
  if (bufferStart == bufferEnd) {
    int bufferLength;
    const void* bufferPointer;
    if (!inputStream->Next(&bufferPointer, &bufferLength)) {
      throw ParseError("bad read in RleDecoderV2::readByte");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }

  unsigned char result = static_cast<unsigned char>(*bufferStart++);
  return result;
}

int64_t RleDecoderV2::readLongBE(uint64_t bsz) {
  int64_t ret = 0, val;
  uint64_t n = bsz;
  while (n > 0) {
    n--;
    val = readByte();
    ret |= (val << (n * 8));
  }
  return ret;
}

inline int64_t RleDecoderV2::readVslong() {
  return unZigZag(readVulong());
}

uint64_t RleDecoderV2::readVulong() {
  uint64_t ret = 0, b;
  uint64_t offset = 0;
  do {
    b = readByte();
    ret |= (0x7f & b) << offset;
    offset += 7;
  } while (b >= 0x80);
  return ret;
}

void RleDecoderV2::readLongs(int64_t *data, uint64_t offset, uint64_t len, uint64_t fbs) {
  uint64_t startBit = 0;
#if ENABLE_AVX512
  if (detect_platform() == arch_t::avx512_arch) {
    switch (fbs) {
      case 1:
        unrolledUnpackVector1(data, offset, len);
        return;
      case 2:
        unrolledUnpackVector2(data, offset, len);
        return;
      case 3:
        unrolledUnpackVector3(data, offset, len);
        return;
      case 4:
        unrolledUnpackVector4(data, offset, len);
        return;
      case 5:
        unrolledUnpackVector5(data, offset, len);
        return;
      case 6:
        unrolledUnpackVector6(data, offset, len);
        return;
      case 7:
        unrolledUnpackVector7(data, offset, len);
        return;
      case 8:
        unrolledUnpack8(data, offset, len);
        return;
      case 9:
        unrolledUnpackVector9(data, offset, len);
        return;
      case 10:
        unrolledUnpackVector10(data, offset, len);
        return;
      case 11:
        unrolledUnpackVector11(data, offset, len);
        return;
      case 12:
        unrolledUnpackVector12(data, offset, len);
        return;
      case 13:
        unrolledUnpackVector13(data, offset, len);
        return;
      case 14:
        unrolledUnpackVector14(data, offset, len);
        return;
      case 15:
        unrolledUnpackVector15(data, offset, len);
        return;
      case 16:
        unrolledUnpackVector16(data, offset, len);
        return;
      case 17:
        unrolledUnpackVector17(data, offset, len);
        return;
      case 18:
        unrolledUnpackVector18(data, offset, len);
        return;
      case 19:
        unrolledUnpackVector19(data, offset, len);
        return;
      case 20:
        unrolledUnpackVector20(data, offset, len);
        return;
      case 21:
        unrolledUnpackVector21(data, offset, len);
        return;
      case 22:
        unrolledUnpackVector22(data, offset, len);
        return;
      case 23:
        unrolledUnpackVector23(data, offset, len);
        return;
      case 24:
        unrolledUnpackVector24(data, offset, len);
        return;
      case 26:
        unrolledUnpackVector26(data, offset, len);
        return;
      case 28:
        unrolledUnpackVector28(data, offset, len);
        return;
      case 30:
        unrolledUnpackVector30(data, offset, len);
        return;
      case 32:
        unrolledUnpackVector32(data, offset, len);
        return;
      case 40:
        unrolledUnpack40(data, offset, len);
        return;
      case 48:
        unrolledUnpack48(data, offset, len);
        return;
      case 56:
        unrolledUnpack56(data, offset, len);
        return;
      case 64:
        unrolledUnpack64(data, offset, len);
        return;
      default:
        // Fallback to the default implementation for deprecated bit size.
        plainUnpackLongs(data, offset, len, fbs, startBit);
        return;
    }
  } else {
    switch (fbs) {
      case 4:
        unrolledUnpack4(data, offset, len);
        return;
      case 8:
        unrolledUnpack8(data, offset, len);
        return;
      case 16:
        unrolledUnpack16(data, offset, len);
        return;
      case 24:
        unrolledUnpack24(data, offset, len);
        return;
      case 32:
        unrolledUnpack32(data, offset, len);
        return;
      case 40:
        unrolledUnpack40(data, offset, len);
        return;
      case 48:
        unrolledUnpack48(data, offset, len);
        return;
      case 56:
        unrolledUnpack56(data, offset, len);
        return;
      case 64:
        unrolledUnpack64(data, offset, len);
        return;
      default:
        // Fallback to the default implementation for deprecated bit size.
        plainUnpackLongs(data, offset, len, fbs, startBit);
        return;
    }
  }
#else
  switch (fbs) {
    case 4:
      unrolledUnpack4(data, offset, len);
      return;
    case 8:
      unrolledUnpack8(data, offset, len);
      return;
    case 16:
      unrolledUnpack16(data, offset, len);
      return;
    case 24:
      unrolledUnpack24(data, offset, len);
      return;
    case 32:
      unrolledUnpack32(data, offset, len);
      return;
    case 40:
      unrolledUnpack40(data, offset, len);
      return;
    case 48:
      unrolledUnpack48(data, offset, len);
      return;
    case 56:
      unrolledUnpack56(data, offset, len);
      return;
    case 64:
      unrolledUnpack64(data, offset, len);
      return;
    default:
      // Fallback to the default implementation for deprecated bit size.
      plainUnpackLongs(data, offset, len, fbs, startBit);
      return;
  }
#endif
}

#if ENABLE_AVX512
void RleDecoderV2::unrolledUnpackVector1(int64_t* data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 1;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint32_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 8);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= moveLen(align * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 64) {
      __m512i reverseMask1u = _mm512_load_si512(reverseMaskTable1u);
      while (numElements >= 64) {
        uint64_t src_64 = *(uint64_t *)srcPtr;
        // convert mask to 512-bit register. 0 --> 0x00, 1 --> 0xFF
        __m512i srcmm = _mm512_movm_epi8(src_64);
        // make 0x00 --> 0x00, 0xFF --> 0x01
        srcmm = _mm512_abs_epi8(srcmm);
        srcmm = _mm512_shuffle_epi8(srcmm, reverseMask1u);
        _mm512_storeu_si512(vectorBuf8, srcmm);

        srcPtr += 8 * bitWidth;
        resetBufferStart(8 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 8 * bitWidth;
        numElements -= 64;
        std::copy(vectorBuf8, vectorBuf8 + 64, dstPtr);
        dstPtr += 64;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector2(int64_t* data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 2;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint32_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 8);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= moveLen(align * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 64) {
      __mmask64 readMask = ORC_VECTOR_MAX_16U; // first 16 bytes (64 elements)
      __m512i   parse_mask = _mm512_set1_epi16(0x0303); // 2 times 1 then (8 - 2) times 0
      while (numElements >= 64) {
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
        srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);        // ij mn 00 00 00 00 00 00
        srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x00); // ab ef ab ef ij mn ij mn

        // srcmm2: c g k o 0 0 0 0 0 0 0 0 0 0 0 0
        // srcmm3: d h l p 0 0 0 0 0 0 0 0 0 0 0 0
        tmpmm = _mm512_unpacklo_epi8(srcmm2, srcmm3);        // cd gh 00 00 00 00 00 00
        srcmm1 = _mm512_unpackhi_epi8(srcmm2, srcmm3);        // kl op 00 00 00 00 00 00
        srcmm1 = _mm512_shuffle_i64x2(tmpmm, srcmm1, 0x00); // cd gh cd gh kl op kl op

        tmpmm = _mm512_unpacklo_epi16(srcmm0, srcmm1);       // abcd abcd ijkl ijkl
        srcmm0 = _mm512_unpackhi_epi16(srcmm0, srcmm1);       // efgh efgh mnop mnop
        srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x88); // abcd ijkl efgh mnop
        srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8);  // abcd efgh ijkl mnop

        srcmm0 = _mm512_and_si512(srcmm0, parse_mask);

        _mm512_storeu_si512(vectorBuf8, srcmm0);

        srcPtr += 8 * bitWidth;
        resetBufferStart(8 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 8 * bitWidth;
        numElements -= 64;
        std::copy(vectorBuf8, vectorBuf8 + 64, dstPtr);
        dstPtr += 64;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector3(int64_t* data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 3;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint32_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 8);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= moveLen(align * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 64) {
      __mmask64 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_BYTE(bitWidth * 64));
      __m512i   parseMask = _mm512_set1_epi8(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable3u);

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable3u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable3u_1);

      __m512i   shiftMaskPtr[2];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable3u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable3u_1);

      while (numElements >= 64) {
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

        _mm512_storeu_si512(vectorBuf8, zmm[0]);

        srcPtr += 8 * bitWidth;
        resetBufferStart(8 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 8 * bitWidth;
        numElements -= 64;
        std::copy(vectorBuf8, vectorBuf8 + 64, dstPtr);
        dstPtr += 64;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector4(int64_t* data, uint64_t offset, uint64_t len){
  uint32_t bitWidth = 4;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 8);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= moveLen(align * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 64) {
      __mmask64 readMask = ORC_VECTOR_MAX_32U; // first 32 bytes (64 elements)
      __m512i   parseMask = _mm512_set1_epi16(0x0F0F); // 4 times 1 then (8 - 4) times 0
      while (numElements >= 64) {
        __m512i srcmm0, srcmm1, tmpmm;

        srcmm1 = _mm512_maskz_loadu_epi8(readMask, srcPtr);
        srcmm0 = _mm512_srli_epi16(srcmm1, 4);

        // move elements into their places
        // srcmm0: a c e g 0 0 0 0
        // srcmm1: b d f h 0 0 0 0
        tmpmm = _mm512_unpacklo_epi8(srcmm0, srcmm1);        // ab ef 00 00
        srcmm0 = _mm512_unpackhi_epi8(srcmm0, srcmm1);        // cd gh 00 00
        srcmm0 = _mm512_shuffle_i64x2(tmpmm, srcmm0, 0x44); // ab ef cd gh
        srcmm0 = _mm512_shuffle_i64x2(srcmm0, srcmm0, 0xD8);  // ab cd ef gh

        // turn 4 bitWidth into 8 by zeroing 4 of each 8 bits.
        srcmm0 = _mm512_and_si512(srcmm0, parseMask);

        _mm512_storeu_si512(vectorBuf8, srcmm0);

        srcPtr += 8 * bitWidth;
        resetBufferStart(8 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 8 * bitWidth;
        numElements -= 64;
        std::copy(vectorBuf8, vectorBuf8 + 64, dstPtr);
        dstPtr += 64;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector5(int64_t* data, uint64_t offset, uint64_t len){
  uint32_t bitWidth = 5;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 8);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= moveLen(align * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 64) {
      __mmask64 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_BYTE(bitWidth * 64));
      __m512i   parseMask = _mm512_set1_epi8(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable5u);

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable5u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable5u_1);

      __m512i   shiftMaskPtr[2];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable5u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable5u_1);

      while (numElements >= 64) {
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

        _mm512_storeu_si512(vectorBuf8, zmm[0]);

        srcPtr += 8 * bitWidth;
        resetBufferStart(8 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 8 * bitWidth;
        numElements -= 64;
        std::copy(vectorBuf8, vectorBuf8 + 64, dstPtr);
        dstPtr += 64;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector6(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 6;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 8);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= moveLen(align * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 64) {
      __mmask64 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_BYTE(bitWidth * 64));
      __m512i   parseMask = _mm512_set1_epi8(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable6u);

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable6u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable6u_1);

      __m512i   shiftMaskPtr[2];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable6u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable6u_1);

      while (numElements >= 64) {
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

        _mm512_storeu_si512(vectorBuf8, zmm[0]);

        srcPtr += 8 * bitWidth;
        resetBufferStart(8 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 8 * bitWidth;
        numElements -= 64;
        std::copy(vectorBuf8, vectorBuf8 + 64, dstPtr);
        dstPtr += 64;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector7(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 7;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH , ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 8);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= moveLen(align * bitWidth + startBit - ORC_VECTOR_BYTE_WIDTH, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 64) {
      __mmask64 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_BYTE(bitWidth * 64));
      __m512i   parseMask = _mm512_set1_epi8(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable7u);

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable7u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable7u_1);

      __m512i   shiftMaskPtr[2];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable7u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable7u_1);

      while (numElements >= 64) {
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

        _mm512_storeu_si512(vectorBuf8, zmm[0]);

        srcPtr += 8 * bitWidth;
        resetBufferStart(8 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 8 * bitWidth;
        numElements -= 64;
        std::copy(vectorBuf8, vectorBuf8 + 64, dstPtr);
        dstPtr += 64;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector9(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 9;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 16);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 32) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
      __m512i   parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask16u = _mm512_load_si512(reverseMaskTable16u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable9u_0);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable9u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable9u_1);

      __m512i   shiftMaskPtr[3];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable9u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable9u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable9u_2);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable9u);

      while (numElements >= 64) {
        __m512i srcmm, zmm[2];

        srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

        zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm[0] = _mm512_srlv_epi16(zmm[0], shiftMaskPtr[2]);
        zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
      if (numElements >= 32) {
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

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector10(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 10;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 16);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 32) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
      __m512i   parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable10u_0);
      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable10u);
      __m512i   shiftMask = _mm512_load_si512(shiftTable10u);

      while (numElements >= 32) {
        __m512i srcmm, zmm;

        srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

        zmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);
        zmm = _mm512_shuffle_epi8(zmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm = _mm512_srlv_epi16(zmm, shiftMask);
        zmm = _mm512_and_si512(zmm, parseMask0);

        _mm512_storeu_si512(vectorBuf16, zmm);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector11(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 11;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 16);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 32) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
      __m512i   parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverse_mask_16u = _mm512_load_si512(reverseMaskTable16u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable11u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable11u_1);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable11u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable11u_1);

      __m512i   shiftMaskPtr[4];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable11u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable11u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable11u_2);
      shiftMaskPtr[3] = _mm512_load_si512(shiftTable11u_3);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable11u);

      while (numElements >= 64) {
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

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
      if (numElements >= 32) {
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

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector12(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 12;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 16);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 32) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
      __m512i   parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable12u_0);
      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable12u);
      __m512i   shiftMask = _mm512_load_si512(shiftTable12u);

      while (numElements >= 32) {
        __m512i srcmm, zmm;

        srcmm = _mm512_maskz_loadu_epi16(readMask, srcPtr);

        zmm = _mm512_permutexvar_epi32(permutexIdx, srcmm);
        zmm = _mm512_shuffle_epi8(zmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm = _mm512_srlv_epi16(zmm, shiftMask);
        zmm = _mm512_and_si512(zmm, parseMask0);

        _mm512_storeu_si512(vectorBuf16, zmm);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector13(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 13;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 16);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 32) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
      __m512i   parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverse_mask_16u = _mm512_load_si512(reverseMaskTable16u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable13u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable13u_1);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable13u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable13u_1);

      __m512i   shiftMaskPtr[4];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable13u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable13u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable13u_2);
      shiftMaskPtr[3] = _mm512_load_si512(shiftTable13u_3);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable13u);

      while (numElements >= 64) {
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

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
      if (numElements >= 32) {
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

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector14(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 14;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 16);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 32) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
      __m512i   parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable14u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable14u_1);

      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable14u);

      __m512i   shiftMaskPtr[2];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable14u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable14u_1);

      while (numElements >= 32) {
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

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector15(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 15;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 16);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 32) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_WORD(bitWidth * 32));
      __m512i   parseMask0 = _mm512_set1_epi16(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask16u = _mm512_load_si512(reverseMaskTable16u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable15u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable15u_1);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable15u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable15u_1);

      __m512i   shiftMaskPtr[4];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable15u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable15u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable15u_2);
      shiftMaskPtr[3] = _mm512_load_si512(shiftTable15u_3);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable15u);

      while (numElements >= 64) {
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

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
      if (numElements >= 32) {
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

        _mm512_storeu_si512(vectorBuf16, zmm[0]);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector16(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 16;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = len;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  int64_t* dstPtr = data + offset;
  bool resetBuf = false;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
    } else {
      numElements = bufRestByteLen * ORC_VECTOR_BYTE_WIDTH / bitWidth;
      len -= numElements;
      tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
      resetBuf = true;
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (numElements >= 32) {
      __m512i reverse_mask_16u = _mm512_load_si512(reverseMaskTable16u);
      while (numElements >= 32) {
        __m512i srcmm = _mm512_loadu_si512(srcPtr);
        srcmm = _mm512_shuffle_epi8(srcmm, reverse_mask_16u);
        _mm512_storeu_si512(vectorBuf16, srcmm);

        srcPtr += 4 * bitWidth;
        resetBufferStart(4 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 4 * bitWidth;
        numElements -= 32;
        std::copy(vectorBuf16, vectorBuf16 + 32, dstPtr);
        dstPtr += 32;
      }
    }

    if (numElements > 0) {
      bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      unrolledUnpack16(dstPtr, 0, numElements);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    } 
    
    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);;
      unrolledUnpack16(dstPtr, 0, 1);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector17(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 17;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(bitWidth);
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable17u_0);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable17u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable17u_1);

      __m512i   shiftMaskPtr[3];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable17u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable17u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable17u_2);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable17u);

      while (numElements >= 32) {
        __m512i srcmm, zmm[2];

        srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1u);

        zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
        zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }

      if (numElements >= 16) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector18(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 18;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16) {
      __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable18u_0);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable18u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable18u_1);

      __m512i   shiftMaskPtr[3];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable18u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable18u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable18u_2);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable18u);

      while (numElements >= 32) {
        __m512i srcmm, zmm[2];

        srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

        zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
        zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }

      if (numElements >= 16) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector19(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 19;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(bitWidth);
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable19u_0);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable19u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable19u_1);

      __m512i   shiftMaskPtr[3];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable19u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable19u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable19u_2);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable19u);

      while (numElements >= 32) {
        __m512i srcmm, zmm[2];

        srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

        zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
        zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }

      if (numElements >= 16) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector20(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 20;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0u) {
      uint32_t align = getAlign(startBit, bitWidth, 32u);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16u) {
      __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable20u_0);
      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable20u);
      __m512i   shiftMask = _mm512_load_si512(shiftTable20u);

      while (numElements >= 16u) {
        __m512i srcmm, zmm;

        srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

        zmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);
        zmm = _mm512_shuffle_epi8(zmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm = _mm512_srlv_epi32(zmm, shiftMask);
        zmm = _mm512_and_si512(zmm, parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector21(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 21;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0u) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(bitWidth);
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable21u_0);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable21u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable21u_1);

      __m512i   shiftMaskPtr[3];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable21u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable21u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable21u_2);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable21u);

      while (numElements >= 32) {
        __m512i srcmm, zmm[2];

        srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

        zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
        zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }

      if (numElements >= 16) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector22(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 22;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16) {
      __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable22u_0);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable22u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable22u_1);

      __m512i   shiftMaskPtr[3];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable22u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable22u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable22u_2);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable22u);

      while (numElements >= 32) {
        __m512i srcmm, zmm[2];

        srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

        zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
        zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }

      if (numElements >= 16) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector23(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 23;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;

  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
	tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
	tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
	bufMoveByteLen -= moveLen(align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }
    
    if (numElements >= 16) {
      __mmask32 readMask = ORC_VECTOR_BIT_MASK(bitWidth);
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable23u_0);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable23u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable23u_1);

      __m512i   shiftMaskPtr[3];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable23u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable23u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable23u_2);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable23u);

      while (numElements >= 32) {
        __m512i srcmm, zmm[2];

        srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

        zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
        zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }

      if (numElements >= 16) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    } 
    
    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector24(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 24;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
    } else {
      numElements = bufRestByteLen * ORC_VECTOR_BYTE_WIDTH / bitWidth;
      len -= numElements;
      tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
      resetBuf = true;
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (numElements >= 16) {
      __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
      
      __m512i shuffleIdx = _mm512_load_si512(shuffleIdxTable24u_0);
      __m512i permutexIdx = _mm512_load_si512(permutexIdxTable24u);

      while (numElements >= 16) {
        __m512i srcmm, zmm;

        srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

        zmm = _mm512_permutexvar_epi32(permutexIdx, srcmm);
        zmm = _mm512_shuffle_epi8(zmm, shuffleIdx);

        _mm512_storeu_si512(vectorBuf32, zmm);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      unrolledUnpack24(dstPtr, 0, numElements);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    } 
    
    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);;
      unrolledUnpack24(dstPtr, 0, 1);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector26(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 26;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= (align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit) / ORC_VECTOR_BYTE_WIDTH;
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16) {
      __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable26u_0);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable26u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable26u_1);

      __m512i   shiftMaskPtr[3];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable26u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable26u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable26u_2);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable26u);

      while (numElements >= 32) {
        __m512i srcmm, zmm[2];

        srcmm = _mm512_i64gather_epi64(gatherIdxmm, srcPtr, 1);

        zmm[0] = _mm512_shuffle_epi8(srcmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm[0] = _mm512_srlv_epi32(zmm[0], shiftMaskPtr[2]);
        zmm[0] = _mm512_and_si512(zmm[0], parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }

      if (numElements >= 16) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector28(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 28;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= (align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit) / ORC_VECTOR_BYTE_WIDTH;
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16) {
      __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));

      __m512i   shuffleIdxPtr = _mm512_load_si512(shuffleIdxTable28u_0);
      __m512i   permutexIdx = _mm512_load_si512(permutexIdxTable28u);
      __m512i   shiftMask = _mm512_load_si512(shiftTable28u);

      while (numElements >= 16) {
        __m512i srcmm, zmm;

        srcmm = _mm512_maskz_loadu_epi32(readMask, srcPtr);

        zmm = _mm512_permutexvar_epi16(permutexIdx, srcmm);
        zmm = _mm512_shuffle_epi8(zmm, shuffleIdxPtr);

        // shifting elements so they start from the start of the word
        zmm = _mm512_srlv_epi32(zmm, shiftMask);
        zmm = _mm512_and_si512(zmm, parseMask0);

        _mm512_storeu_si512(vectorBuf32, zmm);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector30(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 30;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t startBit = 0;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    if (startBit != 0) {
      bufMoveByteLen += moveLen(len * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
    } else {
      bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
      len -= numElements;
    } else {
      if (startBit != 0) {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH + ORC_VECTOR_BYTE_WIDTH - startBit, bitWidth);
        resetBuf = true;
      } else {
        numElements = (bufRestByteLen * ORC_VECTOR_BYTE_WIDTH) / bitWidth;
        len -= numElements;
        tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
        resetBuf = true;
      }
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (startBit > 0) {
      uint32_t align = getAlign(startBit, bitWidth, 32);
      if (align > numElements) {
        align = numElements;
      }
      if (align != 0) {
        bufMoveByteLen -= (align * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit) / ORC_VECTOR_BYTE_WIDTH;
        plainUnpackLongs(dstPtr, 0, align, bitWidth, startBit);
        srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
        bufRestByteLen = bufferEnd - bufferStart;
        dstPtr += align;
        numElements -= align;
      }
    }

    if (numElements >= 16) {
      __mmask16 readMask = ORC_VECTOR_BIT_MASK(ORC_VECTOR_BITS_2_DWORD(bitWidth * 16));
      __m512i   parseMask0 = _mm512_set1_epi32(ORC_VECTOR_BIT_MASK(bitWidth));
      __m512i   nibbleReversemm = _mm512_load_si512(nibbleReverseTable);
      __m512i   reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      __m512i   maskmm = _mm512_set1_epi8(0x0F);

      __m512i   shuffleIdxPtr[2];
      shuffleIdxPtr[0] = _mm512_load_si512(shuffleIdxTable30u_0);
      shuffleIdxPtr[1] = _mm512_load_si512(shuffleIdxTable30u_1);

      __m512i   permutexIdxPtr[2];
      permutexIdxPtr[0] = _mm512_load_si512(permutexIdxTable30u_0);
      permutexIdxPtr[1] = _mm512_load_si512(permutexIdxTable30u_1);

      __m512i   shiftMaskPtr[4];
      shiftMaskPtr[0] = _mm512_load_si512(shiftTable30u_0);
      shiftMaskPtr[1] = _mm512_load_si512(shiftTable30u_1);
      shiftMaskPtr[2] = _mm512_load_si512(shiftTable30u_2);
      shiftMaskPtr[3] = _mm512_load_si512(shiftTable30u_3);

      __m512i   gatherIdxmm = _mm512_load_si512(gatherIdxTable30u);

      while (numElements >= 32) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
      if (numElements >= 16) {
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

        _mm512_storeu_si512(vectorBuf32, zmm[0]);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }

    if (numElements > 0) {
      if (startBit != 0) {
        bufMoveByteLen -= moveLen(numElements * bitWidth - ORC_VECTOR_BYTE_WIDTH + startBit, ORC_VECTOR_BYTE_WIDTH);
      } else {
        bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      }
      plainUnpackLongs(dstPtr, 0, numElements, bitWidth, startBit);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    }

    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
      plainUnpackLongs(dstPtr, 0, 1, bitWidth, startBit);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}

void RleDecoderV2::unrolledUnpackVector32(int64_t *data, uint64_t offset, uint64_t len) {
  uint32_t bitWidth = 32;
  const uint8_t *srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  uint64_t numElements = 0;
  int64_t* dstPtr = data + offset;
  uint64_t bufMoveByteLen = 0;
  uint64_t bufRestByteLen = bufferEnd - bufferStart;
  bool resetBuf = false;
  uint64_t tailBitLen = 0;
  uint32_t backupByteLen = 0;

  while (len > 0) {
    bufMoveByteLen += moveLen(len * bitWidth, ORC_VECTOR_BYTE_WIDTH);

    if (bufMoveByteLen <= bufRestByteLen) {
      numElements = len;
      resetBuf = false;
    } else {
      numElements = bufRestByteLen * ORC_VECTOR_BYTE_WIDTH / bitWidth;
      len -= numElements;
      tailBitLen = fmod(bufRestByteLen * ORC_VECTOR_BYTE_WIDTH, bitWidth);
      resetBuf = true;
    }

    if (tailBitLen != 0) {
      backupByteLen = tailBitLen / ORC_VECTOR_BYTE_WIDTH;
      tailBitLen = 0;
    }

    if (numElements >= 16) {
      __m512i reverseMask32u = _mm512_load_si512(reverseMaskTable32u);
      while (numElements >= 16) {
        __m512i srcmm = _mm512_loadu_si512(srcPtr);
        srcmm = _mm512_shuffle_epi8(srcmm, reverseMask32u);
        _mm512_storeu_si512(vectorBuf32, srcmm);

        srcPtr += 2 * bitWidth;
        resetBufferStart(2 * bitWidth, false, 0);
        bufRestByteLen = bufferEnd - bufferStart;
        bufMoveByteLen -= 2 * bitWidth;
        numElements -= 16;
        std::copy(vectorBuf32, vectorBuf32 + 16, dstPtr);
        dstPtr += 16;
      }
    }
    
    if (numElements > 0) {
      bufMoveByteLen -= moveLen(numElements * bitWidth, ORC_VECTOR_BYTE_WIDTH);
      unrolledUnpack32(dstPtr, 0, numElements);
      srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
      dstPtr += numElements;
      bufRestByteLen = bufferEnd - bufferStart;
    }

    if (bufMoveByteLen <= bufRestByteLen) {
      resetBufferStart(bufMoveByteLen, resetBuf, backupByteLen);
      return;
    } 
    
    if (backupByteLen != 0) {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);;
      unrolledUnpack32(dstPtr, 0, 1);
      dstPtr++;
      backupByteLen = 0;
      len--;
    } else {
      resetBufferStart(bufRestByteLen, resetBuf, backupByteLen);
    }

    bufRestByteLen = bufferEnd - bufferStart;
    bufMoveByteLen = 0;
    srcPtr = reinterpret_cast<const uint8_t *>(bufferStart);
  }
}
#endif

void RleDecoderV2::unrolledUnpack4(int64_t* data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Make sure bitsLeft is 0 before the loop. bitsLeft can only be 0, 4, or 8.
    while (bitsLeft > 0 && curIdx < offset + len) {
      bitsLeft -= 4;
      data[curIdx++] = (curByte >> bitsLeft) & 15;
    }
    if (curIdx == offset + len) return;

    // Exhaust the buffer
    uint64_t numGroups = (offset + len - curIdx) / 2;
    numGroups = std::min(numGroups, static_cast<uint64_t>(bufferEnd - bufferStart));
    // Avoid updating 'bufferStart' inside the loop.
    const auto *buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    uint32_t localByte;
    for (uint64_t i = 0; i < numGroups; ++i) {
      localByte = *buffer++;
      data[curIdx] = (localByte >> 4) & 15;
      data[curIdx + 1] = localByte & 15;
      curIdx += 2;
    }
    bufferStart = reinterpret_cast<const char*>(buffer);
    if (curIdx == offset + len) return;

    // readByte() will update 'bufferStart' and 'bufferEnd'
    curByte = readByte();
    bitsLeft = 8;
  }
}

void RleDecoderV2::unrolledUnpack8(int64_t* data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Exhaust the buffer
    int64_t bufferNum = bufferEnd - bufferStart;
    bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
    // Avoid updating 'bufferStart' inside the loop.
    const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    for (int i = 0; i < bufferNum; ++i) {
      data[curIdx++] = *buffer++;
    }
    bufferStart = reinterpret_cast<const char*>(buffer);
    if (curIdx == offset + len) return;

    // readByte() will update 'bufferStart' and 'bufferEnd'.
    data[curIdx++] = readByte();
  }
}

void RleDecoderV2::unrolledUnpack16(int64_t* data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Exhaust the buffer
    int64_t bufferNum = (bufferEnd - bufferStart) / 2;
    bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
    uint16_t b0, b1;
    // Avoid updating 'bufferStart' inside the loop.
    const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    for (int i = 0; i < bufferNum; ++i) {
      b0 = static_cast<uint16_t>(*buffer);
      b1 = static_cast<uint16_t>(*(buffer + 1));
      buffer += 2;
      data[curIdx++] = (b0 << 8) | b1;
    }
    bufferStart = reinterpret_cast<const char*>(buffer);
    if (curIdx == offset + len) return;

    // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
    b0 = readByte();
    b1 = readByte();
    data[curIdx++] = (b0 << 8) | b1;
  }
}

void RleDecoderV2::unrolledUnpack24(int64_t* data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Exhaust the buffer
    int64_t bufferNum = (bufferEnd - bufferStart) / 3;
    bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
    uint32_t b0, b1, b2;
    // Avoid updating 'bufferStart' inside the loop.
    const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    for (int i = 0; i < bufferNum; ++i) {
      b0 = static_cast<uint32_t>(*buffer);
      b1 = static_cast<uint32_t>(*(buffer + 1));
      b2 = static_cast<uint32_t>(*(buffer + 2));
      buffer += 3;
      data[curIdx++] = static_cast<int64_t>((b0 << 16) | (b1 << 8) | b2);
    }
    bufferStart += bufferNum * 3;
    if (curIdx == offset + len) return;

    // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
    b0 = readByte();
    b1 = readByte();
    b2 = readByte();
    data[curIdx++] = static_cast<int64_t>((b0 << 16) | (b1 << 8) | b2);
  }
}

void RleDecoderV2::unrolledUnpack32(int64_t* data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Exhaust the buffer
    int64_t bufferNum = (bufferEnd - bufferStart) / 4;
    bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
    uint32_t b0, b1, b2, b3;
    // Avoid updating 'bufferStart' inside the loop.
    const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    for (int i = 0; i < bufferNum; ++i) {
      b0 = static_cast<uint32_t>(*buffer);
      b1 = static_cast<uint32_t>(*(buffer + 1));
      b2 = static_cast<uint32_t>(*(buffer + 2));
      b3 = static_cast<uint32_t>(*(buffer + 3));
      buffer += 4;
      data[curIdx++] = static_cast<int64_t>((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
    }
    bufferStart = reinterpret_cast<const char*>(buffer);
    if (curIdx == offset + len) return;

    // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
    b0 = readByte();
    b1 = readByte();
    b2 = readByte();
    b3 = readByte();
    data[curIdx++] = static_cast<int64_t>((b0 << 24) | (b1 << 16) | (b2 << 8) | b3);
  }
}

void RleDecoderV2::unrolledUnpack40(int64_t* data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Exhaust the buffer
    int64_t bufferNum = (bufferEnd - bufferStart) / 5;
    bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
    uint64_t b0, b1, b2, b3, b4;
    // Avoid updating 'bufferStart' inside the loop.
    const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    for (int i = 0; i < bufferNum; ++i) {
      b0 = static_cast<uint32_t>(*buffer);
      b1 = static_cast<uint32_t>(*(buffer + 1));
      b2 = static_cast<uint32_t>(*(buffer + 2));
      b3 = static_cast<uint32_t>(*(buffer + 3));
      b4 = static_cast<uint32_t>(*(buffer + 4));
      buffer += 5;
      data[curIdx++] = static_cast<int64_t>((b0 << 32) | (b1 << 24) | (b2 << 16) | (b3 << 8) | b4);
    }
    bufferStart = reinterpret_cast<const char*>(buffer);
    if (curIdx == offset + len) return;

    // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
    b0 = readByte();
    b1 = readByte();
    b2 = readByte();
    b3 = readByte();
    b4 = readByte();
    data[curIdx++] = static_cast<int64_t>((b0 << 32) | (b1 << 24) | (b2 << 16) | (b3 << 8) | b4);
  }
}

void RleDecoderV2::unrolledUnpack48(int64_t *data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Exhaust the buffer
    int64_t bufferNum = (bufferEnd - bufferStart) / 6;
    bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
    uint64_t b0, b1, b2, b3, b4, b5;
    // Avoid updating 'bufferStart' inside the loop.
    const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    for (int i = 0; i < bufferNum; ++i) {
      b0 = static_cast<uint32_t>(*buffer);
      b1 = static_cast<uint32_t>(*(buffer + 1));
      b2 = static_cast<uint32_t>(*(buffer + 2));
      b3 = static_cast<uint32_t>(*(buffer + 3));
      b4 = static_cast<uint32_t>(*(buffer + 4));
      b5 = static_cast<uint32_t>(*(buffer + 5));
      buffer += 6;
      data[curIdx++] = static_cast<int64_t>((b0 << 40) | (b1 << 32) | (b2 << 24) | (b3 << 16) | (b4 << 8) | b5);
    }
    bufferStart = reinterpret_cast<const char*>(buffer);
    if (curIdx == offset + len) return;

    // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
    b0 = readByte();
    b1 = readByte();
    b2 = readByte();
    b3 = readByte();
    b4 = readByte();
    b5 = readByte();
    data[curIdx++] = static_cast<int64_t>((b0 << 40) | (b1 << 32) | (b2 << 24) | (b3 << 16) | (b4 << 8) | b5);
  }
}

void RleDecoderV2::unrolledUnpack56(int64_t *data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Exhaust the buffer
    int64_t bufferNum = (bufferEnd - bufferStart) / 7;
    bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
    uint64_t b0, b1, b2, b3, b4, b5, b6;
    // Avoid updating 'bufferStart' inside the loop.
    const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    for (int i = 0; i < bufferNum; ++i) {
      b0 = static_cast<uint32_t>(*buffer);
      b1 = static_cast<uint32_t>(*(buffer + 1));
      b2 = static_cast<uint32_t>(*(buffer + 2));
      b3 = static_cast<uint32_t>(*(buffer + 3));
      b4 = static_cast<uint32_t>(*(buffer + 4));
      b5 = static_cast<uint32_t>(*(buffer + 5));
      b6 = static_cast<uint32_t>(*(buffer + 6));
      buffer += 7;
      data[curIdx++] = static_cast<int64_t>((b0 << 48) | (b1 << 40) | (b2 << 32) | (b3 << 24) | (b4 << 16) | (b5 << 8) | b6);
    }
    bufferStart = reinterpret_cast<const char*>(buffer);
    if (curIdx == offset + len) return;

    // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
    b0 = readByte();
    b1 = readByte();
    b2 = readByte();
    b3 = readByte();
    b4 = readByte();
    b5 = readByte();
    b6 = readByte();
    data[curIdx++] = static_cast<int64_t>((b0 << 48) | (b1 << 40) | (b2 << 32) | (b3 << 24) | (b4 << 16) | (b5 << 8) | b6);
  }
}

void RleDecoderV2::unrolledUnpack64(int64_t *data, uint64_t offset, uint64_t len) {
  uint64_t curIdx = offset;
  while (curIdx < offset + len) {
    // Exhaust the buffer
    int64_t bufferNum = (bufferEnd - bufferStart) / 8;
    bufferNum = std::min(bufferNum, static_cast<int64_t>(offset + len - curIdx));
    uint64_t b0, b1, b2, b3, b4, b5, b6, b7;
    // Avoid updating 'bufferStart' inside the loop.
    const auto* buffer = reinterpret_cast<const unsigned char*>(bufferStart);
    for (int i = 0; i < bufferNum; ++i) {
      b0 = static_cast<uint32_t>(*buffer);
      b1 = static_cast<uint32_t>(*(buffer + 1));
      b2 = static_cast<uint32_t>(*(buffer + 2));
      b3 = static_cast<uint32_t>(*(buffer + 3));
      b4 = static_cast<uint32_t>(*(buffer + 4));
      b5 = static_cast<uint32_t>(*(buffer + 5));
      b6 = static_cast<uint32_t>(*(buffer + 6));
      b7 = static_cast<uint32_t>(*(buffer + 7));
      buffer += 8;
      data[curIdx++] = static_cast<int64_t>((b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) | (b4 << 24) | (b5 << 16) | (b6 << 8) | b7);
    }
    bufferStart = reinterpret_cast<const char*>(buffer);
    if (curIdx == offset + len) return;

    // One of the following readByte() will update 'bufferStart' and 'bufferEnd'.
    b0 = readByte();
    b1 = readByte();
    b2 = readByte();
    b3 = readByte();
    b4 = readByte();
    b5 = readByte();
    b6 = readByte();
    b7 = readByte();
    data[curIdx++] = static_cast<int64_t>((b0 << 56) | (b1 << 48) | (b2 << 40) | (b3 << 32) | (b4 << 24) | (b5 << 16) | (b6 << 8) | b7);
  }
}

void RleDecoderV2::plainUnpackLongs(int64_t *data, uint64_t offset, uint64_t len,
                                    uint64_t fbs, uint64_t& startBit) {
  for (uint64_t i = offset; i < (offset + len); i++) {
    uint64_t result = 0;
    uint64_t bitsLeftToRead = fbs;
    while (bitsLeftToRead > bitsLeft) {
      result <<= bitsLeft;
      result |= curByte & ((1 << bitsLeft) - 1);
      bitsLeftToRead -= bitsLeft;
      curByte = readByte();
      bitsLeft = 8;
    }

    // handle the left over bits
    if (bitsLeftToRead > 0) {
      result <<= bitsLeftToRead;
      bitsLeft -= static_cast<uint32_t>(bitsLeftToRead);
      result |= (curByte >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
    }
    data[i] = static_cast<int64_t>(result);
    startBit = bitsLeft == 0 ? 0 : (8 - bitsLeft);
  }
}

RleDecoderV2::RleDecoderV2(std::unique_ptr<SeekableInputStream> input,
                           bool _isSigned, MemoryPool& pool,
                           ReaderMetrics* _metrics
                           ): RleDecoder(_metrics),
                              inputStream(std::move(input)),
                              isSigned(_isSigned),
                              firstByte(0),
                              runLength(0),
                              runRead(0),
                              bufferStart(nullptr),
                              bufferEnd(bufferStart),
                              bitsLeft(0),
                              curByte(0),
                              unpackedPatch(pool, 0),
                              literals(pool, MAX_LITERAL_SIZE) {
  // PASS
}

void RleDecoderV2::seek(PositionProvider& location) {
  // move the input stream
  inputStream->seek(location);
  // clear state
  bufferEnd = bufferStart = nullptr;
  runRead = runLength = 0;
  // skip ahead the given number of records
  skip(location.next());
}

void RleDecoderV2::skip(uint64_t numValues) {
  // simple for now, until perf tests indicate something encoding specific is
  // needed
  const uint64_t N = 64;
  int64_t dummy[N];

  while (numValues) {
    uint64_t nRead = std::min(N, numValues);
    next(dummy, nRead, nullptr);
    numValues -= nRead;
  }
}

void RleDecoderV2::next(int64_t* const data,
                        const uint64_t numValues,
                        const char* const notNull) {
  SCOPED_STOPWATCH(metrics, DecodingLatencyUs, DecodingCall);
  uint64_t nRead = 0;

  while (nRead < numValues) {
    // Skip any nulls before attempting to read first byte.
    while (notNull && !notNull[nRead]) {
      if (++nRead == numValues) {
        return; // ended with null values
      }
    }

    if (runRead == runLength) {
      resetRun();
      firstByte = readByte();
    }

    uint64_t offset = nRead, length = numValues - nRead;

    EncodingType enc = static_cast<EncodingType>
        ((firstByte >> 6) & 0x03);
    switch(static_cast<int64_t>(enc)) {
    case SHORT_REPEAT:
      nRead += nextShortRepeats(data, offset, length, notNull);
      break;
    case DIRECT:
      nRead += nextDirect(data, offset, length, notNull);
      break;
    case PATCHED_BASE:
      nRead += nextPatched(data, offset, length, notNull);
      break;
    case DELTA:
      nRead += nextDelta(data, offset, length, notNull);
      break;
    default:
      throw ParseError("unknown encoding");
    }
  }
}

uint64_t RleDecoderV2::nextShortRepeats(int64_t* const data,
                                        uint64_t offset,
                                        uint64_t numValues,
                                        const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bytes
    uint64_t byteSize = (firstByte >> 3) & 0x07;
    byteSize += 1;

    runLength = firstByte & 0x07;
    // run lengths values are stored only after MIN_REPEAT value is met
    runLength += MIN_REPEAT;
    runRead = 0;

    // read the repeated value which is store using fixed bytes
    literals[0] = readLongBE(byteSize);

    if (isSigned) {
      literals[0] = unZigZag(static_cast<uint64_t>(literals[0]));
    }
  }

  uint64_t nRead = std::min(runLength - runRead, numValues);

  if (notNull) {
    for(uint64_t pos = offset; pos < offset + nRead; ++pos) {
      if (notNull[pos]) {
        data[pos] = literals[0];
        ++runRead;
      }
    }
  } else {
    for(uint64_t pos = offset; pos < offset + nRead; ++pos) {
      data[pos] = literals[0];
      ++runRead;
    }
  }

  return nRead;
}

uint64_t RleDecoderV2::nextDirect(int64_t* const data,
                                  uint64_t offset,
                                  uint64_t numValues,
                                  const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    uint32_t bitSize = decodeBitWidth(fbo);

    // extract the run length
    runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
    runLength |= readByte();
    // runs are one off
    runLength += 1;
    runRead = 0;

    readLongs(literals.data(), 0, runLength, bitSize);
    if (isSigned) {
      for (uint64_t i = 0; i < runLength; ++i) {
        literals[i] = unZigZag(static_cast<uint64_t>(literals[i]));
      }
    }
  }

  return copyDataFromBuffer(data, offset, numValues, notNull);
}

void RleDecoderV2::adjustGapAndPatch(uint32_t patchBitSize, int64_t patchMask,
                                     int64_t* resGap, int64_t* resPatch,
                                     uint64_t* patchIdx) {
  uint64_t idx = *patchIdx;
  uint64_t gap = static_cast<uint64_t>(unpackedPatch[idx]) >> patchBitSize;
  int64_t patch = unpackedPatch[idx] & patchMask;
  int64_t actualGap = 0;

  // special case: gap is >255 then patch value will be 0.
  // if gap is <=255 then patch value cannot be 0
  while (gap == 255 && patch == 0) {
    actualGap += 255;
    ++idx;
    gap = static_cast<uint64_t>(unpackedPatch[idx]) >> patchBitSize;
    patch = unpackedPatch[idx] & patchMask;
  }
  // add the left over gap
  actualGap += gap;

  *resGap = actualGap;
  *resPatch = patch;
  *patchIdx = idx;
}

uint64_t RleDecoderV2::nextPatched(int64_t* const data,
                                   uint64_t offset,
                                   uint64_t numValues,
                                   const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    uint32_t bitSize = decodeBitWidth(fbo);

    // extract the run length
    runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
    runLength |= readByte();
    // runs are one off
    runLength += 1;
    runRead = 0;

    // extract the number of bytes occupied by base
    uint64_t thirdByte = readByte();
    uint64_t byteSize = (thirdByte >> 5) & 0x07;
    // base width is one off
    byteSize += 1;

    // extract patch width
    uint32_t pwo = thirdByte & 0x1f;
    uint32_t patchBitSize = decodeBitWidth(pwo);

    // read fourth byte and extract patch gap width
    uint64_t fourthByte = readByte();
    uint32_t pgw = (fourthByte >> 5) & 0x07;
    // patch gap width is one off
    pgw += 1;

    // extract the length of the patch list
    size_t pl = fourthByte & 0x1f;
    if (pl == 0) {
      throw ParseError("Corrupt PATCHED_BASE encoded data (pl==0)!");
    }

    // read the next base width number of bytes to extract base value
    int64_t base = readLongBE(byteSize);
    int64_t mask = (static_cast<int64_t>(1) << ((byteSize * 8) - 1));
    // if mask of base value is 1 then base is negative value else positive
    if ((base & mask) != 0) {
      base = base & ~mask;
      base = -base;
    }

    readLongs(literals.data(), 0, runLength, bitSize);
    // any remaining bits are thrown out
    resetReadLongs();

    // TODO: something more efficient than resize
    unpackedPatch.resize(pl);
    // TODO: Skip corrupt?
    //    if ((patchBitSize + pgw) > 64 && !skipCorrupt) {
    if ((patchBitSize + pgw) > 64) {
      throw ParseError("Corrupt PATCHED_BASE encoded data "
                       "(patchBitSize + pgw > 64)!");
    }
    uint32_t cfb = getClosestFixedBits(patchBitSize + pgw);
    readLongs(unpackedPatch.data(), 0, pl, cfb);
    // any remaining bits are thrown out
    resetReadLongs();

    // apply the patch directly when decoding the packed data
    int64_t patchMask = ((static_cast<int64_t>(1) << patchBitSize) - 1);

    int64_t gap = 0;
    int64_t patch = 0;
    uint64_t patchIdx = 0;
    adjustGapAndPatch(patchBitSize, patchMask, &gap, &patch, &patchIdx);

    for (uint64_t i = 0; i < runLength; ++i) {
      if (static_cast<int64_t>(i) != gap) {
        // no patching required. add base to unpacked value to get final value
        literals[i] += base;
      } else {
        // extract the patch value
        int64_t patchedVal = literals[i] | (patch << bitSize);

        // add base to patched value
        literals[i] = base + patchedVal;

        // increment the patch to point to next entry in patch list
        ++patchIdx;

        if (patchIdx < unpackedPatch.size()) {
          adjustGapAndPatch(patchBitSize, patchMask, &gap, &patch,
                            &patchIdx);

          // next gap is relative to the current gap
          gap += i;
        }
      }
    }
  }

  return copyDataFromBuffer(data, offset, numValues, notNull);
}

uint64_t RleDecoderV2::nextDelta(int64_t* const data,
                                 uint64_t offset,
                                 uint64_t numValues,
                                 const char* const notNull) {
  if (runRead == runLength) {
    // extract the number of fixed bits
    unsigned char fbo = (firstByte >> 1) & 0x1f;
    uint32_t bitSize;
    if (fbo != 0) {
      bitSize = decodeBitWidth(fbo);
    } else {
      bitSize = 0;
    }

    // extract the run length
    runLength = static_cast<uint64_t>(firstByte & 0x01) << 8;
    runLength |= readByte();
    ++runLength; // account for first value
    runRead = 0;

    int64_t prevValue;
    // read the first value stored as vint
    if (isSigned) {
      prevValue = readVslong();
    } else {
      prevValue = static_cast<int64_t>(readVulong());
    }

    literals[0] = prevValue;

    // read the fixed delta value stored as vint (deltas can be negative even
    // if all number are positive)
    int64_t deltaBase = readVslong();

    if (bitSize == 0) {
      // add fixed deltas to adjacent values
      for (uint64_t i = 1; i < runLength; ++i) {
        literals[i] = literals[i - 1] + deltaBase;
      }
    } else {
      prevValue = literals[1] = prevValue + deltaBase;
      if (runLength < 2) {
        std::stringstream ss;
        ss << "Illegal run length for delta encoding: " << runLength;
        throw ParseError(ss.str());
      }
      // write the unpacked values, add it to previous value and store final
      // value to result buffer. if the delta base value is negative then it
      // is a decreasing sequence else an increasing sequence.
      // read deltas using the literals buffer.
      readLongs(literals.data(), 2, runLength - 2, bitSize);
      if (deltaBase < 0) {
        for (uint64_t i = 2; i < runLength; ++i) {
          prevValue = literals[i] = prevValue - literals[i];
        }
      } else {
        for (uint64_t i = 2; i < runLength; ++i) {
          prevValue = literals[i] = prevValue + literals[i];
        }
      }
    }
  }

  return copyDataFromBuffer(data, offset, numValues, notNull);
}

uint64_t RleDecoderV2::copyDataFromBuffer(int64_t* data, uint64_t offset,
                                          uint64_t numValues, const char* notNull) {
  uint64_t nRead = std::min(runLength - runRead, numValues);
  if (notNull) {
    for (uint64_t i = offset; i < (offset + nRead); ++i) {
      if (notNull[i]) {
        data[i] = literals[runRead++];
      }
    }
  } else {
    memcpy(data + offset, literals.data() + runRead, nRead * sizeof(int64_t));
    runRead += nRead;
  }
  return nRead;
}

}  // namespace orc
