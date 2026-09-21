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

#include "BpackingRvv.hh"

#if defined(ORC_HAVE_RUNTIME_RVV) && defined(__riscv)

#include <riscv_vector.h>

#include "CpuInfoUtil.hh"
#include "RLEv2.hh"

namespace orc {

  namespace {
    // Returns VLEN/8 elements per vector block. Most kernels use e8m1 groups,
    // so each block always consumes whole bytes. The gather indices are u8,
    // so VLEN must be <=256. We cache this value because vsetvli has a
    // non-negligible cost and should not be called repeatedly.
    inline uint64_t vecElems() {
      static const uint64_t elems = __riscv_vsetvlmax_e8m1();
      return elems;
    }

    // Constants for each bit width. The bitstream is big-endian, so value i
    // starts at bit i*W. NB bytes from that byte offset cover the whole value.
    // vrgather.vv collects those bytes into lanes, then we assemble the word,
    // shift right, and mask.
    template <uint32_t W>
    struct KernelTraits {
      // Bytes per element window.
      static constexpr int NB = (W == 8)                          ? 1
                                : (W == 16)                       ? 2
                                : (W == 1 || W == 2 || W == 4)    ? 1
                                : (W == 3 || (W >= 5 && W <= 7))  ? 2
                                : (W >= 9 && W <= 15)             ? 3
                                : (W >= 17 && W <= 23)            ? 4
                                : (W == 24)                       ? 3
                                : (W == 32)                       ? 4
                                : (W == 26 || W == 28 || W == 30) ? 5
                                : (W == 40)                       ? 5
                                : (W == 48)                       ? 6
                                : (W == 56)                       ? 7
                                                                  : 8;
      // Kernel kind: 1=u8 (W=4), 2=u16 (W=3,5-7), 3=byte copy (W=8),
      // 4=u32 (9-24,32), 5=u64 from u8m4 (26,28,30), 6=strided u64 load (40,48,56).
      // W=1,2,64 use scalar fallback.
      static constexpr int KIND = (W == 4)                          ? 1
                                  : (W == 3 || (W >= 5 && W <= 7))  ? 2
                                  : (W == 8)                        ? 3
                                  : (NB <= 4)                       ? 4
                                  : (W == 26 || W == 28 || W == 30) ? 5
                                                                    : 6;
      static constexpr uint8_t mask8() {
        return static_cast<uint8_t>((1u << W) - 1);
      }
      static constexpr uint16_t mask16() {
        return static_cast<uint16_t>((1u << W) - 1);
      }
      static constexpr uint32_t mask32() {
        return static_cast<uint32_t>((uint64_t{1} << W) - 1);
      }
      static constexpr uint64_t mask64() {
        return W == 64 ? ~uint64_t{0} : (uint64_t{1} << W) - 1;
      }
    };

    // KIND 1 kernel (W==4): every value fits in one byte.
    template <uint32_t W>
    inline void unpackBlock1(const uint8_t* src, int64_t* dst, vuint8m1_t idx, vuint8m1_t shift,
                             uint64_t tableBytes, size_t vl) {
      vuint8m1_t table = __riscv_vle8_v_u8m1(src, tableBytes);
      vuint8m1_t g = __riscv_vrgather_vv_u8m1(table, idx, vl);
      vuint8m1_t val =
          __riscv_vand_vx_u8m1(__riscv_vsrl_vv_u8m1(g, shift, vl), KernelTraits<W>::mask8(), vl);
      __riscv_vse64_v_u64m8(reinterpret_cast<uint64_t*>(dst), __riscv_vzext_vf8_u64m8(val, vl), vl);
    }

    // KIND 2 kernel (W in 3/5/6/7): every value fits in a 2-byte window,
    // assembled big-endian in u16 lanes.
    template <uint32_t W>
    inline void unpackBlock2(const uint8_t* src, int64_t* dst, vuint8m1_t i0, vuint8m1_t i1,
                             vuint16m2_t shift, uint64_t tableBytes, size_t vl) {
      vuint8m1_t table = __riscv_vle8_v_u8m1(src, tableBytes);
      vuint8m1_t g0 = __riscv_vrgather_vv_u8m1(table, i0, vl);
      vuint8m1_t g1 = __riscv_vrgather_vv_u8m1(table, i1, vl);
      vuint16m2_t w = __riscv_vsll_vx_u16m2(__riscv_vzext_vf2_u16m2(g0, vl), 8, vl);
      w = __riscv_vor_vv_u16m2(w, __riscv_vzext_vf2_u16m2(g1, vl), vl);
      vuint16m2_t val =
          __riscv_vand_vx_u16m2(__riscv_vsrl_vv_u16m2(w, shift, vl), KernelTraits<W>::mask16(), vl);
      __riscv_vse64_v_u64m8(reinterpret_cast<uint64_t*>(dst), __riscv_vzext_vf4_u64m8(val, vl), vl);
    }

    // Gather the bytes of every element's window from the loaded table and
    // assemble the big-endian word: word = g0<<(8*(NB-1)) | g1<<(8*(NB-2)) ...
    template <int NB>
    inline vuint32m4_t assemble32(vuint8m4_t table, vuint8m4_t i0, [[maybe_unused]] vuint8m4_t i1,
                                  [[maybe_unused]] vuint8m4_t i2, [[maybe_unused]] vuint8m4_t i3,
                                  size_t vl) {
      vuint8m4_t g = __riscv_vrgather_vv_u8m4(table, i0, vl);
      vuint32m4_t w = __riscv_vsll_vx_u32m4(
          __riscv_vzext_vf4_u32m4(__riscv_vget_v_u8m4_u8m1(g, 0), vl), 8 * (NB - 1), vl);
      if constexpr (NB >= 2) {
        g = __riscv_vrgather_vv_u8m4(table, i1, vl);
        w = __riscv_vor_vv_u32m4(
            w,
            __riscv_vsll_vx_u32m4(__riscv_vzext_vf4_u32m4(__riscv_vget_v_u8m4_u8m1(g, 0), vl),
                                  8 * (NB - 2), vl),
            vl);
      }
      if constexpr (NB >= 3) {
        g = __riscv_vrgather_vv_u8m4(table, i2, vl);
        w = __riscv_vor_vv_u32m4(
            w,
            __riscv_vsll_vx_u32m4(__riscv_vzext_vf4_u32m4(__riscv_vget_v_u8m4_u8m1(g, 0), vl),
                                  8 * (NB - 3), vl),
            vl);
      }
      if constexpr (NB >= 4) {
        g = __riscv_vrgather_vv_u8m4(table, i3, vl);
        w = __riscv_vor_vv_u32m4(w, __riscv_vzext_vf4_u32m4(__riscv_vget_v_u8m4_u8m1(g, 0), vl),
                                 vl);
      }
      return w;
    }

    // Same assembly into u64m8 lanes from a u8m4 table (windows up to 5 bytes).
    template <int NB>
    inline vuint64m8_t assemble64(vuint8m4_t table, vuint8m4_t j0, [[maybe_unused]] vuint8m4_t j1,
                                  [[maybe_unused]] vuint8m4_t j2, [[maybe_unused]] vuint8m4_t j3,
                                  [[maybe_unused]] vuint8m4_t j4, size_t vl) {
      vuint8m4_t g = __riscv_vrgather_vv_u8m4(table, j0, vl);
      vuint64m8_t w = __riscv_vsll_vx_u64m8(
          __riscv_vzext_vf8_u64m8(__riscv_vget_v_u8m4_u8m1(g, 0), vl), 8 * (NB - 1), vl);
      if constexpr (NB >= 2) {
        g = __riscv_vrgather_vv_u8m4(table, j1, vl);
        w = __riscv_vor_vv_u64m8(
            w,
            __riscv_vsll_vx_u64m8(__riscv_vzext_vf8_u64m8(__riscv_vget_v_u8m4_u8m1(g, 0), vl),
                                  8 * (NB - 2), vl),
            vl);
      }
      if constexpr (NB >= 3) {
        g = __riscv_vrgather_vv_u8m4(table, j2, vl);
        w = __riscv_vor_vv_u64m8(
            w,
            __riscv_vsll_vx_u64m8(__riscv_vzext_vf8_u64m8(__riscv_vget_v_u8m4_u8m1(g, 0), vl),
                                  8 * (NB - 3), vl),
            vl);
      }
      if constexpr (NB >= 4) {
        g = __riscv_vrgather_vv_u8m4(table, j3, vl);
        w = __riscv_vor_vv_u64m8(
            w,
            __riscv_vsll_vx_u64m8(__riscv_vzext_vf8_u64m8(__riscv_vget_v_u8m4_u8m1(g, 0), vl),
                                  8 * (NB - 4), vl),
            vl);
      }
      if constexpr (NB >= 5) {
        g = __riscv_vrgather_vv_u8m4(table, j4, vl);
        w = __riscv_vor_vv_u64m8(w, __riscv_vzext_vf8_u64m8(__riscv_vget_v_u8m4_u8m1(g, 0), vl),
                                 vl);
      }
      return w;
    }

    // KIND 4 kernel (u32 lanes): W in 9..15 (NB=3), 16 (NB=2), 17..23 (NB=4),
    // 24 (NB=3), 32 (NB=4).
    template <uint32_t W>
    inline void unpackBlock32(const uint8_t* src, int64_t* dst, vuint8m4_t i0, vuint8m4_t i1,
                              vuint8m4_t i2, vuint8m4_t i3, vuint32m4_t shift, uint64_t tableBytes,
                              size_t vl) {
      constexpr int NB = KernelTraits<W>::NB;
      vuint8m4_t table = __riscv_vle8_v_u8m4(src, tableBytes);
      vuint32m4_t word = assemble32<NB>(table, i0, i1, i2, i3, vl);
      vuint32m4_t val = __riscv_vsrl_vv_u32m4(word, shift, vl);
      val = __riscv_vand_vx_u32m4(val, KernelTraits<W>::mask32(), vl);
      __riscv_vse64_v_u64m8(reinterpret_cast<uint64_t*>(dst), __riscv_vzext_vf2_u64m8(val, vl), vl);
    }

    // KIND 5 kernel (u64 lanes from a u8m4 table): W in 26, 28, 30.
    template <uint32_t W>
    inline void unpackBlock64(const uint8_t* src, int64_t* dst, vuint8m4_t j0, vuint8m4_t j1,
                              vuint8m4_t j2, vuint8m4_t j3, vuint8m4_t j4, vuint64m8_t shift,
                              uint64_t tableBytes, size_t vl) {
      constexpr int NB = KernelTraits<W>::NB;
      vuint8m4_t table = __riscv_vle8_v_u8m4(src, tableBytes);
      vuint64m8_t word = assemble64<NB>(table, j0, j1, j2, j3, j4, vl);
      vuint64m8_t val = __riscv_vsrl_vv_u64m8(word, shift, vl);
      val = __riscv_vand_vx_u64m8(val, KernelTraits<W>::mask64(), vl);
      __riscv_vse64_v_u64m8(reinterpret_cast<uint64_t*>(dst), val, vl);
    }

    // KIND 6 kernel (u64 lanes, single strided u64 load): W in 40, 48, 56.
    // These widths are byte-aligned, so element i spans NB consecutive bytes.
    // A strided u64 load pulls those bytes (plus up to 8-NB look-ahead bytes)
    // into each lane. Byte-reversing the lane turns memory order into bitstream
    // order, then a shift drops the look-ahead bytes.
    template <uint32_t W>
    inline void unpackBlock64Strided(const uint8_t* src, int64_t* dst, size_t vl) {
      constexpr int NB = KernelTraits<W>::NB;
      constexpr uint64_t SHIFT = 8 * (8 - NB);
      constexpr uint64_t kByteMask = 0x00FF00FF00FF00FFULL;  // keep even byte positions
      constexpr uint64_t kWordMask = 0x0000FFFF0000FFFFULL;  // keep even 16-bit groups

      vuint64m8_t u = __riscv_vlse64_v_u64m8(reinterpret_cast<const uint64_t*>(src), NB, vl);

      // Byte-reverse every lane: swap adjacent bytes, then 16-bit groups, then
      // 32-bit groups. All operations are pure ALU (no gather, no vbrev8).
      vuint64m8_t x = __riscv_vor_vv_u64m8(
          __riscv_vand_vx_u64m8(__riscv_vsll_vx_u64m8(u, 8, vl), ~kByteMask, vl),
          __riscv_vand_vx_u64m8(__riscv_vsrl_vx_u64m8(u, 8, vl), kByteMask, vl), vl);
      x = __riscv_vor_vv_u64m8(
          __riscv_vand_vx_u64m8(__riscv_vsll_vx_u64m8(x, 16, vl), ~kWordMask, vl),
          __riscv_vand_vx_u64m8(__riscv_vsrl_vx_u64m8(x, 16, vl), kWordMask, vl), vl);
      vuint64m8_t val = __riscv_vor_vv_u64m8(__riscv_vsll_vx_u64m8(x, 32, vl),
                                             __riscv_vsrl_vx_u64m8(x, 32, vl), vl);
      // Drop the look-ahead bytes; value is right-aligned and already zero above.
      val = __riscv_vsrl_vx_u64m8(val, SHIFT, vl);
      __riscv_vse64_v_u64m8(reinterpret_cast<uint64_t*>(dst), val, vl);
    }

    inline void unpackBlock8(const uint8_t* src, int64_t* dst, size_t vl) {
      vuint8m1_t v = __riscv_vle8_v_u8m1(src, vl);
      __riscv_vse64_v_u64m8(reinterpret_cast<uint64_t*>(dst), __riscv_vzext_vf8_u64m8(v, vl), vl);
    }

    // Returns the smallest k such that (k*W + startBit) % 8 == 0.
    // Returns UINT64_MAX if no such k exists (i.e., alignment is impossible).
    inline uint64_t alignCount(uint32_t bitWidth, uint64_t startBit) {
      for (uint64_t k = 1; k <= 8; ++k) {
        if ((k * bitWidth + startBit) % 8 == 0) return k;
      }
      return UINT64_MAX;
    }

    template <uint32_t W>
    inline void unpackValues(RleDecoderV2* decoder, UnpackDefault& unpackDefault, int64_t* data,
                             uint64_t offset, uint64_t len) {
      constexpr int NB = KernelTraits<W>::NB;
      constexpr int KIND = KernelTraits<W>::KIND;
      const uint64_t elemsPerBlock = vecElems();
      const uint64_t blockBytes = elemsPerBlock * W / 8;
      const uint64_t tableBytes = ((elemsPerBlock - 1) * W) / 8 + (KIND == 6 ? 8 : NB);
      const size_t vl = static_cast<size_t>(elemsPerBlock);

      if (len < elemsPerBlock) {
        unpackDefault.plainUnpackLongs(data, offset, len, W);
        return;
      }

      // Compute index and shift vectors once per width; they're VLEN-dependent.
      // RVV types can't be stored in structs, so keep them as local variables.
      [[maybe_unused]] vuint8m1_t k1Idx, k1Shift, k2Idx0, k2Idx1;
      [[maybe_unused]] vuint16m2_t k2Shift;
      [[maybe_unused]] vuint8m4_t i0, i1, i2, i3;
      [[maybe_unused]] vuint8m4_t j0, j1, j2, j3, j4;
      [[maybe_unused]] vuint32m4_t shift32;
      [[maybe_unused]] vuint64m8_t shift64;
      if constexpr (KIND == 1) {
        vuint16m2_t bitPos = __riscv_vmul_vx_u16m2(__riscv_vid_v_u16m2(vl), W, vl);
        k1Idx = __riscv_vncvt_x_x_w_u8m1(__riscv_vsrl_vx_u16m2(bitPos, 3, vl), vl);
        vuint16m2_t ofs = __riscv_vand_vx_u16m2(bitPos, 7, vl);
        k1Shift = __riscv_vncvt_x_x_w_u8m1(__riscv_vrsub_vx_u16m2(ofs, 8 - W, vl), vl);
      } else if constexpr (KIND == 2) {
        vuint16m2_t bitPos = __riscv_vmul_vx_u16m2(__riscv_vid_v_u16m2(vl), W, vl);
        vuint8m1_t byteIdx = __riscv_vncvt_x_x_w_u8m1(__riscv_vsrl_vx_u16m2(bitPos, 3, vl), vl);
        k2Idx0 = byteIdx;
        k2Idx1 = __riscv_vadd_vx_u8m1(byteIdx, 1, vl);
        vuint16m2_t ofs = __riscv_vand_vx_u16m2(bitPos, 7, vl);
        k2Shift = __riscv_vrsub_vx_u16m2(ofs, 16 - W, vl);
      } else if constexpr (KIND == 4) {
        vuint16m2_t bitPos = __riscv_vmul_vx_u16m2(__riscv_vid_v_u16m2(vl), W, vl);
        vuint8m1_t byteIdx = __riscv_vncvt_x_x_w_u8m1(__riscv_vsrl_vx_u16m2(bitPos, 3, vl), vl);
        vuint8m4_t base = __riscv_vlmul_ext_v_u8m1_u8m4(byteIdx);
        i0 = base;
        i1 = NB >= 2 ? __riscv_vadd_vx_u8m4(base, 1, vl) : base;
        i2 = NB >= 3 ? __riscv_vadd_vx_u8m4(base, 2, vl) : base;
        i3 = NB >= 4 ? __riscv_vadd_vx_u8m4(base, 3, vl) : base;
        vuint32m4_t ofs =
            __riscv_vand_vx_u32m4(__riscv_vmul_vx_u32m4(__riscv_vid_v_u32m4(vl), W, vl), 7, vl);
        shift32 = __riscv_vrsub_vx_u32m4(ofs, 8 * NB - W, vl);
      } else if constexpr (KIND == 5) {
        vuint16m2_t bitPos = __riscv_vmul_vx_u16m2(__riscv_vid_v_u16m2(vl), W, vl);
        vuint8m1_t byteIdx = __riscv_vncvt_x_x_w_u8m1(__riscv_vsrl_vx_u16m2(bitPos, 3, vl), vl);
        vuint8m4_t base = __riscv_vlmul_ext_v_u8m1_u8m4(byteIdx);
        j0 = base;
        j1 = __riscv_vadd_vx_u8m4(base, 1, vl);
        j2 = __riscv_vadd_vx_u8m4(base, 2, vl);
        j3 = __riscv_vadd_vx_u8m4(base, 3, vl);
        j4 = __riscv_vadd_vx_u8m4(base, 4, vl);
        vuint64m8_t ofs =
            __riscv_vand_vx_u64m8(__riscv_vmul_vx_u64m8(__riscv_vid_v_u64m8(vl), W, vl), 7, vl);
        shift64 = __riscv_vrsub_vx_u64m8(ofs, 8 * NB - W, vl);
      }

      int64_t* dstPtr = data + offset;
      while (len > 0) {
        // Vector kernels need the bit position to be byte-aligned. If we can't
        // reach alignment, fall back to scalar for the rest.
        if (decoder->getBitsLeft() != 0) {
          uint64_t k = alignCount(W, 8 - decoder->getBitsLeft());
          if (k >= len) {
            unpackDefault.plainUnpackLongs(dstPtr, 0, len, W);
            return;
          }
          unpackDefault.plainUnpackLongs(dstPtr, 0, k, W);
          dstPtr += k;
          len -= k;
        }

        const uint8_t* srcPtr = reinterpret_cast<const uint8_t*>(decoder->getBufStart());
        uint64_t avail = decoder->bufLength();
        uint64_t consumed = 0;
        // Each block consumes exactly blockBytes bytes; tableBytes is the max
        // window size, so we never overread the buffer.
        while (len >= elemsPerBlock && consumed + tableBytes <= avail) {
          if constexpr (KIND == 1) {
            unpackBlock1<W>(srcPtr + consumed, dstPtr, k1Idx, k1Shift, tableBytes, vl);
          } else if constexpr (KIND == 2) {
            unpackBlock2<W>(srcPtr + consumed, dstPtr, k2Idx0, k2Idx1, k2Shift, tableBytes, vl);
          } else if constexpr (KIND == 3) {
            unpackBlock8(srcPtr + consumed, dstPtr, vl);
          } else if constexpr (KIND == 4) {
            unpackBlock32<W>(srcPtr + consumed, dstPtr, i0, i1, i2, i3, shift32, tableBytes, vl);
          } else if constexpr (KIND == 5) {
            unpackBlock64<W>(srcPtr + consumed, dstPtr, j0, j1, j2, j3, j4, shift64, tableBytes,
                             vl);
          } else {
            unpackBlock64Strided<W>(srcPtr + consumed, dstPtr, vl);
          }
          consumed += blockBytes;
          dstPtr += elemsPerBlock;
          len -= elemsPerBlock;
        }
        if (consumed > 0) {
          decoder->resetBufferStart(consumed, false, 0);
        }
        if (len == 0) return;
        if (len < elemsPerBlock) {
          unpackDefault.plainUnpackLongs(dstPtr, 0, len, W);
          return;
        }
        // Buffer too short for a full block. Use scalar to consume enough
        // values (and possibly refill), then loop back.
        uint64_t elems = (decoder->bufLength() * 8) / W + 1;
        if (elems > len) elems = len;
        unpackDefault.plainUnpackLongs(dstPtr, 0, elems, W);
        dstPtr += elems;
        len -= elems;
      }
    }

  }  // namespace
  UnpackRvv::UnpackRvv(RleDecoderV2* dec) : decoder_(dec), unpackDefault_(UnpackDefault(dec)) {}

  UnpackRvv::~UnpackRvv() {}

  void UnpackRvv::vectorUnpack(int64_t* data, uint64_t offset, uint64_t len, uint32_t bitWidth) {
    switch (bitWidth) {
#define ORC_RVV_UNPACK_CASE(width)                                  \
  case width:                                                       \
    unpackValues<width>(decoder, unpackDefault, data, offset, len); \
    break
      ORC_RVV_UNPACK_CASE(3);
      ORC_RVV_UNPACK_CASE(4);
      ORC_RVV_UNPACK_CASE(5);
      ORC_RVV_UNPACK_CASE(6);
      ORC_RVV_UNPACK_CASE(7);
      ORC_RVV_UNPACK_CASE(8);
      ORC_RVV_UNPACK_CASE(9);
      ORC_RVV_UNPACK_CASE(10);
      ORC_RVV_UNPACK_CASE(11);
      ORC_RVV_UNPACK_CASE(12);
      ORC_RVV_UNPACK_CASE(13);
      ORC_RVV_UNPACK_CASE(14);
      ORC_RVV_UNPACK_CASE(15);
      ORC_RVV_UNPACK_CASE(16);
      ORC_RVV_UNPACK_CASE(17);
      ORC_RVV_UNPACK_CASE(18);
      ORC_RVV_UNPACK_CASE(19);
      ORC_RVV_UNPACK_CASE(20);
      ORC_RVV_UNPACK_CASE(21);
      ORC_RVV_UNPACK_CASE(22);
      ORC_RVV_UNPACK_CASE(23);
      ORC_RVV_UNPACK_CASE(24);
      ORC_RVV_UNPACK_CASE(26);
      ORC_RVV_UNPACK_CASE(28);
      ORC_RVV_UNPACK_CASE(30);
      ORC_RVV_UNPACK_CASE(32);
      ORC_RVV_UNPACK_CASE(40);
      ORC_RVV_UNPACK_CASE(48);
      ORC_RVV_UNPACK_CASE(56);
#undef ORC_RVV_UNPACK_CASE
      // Widths 1 and 2 are too narrow for gather to be worthwhile; 64 is
      // better handled by the scalar unrolled path. All fall through.
      case 1:
      case 2:
      case 64:
      default:
        unpackDefault.plainUnpackLongs(data, offset, len, bitWidth);
        break;
    }
  }

  void BitUnpackRVV::readLongs(RleDecoderV2* decoder, int64_t* data, uint64_t offset, uint64_t len,
                               uint64_t fbs) {
    // Fallback for widths 1,2,64 – handle early to avoid wrapper overhead.
    if (fbs == 1 || fbs == 2 || fbs == 64) {
      BitUnpackDefault::readLongs(decoder, data, offset, len, fbs);
      return;
    }
    static const auto cpuInfo = CpuInfo::getInstance();
    static const uint64_t elemsPerBlock = vecElems();
    if (cpuInfo->isSupported(CpuInfo::RVV) && elemsPerBlock <= 32) {
      UnpackRvv unpackRvv(decoder);
      unpackRvv.vectorUnpack(data, offset, len, static_cast<uint32_t>(fbs));
    } else {
      BitUnpackDefault::readLongs(decoder, data, offset, len, fbs);
    }
  }

}  // namespace orc

#endif
