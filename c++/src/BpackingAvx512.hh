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

#ifndef ORC_BPACKINGAVX512_HH
#define ORC_BPACKINGAVX512_HH

#include <cstdint>
#include <cstdlib>

#include "BpackingDefault.hh"

namespace orc {

#define VECTOR_UNPACK_8BIT_MAX_NUM 64
#define VECTOR_UNPACK_16BIT_MAX_NUM 32
#define VECTOR_UNPACK_32BIT_MAX_NUM 16
#define UNPACK_8Bit_MAX_SIZE 8
#define UNPACK_16Bit_MAX_SIZE 16
#define UNPACK_32Bit_MAX_SIZE 32

  class RleDecoderV2;

  class UnpackAvx512 {
   public:
    UnpackAvx512(RleDecoderV2* dec);
    ~UnpackAvx512();

    void vectorUnpack1(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack2(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack3(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack4(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack5(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack6(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack7(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack9(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack10(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack11(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack12(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack13(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack14(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack15(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack16(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack17(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack18(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack19(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack20(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack21(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack22(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack23(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack24(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack26(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack28(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack30(int64_t* data, uint64_t offset, uint64_t len);
    void vectorUnpack32(int64_t* data, uint64_t offset, uint64_t len);

    void plainUnpackLongs(int64_t* data, uint64_t offset, uint64_t len, uint64_t fbs,
                          uint64_t& startBit);

    /**
     * In the processing of AVX512 unpacking, AVX512 instructions can only process the memory align
     * data. It means that if data input is not memory align (@param startBit != 0), we need to
     * process the unaligned data. After that, it could be use AVX512 instructions to process these
     * memory align data.
     *
     * @tparam hasBitOffset If currently processed data has offset bits in one Byte, 8X-bit width
     * data will not have bits offset in one Byte, so it will be false. For other bits data, it will
     * be true.
     * @param bitWidth The unpacking data bit width
     * @param bitMaxSize The unpacking data needs the Max bit size (8X)
     * @param startBit The start bit position in one Byte
     * @param bufMoveByteLen In the current buffer, it will be processed/moved Bytes length in the
     * unpacking
     * @param bufRestByteLen In the current buffer, there will be some rest Bytes length after
     * unpacking
     * @param remainingNumElements After unpacking, the remaining elements number need to be
     * processed
     * @param tailBitLen After unpacking, the tail bits length
     * @param backupByteLen The backup Byte length after unpacking
     * @param numElements Currently, the number of elements need to be processed
     * @param resetBuf When the current buffer has already been processed, it need to be reset the
     * buffer
     * @param srcPtr the pointer of source data
     * @param dstPtr the pointer of destinative data
     */
    template <bool hasBitOffset>
    inline void alignHeaderBoundary(const uint32_t bitWidth, const uint32_t bitMaxSize,
                                    uint64_t& startBit, uint64_t& bufMoveByteLen,
                                    uint64_t& bufRestByteLen, uint64_t& remainingNumElements,
                                    uint64_t& tailBitLen, uint32_t& backupByteLen,
                                    uint64_t& numElements, bool& resetBuf, const uint8_t*& srcPtr,
                                    int64_t*& dstPtr);

    /**
     * After AVX512 unpacking processed, there could be some scattered data not be process,
     * it needs to be processed by the default way.
     *
     * @tparam hasBitOffset If currently processed data has offset bits in one Byte, 8X-bit width
     * data will not have bits offset in one Byte, so it will be false. For other bits data, it will
     * be true.
     * @param bitWidth The unpacking data bit width
     * @param specialBit 8X bit width data is the specialBit, they have the different unpackDefault
     * functions with others
     * @param startBit The start bit position in one Byte
     * @param bufMoveByteLen In the current buffer, it will be processed/moved Bytes length in the
     * unpacking
     * @param bufRestByteLen In the current buffer, there will be some rest Bytes length after
     * unpacking
     * @param remainingNumElements After unpacking, the remaining elements number need to be
     * processed
     * @param backupByteLen The backup Byte length after unpacking
     * @param numElements Currently, the number of elements need to be processed
     * @param resetBuf When the current buffer has already been processed, it need to be reset the
     * buffer
     * @param srcPtr the pointer of source data
     * @param dstPtr the pointer of destinative data
     */
    template <bool hasBitOffset>
    inline void alignTailerBoundary(const uint32_t bitWidth, const uint32_t specialBit,
                                    uint64_t& startBit, uint64_t& bufMoveByteLen,
                                    uint64_t& bufRestByteLen, uint64_t& remainingNumElements,
                                    uint32_t& backupByteLen, uint64_t& numElements, bool& resetBuf,
                                    const uint8_t*& srcPtr, int64_t*& dstPtr);

   private:
    RleDecoderV2* decoder;
    UnpackDefault unpackDefault;

    // Used by vectorized bit-unpacking data
    uint32_t vectorBuf[VECTOR_UNPACK_32BIT_MAX_NUM + 1];
  };

  class BitUnpackAVX512 : public BitUnpack {
   public:
    static void readLongs(RleDecoderV2* decoder, int64_t* data, uint64_t offset, uint64_t len,
                          uint64_t fbs);
  };

}  // namespace orc

#endif
