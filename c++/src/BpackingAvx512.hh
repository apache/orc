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

#include <stdint.h>
#include <stdlib.h>

#include "BpackingDefault.hh"
#include "Dispatch.hh"
#include "RLEv2.hh"
#include "io/InputStream.hh"
#include "io/OutputStream.hh"

namespace orc {

#define MAX_VECTOR_BUF_8BIT_LENGTH 64
#define MAX_VECTOR_BUF_16BIT_LENGTH 32
#define MAX_VECTOR_BUF_32BIT_LENGTH 16

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

   private:
    RleDecoderV2* decoder;
    UnpackDefault unpackDefault;

    // Used by vectorially 1~8 bit-unpacking data
    uint8_t vectorBuf8[MAX_VECTOR_BUF_8BIT_LENGTH + 1];
    // Used by vectorially 9~16 bit-unpacking data
    uint16_t vectorBuf16[MAX_VECTOR_BUF_16BIT_LENGTH + 1];
    // Used by vectorially 17~32 bit-unpacking data
    uint32_t vectorBuf32[MAX_VECTOR_BUF_32BIT_LENGTH + 1];
  };

  class BitUnpackAVX512 : public BitUnpack {
   public:
    static int readLongs(RleDecoderV2* decoder, int64_t* data, uint64_t offset, uint64_t len, uint64_t fbs);
  };

}  // namespace orc

#endif
