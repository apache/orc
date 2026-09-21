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

#ifndef ORC_BPACKINGRVV_HH
#define ORC_BPACKINGRVV_HH

#include <cstdint>
#include <cstdlib>

#include "BpackingDefault.hh"

namespace orc {

  class RleDecoderV2;

  /**
   * Bit-unpacking with the RISC-V Vector Extension (RVV).
   *
   * This implementation uses the RISC‑V Vector Extension.
   * Each vector operation processes VLEN/8 elements: a single indexed
   * load gathers the packed bytes, per‑lane byte‑reversal handles the big‑endian
   * bit stream, and variable shifts plus masking extract the final values.
   */
  class UnpackRvv {
   public:
    UnpackRvv(RleDecoderV2* dec);
    ~UnpackRvv();

    /**
     * Unpack len values of the given bit width using RVV vector blocks.
     * Falls back to the scalar implementation for bit-level misalignment,
     * buffer boundaries and unsupported bit widths.
     *
     * @param data output buffer
     * @param offset starting offset within output buffer
     * @param len number of values to unpack
     * @param bitWidth bit width for each encoded value
     */
    void vectorUnpack(int64_t* data, uint64_t offset, uint64_t len, uint32_t bitWidth);

   private:
    RleDecoderV2* decoder_;
    UnpackDefault unpackDefault_;
  };

  class BitUnpackRVV : public BitUnpack {
   public:
    static void readLongs(RleDecoderV2* decoder, int64_t* data, uint64_t offset, uint64_t len,
                          uint64_t fbs);
  };

}  // namespace orc

#endif
