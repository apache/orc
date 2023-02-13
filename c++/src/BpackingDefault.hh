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

#ifndef ORC_BPACKINGDEFAULT_HH
#define ORC_BPACKINGDEFAULT_HH

#include <stdint.h>
#include <stdlib.h>

// #include "Adaptor.hh"
#include "RLEv2.hh"
#include "io/InputStream.hh"
#include "io/OutputStream.hh"

namespace orc {

  class UnpackDefault {
   public:
    UnpackDefault(RleDecoderV2* dec);
    ~UnpackDefault();

    void unrolledUnpack4(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack8(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack16(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack24(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack32(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack40(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack48(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack56(int64_t* data, uint64_t offset, uint64_t len);
    void unrolledUnpack64(int64_t* data, uint64_t offset, uint64_t len);

    void plainUnpackLongs(int64_t* data, uint64_t offset, uint64_t len, uint64_t fbs);

    /*  void setBuf(char* bufStart, char* bufEnd) {
        bufferStart = bufStart;
        bufferEnd = bufEnd;
      }

      void getBuf(char** bufStart, char** bufEnd) {
        *bufStart = bufferStart;
        *bufEnd = bufferEnd;
      }*/

   private:
    RleDecoderV2* decoder;
    //  char* bufferStart;
    //  char* bufferEnd;
    uint32_t bitsLeft;
    uint32_t curByte;
  };

}  // namespace orc

#endif
