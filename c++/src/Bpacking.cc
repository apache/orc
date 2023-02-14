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

#include "Bpacking.hh"
#include "CpuInfoUtil.hh"

namespace orc {
  int readLongsDefault(RleDecoderV2* decoder, int64_t* data, uint64_t offset, uint64_t len,
                       uint64_t fbs) {
    UnpackDefault unpackDefault(decoder);
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
    return 0;
  }

#if defined(ORC_HAVE_RUNTIME_AVX512)
  int readLongsAvx512(RleDecoderV2* decoder, int64_t* data, uint64_t offset, uint64_t len,
                      uint64_t fbs) {
    UnpackAvx512 unpackAvx512(decoder);
    UnpackDefault unpackDefault(decoder);
    uint64_t startBit = 0;
    static const auto cpu_info = orc::CpuInfo::GetInstance();
    if (cpu_info->IsSupported(CpuInfo::AVX512)) {
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

    return 0;
  }
#endif

}  // namespace orc
