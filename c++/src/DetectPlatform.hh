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

#ifndef ORC_DETECTPLATFORM_HH
#define ORC_DETECTPLATFORM_HH

#if defined(__GNUC__) || defined(__clang__)
DIAGNOSTIC_IGNORE("-Wold-style-cast")
#endif

namespace orc {
#ifdef _WIN32

#include "intrin.h"
//  Windows CPUID
#define cpuid(info, x) __cpuidex(info, x, 0)
#else
//  GCC Intrinsics
#include <cpuid.h>
#include <dlfcn.h>

  void cpuid(int info[4], int InfoType) {
    __cpuid_count(InfoType, 0, info[0], info[1], info[2], info[3]);
  }

  unsigned long long xgetbv(unsigned int index) {
    unsigned int eax, edx;
    __asm__ __volatile__("xgetbv;" : "=a"(eax), "=d"(edx) : "c"(index));
    return ((unsigned long long)edx << 32) | eax;
  }

#endif

#define CPUID_AVX512F 0x00100000
#define CPUID_AVX512CD 0x00200000
#define CPUID_AVX512VL 0x04000000
#define CPUID_AVX512BW 0x01000000
#define CPUID_AVX512DQ 0x02000000
#define EXC_OSXSAVE 0x08000000  // 27th  bit

#define CPUID_AVX512_MASK \
  (CPUID_AVX512F | CPUID_AVX512CD | CPUID_AVX512VL | CPUID_AVX512BW | CPUID_AVX512DQ)

  enum class Arch { PX_ARCH = 0, AVX2_ARCH = 1, AVX512_ARCH = 2 };

  Arch detectPlatform() {
    Arch detected_platform = Arch::PX_ARCH;
    int cpuInfo[4];
    cpuid(cpuInfo, 1);

    bool avx512_support_cpu = cpuInfo[1] & CPUID_AVX512_MASK;
    bool os_uses_XSAVE_XSTORE = cpuInfo[2] & EXC_OSXSAVE;

    if (avx512_support_cpu && os_uses_XSAVE_XSTORE) {
      // Check if XMM state and YMM state are saved
#ifdef _WIN32
      unsigned long long xcr_feature_mask = _xgetbv(0); /* min VS2010 SP1 compiler is required */
#else
      unsigned long long xcr_feature_mask = xgetbv(0);
#endif

      if ((xcr_feature_mask & 0x6) == 0x6) {      // AVX2 is supported now
        if ((xcr_feature_mask & 0xe0) == 0xe0) {  // AVX512 is supported now
          detected_platform = Arch::AVX512_ARCH;
        }
      }
    }

    return detected_platform;
  }
}  // namespace orc

#endif
