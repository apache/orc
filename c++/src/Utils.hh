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

#ifndef ORC_UTILS_HH
#define ORC_UTILS_HH

#include <stdint.h>
#include <time.h>
#include <sys/time.h>

namespace orc {

static inline uint64_t getCurrentTimeUs() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL +
         static_cast<uint64_t>(ts.tv_nsec) / 1000;
}

class AutoStopwatch {
    uint64_t start;
    volatile uint64_t* latencyUs;
    volatile uint64_t* count;

public:
    AutoStopwatch(volatile uint64_t* latency,
                  volatile uint64_t* cnt) {
        start = getCurrentTimeUs();
        latencyUs = latency;
        count = cnt;
    }

    ~AutoStopwatch() {
        if (latencyUs) {
            uint64_t elapsedTime = getCurrentTimeUs() - start;
             __sync_fetch_and_add(latencyUs, elapsedTime);
        }
        if (count) {
            __sync_fetch_and_add(count, 1);
        }
    }
};

}

#endif
