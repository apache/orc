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

#include <atomic>
#include <chrono>

namespace orc {

class AutoStopwatch {
    std::chrono::high_resolution_clock::time_point start;
    std::atomic<uint64_t>* latencyUs;
    std::atomic<uint64_t>* count;

public:
    AutoStopwatch(std::atomic<uint64_t>* _latencyUs,
                  std::atomic<uint64_t>* _count)
                  : latencyUs(_latencyUs),
                    count(_count) {
        if (latencyUs) {
            start = std::chrono::high_resolution_clock::now();
        }
    }

    ~AutoStopwatch() {
        if (latencyUs) {
            std::chrono::microseconds elapsedTime =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start);
            latencyUs->fetch_add(elapsedTime.count());
        }

        if (count) {
            count->fetch_add(1);
        }
    }
};

}

#endif
