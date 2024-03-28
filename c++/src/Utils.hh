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
    std::chrono::high_resolution_clock::time_point start_;
    std::atomic<uint64_t>* latencyUs_;
    std::atomic<uint64_t>* count_;
    bool minus_;

   public:
    AutoStopwatch(std::atomic<uint64_t>* latencyUs, std::atomic<uint64_t>* count,
                  bool minus = false)
        : latencyUs_(latencyUs), count_(count), minus_(minus) {
      if (latencyUs_) {
        start_ = std::chrono::high_resolution_clock::now();
      }
    }

    ~AutoStopwatch() {
      if (latencyUs_) {
        std::chrono::microseconds elapsedTime =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - start_);
        if (!minus_) {
          latencyUs_->fetch_add(static_cast<uint64_t>(elapsedTime.count()));
        } else {
          latencyUs_->fetch_sub(static_cast<uint64_t>(elapsedTime.count()));
        }
      }

      if (count_) {
        count_->fetch_add(1);
      }
    }
  };

#if ENABLE_METRICS
#define SCOPED_STOPWATCH(METRICS_PTR, LATENCY_VAR, COUNT_VAR)                           \
  AutoStopwatch measure((METRICS_PTR == nullptr ? nullptr : &METRICS_PTR->LATENCY_VAR), \
                        (METRICS_PTR == nullptr ? nullptr : &METRICS_PTR->COUNT_VAR))

#define SCOPED_MINUS_STOPWATCH(METRICS_PTR, LATENCY_VAR)                                         \
  AutoStopwatch measure((METRICS_PTR == nullptr ? nullptr : &METRICS_PTR->LATENCY_VAR), nullptr, \
                        true)
#else
#define SCOPED_STOPWATCH(METRICS_PTR, LATENCY_VAR, COUNT_VAR)
#define SCOPED_MINUS_STOPWATCH(METRICS_PTR, LATENCY_VAR)
#endif

}  // namespace orc

#endif
