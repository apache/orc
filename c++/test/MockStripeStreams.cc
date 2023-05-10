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

#include "MockStripeStreams.hh"

namespace orc {
  MemoryPool& MockStripeStreams::getMemoryPool() const {
    return *getDefaultPool();
  }

  ReaderMetrics* MockStripeStreams::getReaderMetrics() const {
    return getDefaultReaderMetrics();
  }

  const Timezone& MockStripeStreams::getWriterTimezone() const {
    return getTimezoneByName("America/Los_Angeles");
  }

  const Timezone& MockStripeStreams::getReaderTimezone() const {
    return getTimezoneByName("GMT");
  }

  std::unique_ptr<SeekableInputStream> MockStripeStreams::getStream(uint64_t columnId,
                                                                    proto::Stream_Kind kind,
                                                                    bool stream) const {
    return std::unique_ptr<SeekableInputStream>(getStreamProxy(columnId, kind, stream));
  }

}  // namespace orc
