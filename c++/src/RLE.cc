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

#include "RLEv1.hh"
#include "RLEv2.hh"
#include "Exceptions.hh"

namespace orc {

  RleDecoder::~RleDecoder() {
    // PASS
  }

  std::unique_ptr<RleDecoder> createRleDecoder
                         (std::unique_ptr<SeekableInputStream> input,
                          bool isSigned,
                          RleVersion version,
                          MemoryPool& pool) {
    switch (static_cast<int64_t>(version)) {
    case RleVersion_1:
      // We don't have std::make_unique() yet.
      return std::unique_ptr<RleDecoder>(new RleDecoderV1(std::move(input),
                                                          isSigned));
    case RleVersion_2:
      return std::unique_ptr<RleDecoder>(new RleDecoderV2(std::move(input),
                                                          isSigned, pool));
    default:
      throw NotImplementedYet("Not implemented yet");
    }
  }

}  // namespace orc
