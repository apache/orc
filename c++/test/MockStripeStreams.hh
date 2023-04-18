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

#ifndef ORC_MOCKSTRIPESTREAM_HH
#define ORC_MOCKSTRIPESTREAM_HH

#include "ColumnReader.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {
  class MockStripeStreams : public StripeStreams {
   public:
    ~MockStripeStreams() override {}

    std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId, proto::Stream_Kind kind,
                                                   bool stream) const override;

    MOCK_CONST_METHOD0(getSelectedColumns, const std::vector<bool>());
    MOCK_CONST_METHOD1(getEncoding, proto::ColumnEncoding(uint64_t));
    MOCK_CONST_METHOD3(getStreamProxy, SeekableInputStream*(uint64_t, proto::Stream_Kind, bool));
    MOCK_CONST_METHOD0(getErrorStream, std::ostream*());
    MOCK_CONST_METHOD0(getThrowOnHive11DecimalOverflow, bool());
    MOCK_CONST_METHOD0(getForcedScaleOnHive11Decimal, int32_t());
    MOCK_CONST_METHOD0(isDecimalAsLong, bool());
    MOCK_CONST_METHOD0(getSchemaEvolution, const SchemaEvolution*());

    MemoryPool& getMemoryPool() const override;

    ReaderMetrics* getReaderMetrics() const override;

    const Timezone& getWriterTimezone() const override;

    const Timezone& getReaderTimezone() const override;
  };

}  // namespace orc

#endif
