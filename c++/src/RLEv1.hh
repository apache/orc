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

#ifndef ORC_RLEV1_HH
#define ORC_RLEV1_HH

#include "Adaptor.hh"
#include "RLE.hh"

#include <memory>

namespace orc {

class RleEncoderV1 : public RleEncoder {
public:
    RleEncoderV1(std::unique_ptr<BufferedOutputStream> outStream,
                 bool hasSigned);
    ~RleEncoderV1() override;

    /**
     * Encode the next batch of values.
     * @param data the array to be written
     * @param numValues the number of values to write
     * @param notNull If the pointer is null, all values are writen. If the
     *    pointer is not null, positions that are false are skipped.
     */
    void add(const int64_t* data, uint64_t numValues,
             const char* notNull) override;

    /**
     * Get size of buffer used so far.
     */
    uint64_t getBufferSize() const override {
        return outputStream->getSize();
    }

    /**
     * Flushing underlying BufferedOutputStream
     */
    uint64_t flush() override;

    /**
     * record current position
     * @param recorder use the recorder to record current positions
     */
    virtual void recordPosition(PositionRecorder* recorder) const override;

private:
    std::unique_ptr<BufferedOutputStream> outputStream;
    bool isSigned;
    int64_t* literals;
    uint64_t numLiterals;
    int64_t delta;
    bool repeat;
    uint64_t tailRunLength;
    int bufferPosition;
    int bufferLength;
    char* buffer;

    void write(int64_t val);
    void writeByte(char c);
    void writeVulong(int64_t val);
    void writeVslong(int64_t val);
    void writeValues();
};

class RleDecoderV1 : public RleDecoder {
public:
    RleDecoderV1(std::unique_ptr<SeekableInputStream> input,
                 bool isSigned);

    /**
    * Seek to a particular spot.
    */
    void seek(PositionProvider&) override;

    /**
    * Seek over a given number of values.
    */
    void skip(uint64_t numValues) override;

    /**
    * Read a number of values into the batch.
    */
    void next(int64_t* data, uint64_t numValues,
              const char* notNull) override;

private:
    inline signed char readByte();

    inline void readHeader();

    inline uint64_t readLong();

    inline void skipLongs(uint64_t numValues);

    const std::unique_ptr<SeekableInputStream> inputStream;
    const bool isSigned;
    uint64_t remainingValues;
    int64_t value;
    const char* bufferStart;
    const char* bufferEnd;
    int64_t delta;
    bool repeating;
};
}  // namespace orc

#endif  // ORC_RLEV1_HH
