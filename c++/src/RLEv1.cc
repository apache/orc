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

#include "Adaptor.hh"
#include "Compression.hh"
#include "Exceptions.hh"
#include "RLEv1.hh"

#include <algorithm>

namespace orc {

const uint64_t MINIMUM_REPEAT = 3;
const uint64_t BASE_128_MASK = 0x7f;

signed char RleDecoderV1::readByte() {
  if (bufferStart == bufferEnd) {
    int bufferLength;
    const void* bufferPointer;
    if (!inputStream->Next(&bufferPointer, &bufferLength)) {
      throw ParseError("bad read in readByte");
    }
    bufferStart = static_cast<const char*>(bufferPointer);
    bufferEnd = bufferStart + bufferLength;
  }
  return *(bufferStart++);
}

uint64_t RleDecoderV1::readLong() {
  uint64_t result = 0;
  int64_t offset = 0;
  signed char ch = readByte();
  if (ch >= 0) {
    result = static_cast<uint64_t>(ch);
  } else {
    result = static_cast<uint64_t>(ch) & BASE_128_MASK;
    while ((ch = readByte()) < 0) {
      offset += 7;
      result |= (static_cast<uint64_t>(ch) & BASE_128_MASK) << offset;
    }
    result |= static_cast<uint64_t>(ch) << (offset + 7);
  }
  return result;
}

void RleDecoderV1::skipLongs(uint64_t numValues) {
  while (numValues > 0) {
    if (readByte() >= 0) {
      --numValues;
    }
  }
}

void RleDecoderV1::readHeader() {
  signed char ch = readByte();
  if (ch < 0) {
    remainingValues = static_cast<uint64_t>(-ch);
    repeating = false;
  } else {
    remainingValues = static_cast<uint64_t>(ch) + MINIMUM_REPEAT;
    repeating = true;
    delta = readByte();
    value = isSigned
        ? unZigZag(readLong())
        : static_cast<int64_t>(readLong());
  }
}

RleDecoderV1::RleDecoderV1(std::unique_ptr<SeekableInputStream> input,
                           bool hasSigned)
    : inputStream(std::move(input)),
      isSigned(hasSigned),
      remainingValues(0),
      value(0),
      bufferStart(nullptr),
      bufferEnd(bufferStart),
      delta(0),
      repeating(false) {
}

void RleDecoderV1::seek(PositionProvider& location) {
  // move the input stream
  inputStream->seek(location);
  // force a re-read from the stream
  bufferEnd = bufferStart;
  // read a new header
  readHeader();
  // skip ahead the given number of records
  skip(location.next());
}

void RleDecoderV1::skip(uint64_t numValues) {
  while (numValues > 0) {
    if (remainingValues == 0) {
      readHeader();
    }
    uint64_t count = std::min(numValues, remainingValues);
    remainingValues -= count;
    numValues -= count;
    if (repeating) {
      value += delta * static_cast<int64_t>(count);
    } else {
      skipLongs(count);
    }
  }
}

void RleDecoderV1::next(int64_t* const data,
                        const uint64_t numValues,
                        const char* const notNull) {
  uint64_t position = 0;
  // skipNulls()
  if (notNull) {
    // Skip over null values.
    while (position < numValues && !notNull[position]) {
      ++position;
    }
  }
  while (position < numValues) {
    // If we are out of values, read more.
    if (remainingValues == 0) {
      readHeader();
    }
    // How many do we read out of this block?
    uint64_t count = std::min(numValues - position, remainingValues);
    uint64_t consumed = 0;
    if (repeating) {
      if (notNull) {
        for (uint64_t i = 0; i < count; ++i) {
          if (notNull[position + i]) {
            data[position + i] = value + static_cast<int64_t>(consumed) * delta;
            consumed += 1;
          }
        }
      } else {
        for (uint64_t i = 0; i < count; ++i) {
          data[position + i] = value + static_cast<int64_t>(i) * delta;
        }
        consumed = count;
      }
      value += static_cast<int64_t>(consumed) * delta;
    } else {
      if (notNull) {
        for (uint64_t i = 0 ; i < count; ++i) {
          if (notNull[position + i]) {
            data[position + i] = isSigned
                ? unZigZag(readLong())
                : static_cast<int64_t>(readLong());
            ++consumed;
          }
        }
      } else {
        if (isSigned) {
          for (uint64_t i = 0; i < count; ++i) {
            data[position + i] = unZigZag(readLong());
          }
        } else {
          for (uint64_t i = 0; i < count; ++i) {
            data[position + i] = static_cast<int64_t>(readLong());
          }
        }
        consumed = count;
      }
    }
    remainingValues -= consumed;
    position += count;

    // skipNulls()
    if (notNull) {
      // Skip over null values.
      while (position < numValues && !notNull[position]) {
        ++position;
      }
    }
  }
}

}  // namespace orc
