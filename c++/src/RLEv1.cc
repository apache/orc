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

const int MINIMUM_REPEAT = 3;
const int MAXIMUM_REPEAT = 127 + MINIMUM_REPEAT;

const int BASE_128_MASK = 0x7f;

const int MAX_DELTA = 127;
const int MIN_DELTA = -128;
const int MAX_LITERAL_SIZE = 128;

RleEncoderV1::RleEncoderV1(
                          std::unique_ptr<BufferedOutputStream> outStream,
                          bool hasSigned):
                          outputStream(std::move(outStream)) {
  isSigned = hasSigned;
  literals = new int64_t[MAX_LITERAL_SIZE];
  numLiterals = 0;
  delta = 0;
  repeat = false;
  tailRunLength = 0;
  bufferPosition = 0;
  bufferLength = 0;
  buffer = nullptr;
}

RleEncoderV1::~RleEncoderV1() {
  delete [] literals;
}

void RleEncoderV1::add(const int64_t* data, uint64_t numValues,
                          const char* notNull) {
  for (uint64_t i = 0; i < numValues; ++i) {
    if (!notNull || notNull[i]) {
      write(data[i]);
    }
  }
}

void RleEncoderV1::writeByte(char c) {
  if (bufferPosition == bufferLength) {
    int addedSize = 0;
    if (!outputStream->Next(reinterpret_cast<void **>(&buffer), &addedSize)) {
      throw std::bad_alloc();
    }
    bufferPosition = 0;
    bufferLength = addedSize;
  }
  buffer[bufferPosition++] = c;
}

void RleEncoderV1::writeValues() {
  if (numLiterals != 0) {
    if (repeat) {
      writeByte(static_cast<char>
                (static_cast<uint64_t>(numLiterals) - MINIMUM_REPEAT));
      writeByte(static_cast<char>(delta));
      if (isSigned) {
        writeVslong(literals[0]);
      } else {
        writeVulong(literals[0]);
      }
    } else {
      writeByte(static_cast<char>(-numLiterals));
      for(int i=0; i < numLiterals; ++i) {
        if (isSigned) {
          writeVslong(literals[i]);
        } else {
          writeVulong(literals[i]);
        }
      }
    }
    repeat = false;
    numLiterals = 0;
    tailRunLength = 0;
  }
}

uint64_t RleEncoderV1::flush() {
  writeValues();
  outputStream->BackUp(bufferLength - bufferPosition);
  uint64_t dataSize = outputStream->flush();
  bufferLength = bufferPosition = 0;
  return dataSize;
}

void RleEncoderV1::write(int64_t value) {
  if (numLiterals == 0) {
    literals[numLiterals++] = value;
    tailRunLength = 1;
  } else if (repeat) {
    if (value == literals[0] + delta * numLiterals) {
      numLiterals += 1;
      if (numLiterals == MAXIMUM_REPEAT) {
        writeValues();
      }
    } else {
      writeValues();
      literals[numLiterals++] = value;
      tailRunLength = 1;
    }
  } else {
    if (tailRunLength == 1) {
      delta = value - literals[numLiterals - 1];
      if (delta < MIN_DELTA || delta > MAX_DELTA) {
        tailRunLength = 1;
      } else {
        tailRunLength = 2;
      }
    } else if (value == literals[numLiterals - 1] + delta) {
      tailRunLength += 1;
    } else {
      delta = value - literals[numLiterals - 1];
      if (delta < MIN_DELTA || delta > MAX_DELTA) {
        tailRunLength = 1;
      } else {
        tailRunLength = 2;
      }
    }
    if (tailRunLength == MINIMUM_REPEAT) {
      if (numLiterals + 1 == MINIMUM_REPEAT) {
        repeat = true;
        numLiterals += 1;
      } else {
        numLiterals -= static_cast<int>(MINIMUM_REPEAT - 1);
        long base = literals[numLiterals];
        writeValues();
        literals[0] = base;
        repeat = true;
        numLiterals = MINIMUM_REPEAT;
      }
    } else {
      literals[numLiterals++] = value;
      if (numLiterals == MAX_LITERAL_SIZE) {
        writeValues();
      }
    }
  }
}

void RleEncoderV1::writeVslong(int64_t val) {
  writeVulong((val << 1) ^ (val >> 63));
}

void RleEncoderV1::writeVulong(int64_t val) {
  while (true) {
    if ((val & ~0x7f) == 0) {
      writeByte(static_cast<char>(val));
      return;
    } else {
      writeByte(static_cast<char>(0x80 | (val & 0x7f)));
      // cast val to unsigned so as to force 0-fill right shift
      val = (static_cast<uint64_t>(val) >> 7);
    }
  }
}

void RleEncoderV1::recordPosition(PositionRecorder* recorder) const {
  uint64_t flushedSize = outputStream->getSize();
  uint64_t unflushedSize = static_cast<uint64_t>(bufferPosition);
  if (outputStream->isCompressed()) {
    recorder->add(flushedSize);
    recorder->add(unflushedSize);
  } else {
    flushedSize -= static_cast<uint64_t>(bufferLength);
    recorder->add(flushedSize + unflushedSize);
  }
  recorder->add(static_cast<uint64_t>(numLiterals));
}

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
