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

#ifndef ORC_BYTE_RLE_HH
#define ORC_BYTE_RLE_HH

#include <memory>

#include "Compression.hh"

namespace orc {

  class ByteRleDecoder {
  public:
    virtual ~ByteRleDecoder();

    /**
     * Seek to a particular spot.
     */
    virtual void seek(PositionProvider&) = 0;

    /**
     * Seek over a given number of values.
     */
    virtual void skip(uint64_t numValues) = 0;

    /**
     * Read a number of values into the batch.
     * @param data the array to read into
     * @param numValues the number of values to read
     * @param notNull If the pointer is null, all values are read. If the
     *    pointer is not null, positions that are false are skipped.
     */
    virtual void next(char* data, uint64_t numValues, char* notNull) = 0;
  };

  /**
   * Create a byte RLE decoder.
   * @param input the input stream to read from
   */
  std::unique_ptr<ByteRleDecoder> createByteRleDecoder
                                 (std::unique_ptr<SeekableInputStream> input);

  /**
   * Create a boolean RLE decoder.
   *
   * Unlike the other RLE decoders, the boolean decoder sets the data to 0
   * if the value is masked by notNull. This is required for the notNull stream
   * processing to properly apply multiple masks from nested types.
   * @param input the input stream to read from
   */
  std::unique_ptr<ByteRleDecoder> createBooleanRleDecoder
                                 (std::unique_ptr<SeekableInputStream> input);
}

#endif
