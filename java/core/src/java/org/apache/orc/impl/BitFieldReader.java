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
package org.apache.orc.impl;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

public final class BitFieldReader {
  private final RunLengthByteReader input;
  /** The number of bits in one item. Non-test code always uses 1. */
  private int current;
  private byte currentIdx;

  public BitFieldReader(InStream input) {
    this.input = new RunLengthByteReader(input);
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      currentIdx = 0;
    } else {
      throw new EOFException("Read past end of bit field from " + this);
    }
  }

  public int next() throws IOException {
    // mod 8
    if ((currentIdx & 7) == 0) {
      readByte();
    }

    currentIdx++;
    // Highest bit is the first val
    return ((current >>> (8 - currentIdx)) & 1);
  }

  public void nextVector(LongColumnVector previous,
                         long previousLen) throws IOException {
    previous.isRepeating = true;
    for (int i = 0; i < previousLen; i++) {
      if (previous.noNulls || !previous.isNull[i]) {
        previous.vector[i] = next();
      } else {
        // The default value of null for int types in vectorized
        // processing is 1, so set that if the value is null
        previous.vector[i] = 1;
      }

      // The default value for nulls in Vectorization for int types is 1
      // and given that non null value can also be 1, we need to check for isNull also
      // when determining the isRepeating flag.
      if (previous.isRepeating
          && i > 0
          && ((previous.vector[0] != previous.vector[i]) ||
          (previous.isNull[0] != previous.isNull[i]))) {
        previous.isRepeating = false;
      }
    }
  }

  public void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed > 8) {
      throw new IllegalArgumentException("Seek past end of byte at " +
          consumed + " in " + input);
    } else if (consumed != 0) {
      readByte();
      currentIdx = (byte) consumed;
    } else {
      currentIdx = 0;
    }
  }

  public void skip(long totalBits) throws IOException {
    final int availableBits = 8 - currentIdx;
    if (totalBits <= availableBits) {
      currentIdx += totalBits;
    } else {
      final long bitsToSkip = (totalBits - availableBits);
      input.skip(bitsToSkip / 8);
      current = input.next();
      currentIdx = (byte) (bitsToSkip % 8);
    }
  }

  @Override
  public String toString() {
    return "bit reader current: " + current
        + " current bit index: " + currentIdx
        + " from " + input;
  }
}
