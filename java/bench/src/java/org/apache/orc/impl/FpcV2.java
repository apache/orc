/*
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

import org.apache.orc.impl.writer.ChunkStartPosition;

import java.io.IOException;
import java.util.Arrays;

/**
 * The original FPC is described in "High Throughput Compression of
 * Double-Precision Floating-Point Data" by Martin Burtscher and Paruj
 * Ratanaworabhan. Visit http://cs.txstate.edu/~burtscher/research/FPC/
 * for more information.
 *
 * <ul>
 *   <li>This version hard codes the table size to 128 bits.</li>
 *   <li>The values are encoded as &lt;header&gt;&lt;diff1&gt;&lt;diff2&gt;</li>
 *   <li>The header with the first value in the low 4 bits.</li>
 *   <li>Every 32k values, it starts a new chunk and resets the current state.
 *       Since the state is critical for decompressing the stream and it is
 *       rebuilt while decompressing the chunk, we record the byte position of
 *       the start of the chunk and how many values we are in to it.</li>
 *   <li>The index position includes how many values we are into the compression
 *       window.</li>
 * </ul>
 */
public class FpcV2 {
  // The size of the tables for the predictors. This must be a power of two.
  private static final int TABLE_SIZE = 1 << 7;

  // The number of floating point values between resets. This must be even.
  private static final int CHUNK_SIZE = 32 * 1024;

  public static final class FpcCompressor {

    private final DfcmPredictor dfcmPredictor;
    private final FcmPredictor fcmPredictor;
    private final PositionedOutputStream output;
    private final ChunkStartPosition chunkPosition = new ChunkStartPosition();

    // the number of values in the current chunk
    private int numValues;

    // buffer for two doubles:
    // 1 header byte
    // 2 diffs (up to 8 bytes each)
    private byte[] bytes = new byte[17];
    // position in the buffer
    private int position;

    public FpcCompressor(PositionedOutputStream output) throws IOException {
      this.output = output;
      this.dfcmPredictor = new DfcmPredictor(TABLE_SIZE);
      this.fcmPredictor = new FcmPredictor(TABLE_SIZE);
      reset();
    }

    private void reset() throws IOException {
      dfcmPredictor.clear();
      fcmPredictor.clear();
      position = 1;
      numValues = 0;
      // capture the position in the stream at the start of the chunk
      chunkPosition.reset();
      output.getPosition(chunkPosition);
    }

    private void writeDiff(long diff, int encodedBytes) throws IOException {
      switch(encodedBytes) {
        case 7: // 8 bytes
          bytes[position++] = (byte) (diff >> 56);
        case 6: // 7 bytes
          bytes[position++] = (byte) (diff >> 48);
        case 5: // 6 bytes
          bytes[position++] = (byte) (diff >> 40);
        case 4: // 5 bytes
          bytes[position++] = (byte) (diff >> 32);
        case 3: // 4, 3 bytes
          bytes[position++] = (byte) (diff >> 24);
          bytes[position++] = (byte) (diff >> 16);
        case 2: // 2 bytes
          bytes[position++] = (byte) (diff >> 8);
        case 1: // 1 byte
          bytes[position++] = (byte) diff;
        case 0: // 0 byte
          break;
        default:
          throw new IllegalArgumentException();
      }
    }

    /**
     * Count the number of leading bytes that are zero.
     * @param x the 32 bit value to check
     * @return 0 to 4 bytes
     */
    private static int leadingZeroBytes32(int x) {
      if ((x & (-1 << 16)) == 0) {
        if ((x & (-1 << 8)) == 0) {
          return x == 0 ? 4 : 3;
        } else {
          return 2;
        }
      } else {
        return (x & (-1 << 24)) == 0 ? 1 : 0;
      }
    }

    /**
     * Count the number of leading bytes that are zero.
     * @param x the 64 bit value to check
     * @return 0 to 8 bytes
     */
    private static int leadingZeroBytes(long x) {
      if ((x & (-1L << 32)) == 0) {
        return 4 + leadingZeroBytes32((int) x);
      } else {
        return leadingZeroBytes32((int) (x >> 32));
      }
    }

    private static int encodeBytes(int bytes) {
      return bytes > 3 ? bytes - 1 : bytes;
    }

    public void compress(double[] literals, int offset,
                         int length) throws IOException {
      for(int ix = 0; ix < length; ix++) {
        // convert floating to long bits
        long bits = Double.doubleToLongBits(literals[ix + offset]);

        // DFCM prediction
        final long dfcmPredicted = dfcmPredictor.predictNext();
        dfcmPredictor.update(bits);
        final long dfcmDiff = bits ^ dfcmPredicted;

        // FCM prediction
        final long fcmPredicted = fcmPredictor.predictNext();
        fcmPredictor.update(bits);
        final long fcmDiff = bits ^ fcmPredicted;

        // decide a prediction between FCM and DFCM then encode its XOR
        // difference with the original bits
        int dfcmZeros = leadingZeroBytes(dfcmDiff);
        int fcmZeros = leadingZeroBytes(fcmDiff);
        int header;
        if (dfcmZeros > fcmZeros) {
          int encoded = encodeBytes(8 - dfcmZeros);
          header = 8 | encoded;
          writeDiff(dfcmDiff, encoded);
        } else {
          int encoded = encodeBytes(8 - fcmZeros);
          header = encoded;
          writeDiff(fcmDiff, encoded);
        }
        if (numValues++ % 2 == 0) {
          bytes[0] = (byte) header;
        } else {
          bytes[0] |= (header << 4);
          output.write(bytes, 0, position);
          position = 1;
        }
        if (numValues == CHUNK_SIZE) {
          reset();
        }
      }
    }

    public void flush() throws IOException {
      if (numValues % 2 != 0) {
        output.write(bytes, 0, position);
      }
      output.flush();
      reset();
    }

    void recordPosition(PositionRecorder recorder) throws IOException {
      chunkPosition.playbackPosition(recorder);
      recorder.addPosition(numValues);
    }
  }

  public static final class FpcExtractor {
    private static final int MAX_SERIALIZED_SIZE = 17;
    private final InStream input;
    private final FcmPredictor fcmPredictor = new FcmPredictor(TABLE_SIZE);
    private final DfcmPredictor dfcmPredictor = new DfcmPredictor(TABLE_SIZE);
    private final byte[] bytes = new byte[64 * 1024];
    private int position = 0;
    private int limit = 0;
    int numLiterals = 0;
    byte flag;

    void fetchData() throws IOException {
      int oldBytes = limit - position;
      for (int i = 0; i < oldBytes && i < bytes.length; ++i) {
        bytes[i] = bytes[i + position];
      }
      limit = input.read(bytes, oldBytes, bytes.length - oldBytes) + oldBytes;
      position = 0;
    }

    public FpcExtractor(InStream input) throws IOException {
      this.input = input;
    }

    public void extract(double[] values, int offset,
                        int length) throws IOException {
      for(int i=0; i < length; ++i) {
        if (numLiterals % 2 == 0) {
          if (limit - position < MAX_SERIALIZED_SIZE) {
            fetchData();
          }
          flag = bytes[position++];
        } else {
          flag >>= 4;
        }
        values[i + offset] = decodeValue(flag);
        if (++numLiterals == CHUNK_SIZE) {
          reset();
        }
      }
    }

    private void reset() {
      fcmPredictor.clear();
      dfcmPredictor.clear();
      numLiterals = 0;
    }

    private double decodeValue(byte flags) throws IOException {
      long bits;
      int nbytes = flags & 7;
      long xorDiff = readBytes(nbytes);
      if ((flags & 8) == 0) {
        bits = fcmPredictor.predictNext() ^ xorDiff;
      } else {
        bits = dfcmPredictor.predictNext() ^ xorDiff;
      }
      fcmPredictor.update(bits);
      dfcmPredictor.update(bits);
      return Double.longBitsToDouble(bits);
    }

    private long readBytes(int nbytes) throws IOException {
      long value = 0;
      switch (nbytes) {
        case 7: // 8 bytes
          value = (bytes[position++] & 0xFFL) << 56;
        case 6: // 7 bytes
          value |= (bytes[position++] & 0xFFL) << 48;
        case 5: // 6 bytes
          value |= (bytes[position++] & 0xFFL) << 40;
        case 4: // 5 bytes
          value |= (bytes[position++] & 0xFFL) << 32;
        case 3: // 4, 3 bytes
          value |= (bytes[position++] & 0xFFL) << 24;
          value |= (bytes[position++] & 0xFFL) << 16;
        case 2: // 2 bytes
          value |= (bytes[position++] & 0xFFL) << 8;
        case 1: // 1 bytes
          value |= (bytes[position++] & 0xFFL);
        case 0: // 0 bytes
          break;
        default:
          throw new IllegalArgumentException();
      }
      return value;
    }

    public void seek(PositionProvider provider) throws IOException {
      reset();
      input.seek(provider);
      skip(provider.getNext());
    }

    /**
     * Skip values without returning them.
     * @param count the number of doubles to skip
     * @throws IOException
     */
    public void skip(long count) throws IOException {
      for(int i=0; i < count; ++i) {
        if (numLiterals % 2 == 0) {
          if (limit - position < MAX_SERIALIZED_SIZE) {
            fetchData();
          }
          flag = bytes[position++];
        } else {
          flag >>= 4;
        }
        decodeValue(flag);
        if (++numLiterals == CHUNK_SIZE) {
          reset();
        }
      }
    }
  }

  /**
   * Ported from Akumuli FcmPredictor.
   * Visit http://akumuli.org for more information.
   */
  static final class FcmPredictor {
    private final long[] table;
    private final int mask;
    private int lastHash;

    FcmPredictor(int tableSize) {
      lastHash = 0;
      mask = tableSize - 1;
      assert((tableSize & mask) == 0);
      table = new long[tableSize];
    }

    public long predictNext() {
      return table[lastHash];
    }

    public void update(long value) {
      table[lastHash] = value;
      lastHash = (int) (((lastHash << 6) ^ (value >>> 48)) & mask);
    }

    public void clear() {
      Arrays.fill(table, 0);
      lastHash = 0;
    }
  }

  /**
   * Ported from Akumuli DfcmPredictor.
   * Visit http://akumuli.org for more information.
   */
  static final class DfcmPredictor {
    private final long[] table;
    private final int mask;
    private int lastHash;
    private long lastValue;

    DfcmPredictor(int tableSize) {
      lastHash = 0;
      lastValue = 0;
      mask = tableSize - 1;
      assert((tableSize & mask) == 0);
      table = new long[tableSize];
    }

    public long predictNext() {
      return table[lastHash] + lastValue;
    }

    public void update(long value) {
      table[lastHash] = value - lastValue;
      lastHash = (int) (((lastHash << 2) ^ ((value - lastValue) >>> 40))
          & mask);
      lastValue = value;
    }

    public void clear() {
      Arrays.fill(table, 0);
      lastHash = 0;
      lastValue = 0;
    }
  }

}
