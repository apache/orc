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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class FpcV1 {
  /**
   * A modified version of FPC.
   *
   * The original FPC is described in "High Throughput Compression of
   * Double-Precision Floating-Point Data" by Martin Burtscher and Paruj
   * Ratanaworabhan. Visit http://cs.txstate.edu/~burtscher/research/FPC/
   * for more information.
   *
   * This modified FPC has two main differences. The first difference is that it
   * tries to multiply values by one thousand when it doesn't loss any precision.
   * The second difference is that it truncates trailing zeros, while the
   * original FPC truncates leading zeros. They are mainly reasonable for real
   * world data sets and standard benchmark data sets, such as TPC-DS.
   *
   * A data set usually has a limited number of digits in its fractional part.
   * For example, world's most traded ten currencies(USD, EUR, JPY, GBP, AUD,
   * CAD, CHF, CNY, SEK, NZD) have fractional units, which are cents of basic
   * units.
   *
   * A small integer value has many trailing zeros in the IEEE 754
   * floating-precision binary floating-point format. The original FPC truncates
   * leading zeros of XOR bits. Truncating trailing zeros makes compression ratio
   * higher.
   * @see FpcExtractor
   */
  public static class FpcCompressor {
    private static int PREDICTOR_N = 1 << 7;
    private static int EXPONENT = 3;
    private static double POWER_OF_TEN = Math.pow(10, EXPONENT);
    private static double MAX = Math.pow(10, 13 - EXPONENT);
    private static double MIN = -MAX;

    private final int tableSize;
    private final Predictor dfcmPredictor;
    private final Predictor fcmPredictor;
    private final OutputStream output;
    private byte[] bytes;
    private int position;

    public FpcCompressor(OutputStream output) throws IOException {
      this(PREDICTOR_N, output);
    }

    private FpcCompressor(int tableSize, OutputStream output) throws
        IOException {
      this.tableSize = tableSize;
      this.output = output;
      this.dfcmPredictor = new DfcmPredictor(tableSize);
      this.fcmPredictor = new FcmPredictor(tableSize);
    }

    private void encodeValue(long diff, byte flag) throws IOException {
      final int nbytes = (flag & 7);
      final int nshift = (64 - (nbytes >= 3 ? nbytes + 1 : nbytes) * 8);
      diff >>>= nshift;
      switch(nbytes) {
        case 7: // 8 bytes
          bytes[position++] = (byte) (diff & 0xFF);
          diff >>>= 8;
        case 6: // 7 bytes
          bytes[position++] = (byte) (diff & 0xFF);
          diff >>>= 8;
        case 5: // 6 bytes
          bytes[position++] = (byte) (diff & 0xFF);
          diff >>>= 8;
        case 4: // 5 bytes
          bytes[position++] = (byte) (diff & 0xFF);
          diff >>>= 8;
        case 3: // 4, 3 bytes
          bytes[position++] = (byte) (diff & 0xFF);
          diff >>>= 8;
          bytes[position++] = (byte) (diff & 0xFF);
          diff >>>= 8;
        case 2: // 2 bytes
          bytes[position++] = (byte) (diff & 0xFF);
          diff >>>= 8;
        case 1: // 1 byte
          bytes[position++] = (byte) (diff & 0xFF);
        case 0: // 0 byte
          break;
        default:
          throw new IllegalArgumentException();
      }
    }

    private boolean isValidAfterMulDiv(double[] literals, int numLiterals) {
      for (int i = 0; i < numLiterals; i++) {
        final double d = literals[i];
        if (d < MIN || d > MAX) {
          return false;
        }
      }
      for (int i = 0; i < numLiterals; i++) {
        final double d = literals[i];
        if (Double.doubleToLongBits((d * POWER_OF_TEN) / POWER_OF_TEN) !=
            Double.doubleToLongBits(d)) {
          return false;
        }
      }
      return true;
    }

    public void compress(double[] literals, int numLiterals) throws IOException {
      // clear
      long prevDiff = 0;
      byte prevFlag = 0;
      dfcmPredictor.clear();
      fcmPredictor.clear();

      // write table size
      final int tableSizeTrailingZeros = Long.numberOfTrailingZeros(tableSize);
      output.write(tableSizeTrailingZeros);

      // expect the number of required bytes
      final int expectedBytes = (numLiterals / 2) + (numLiterals * 8);
      if (bytes == null || expectedBytes > bytes.length) {
        bytes = new byte[expectedBytes * 2];
      } else {
        Arrays.fill(bytes, (byte) 0);
      }

      // write an exponent
      final boolean isValidAfterMulDiv = isValidAfterMulDiv(literals,
          numLiterals);
      if (isValidAfterMulDiv) {
        output.write(EXPONENT);
      } else {
        output.write(0);
      }

      position = numLiterals / 2;
      for (int ix = 0; ix != numLiterals; ix++) {
        // convert floating to long bits
        long bits;
        if (isValidAfterMulDiv) {
          bits = Double.doubleToLongBits(literals[ix] * POWER_OF_TEN);
        } else {
          bits = Double.doubleToLongBits(literals[ix]);
        }

        // DFCM prediction
        final long dfcmPredicted = dfcmPredictor.predictNext();
        dfcmPredictor.update(bits);
        final long dfcmDiff = bits ^ dfcmPredicted;
        final int dfcmTrailingZeros;
        if (dfcmDiff == 0) {
          dfcmTrailingZeros = 64;
        } else {
          dfcmTrailingZeros = Long.numberOfTrailingZeros(dfcmDiff);
        }

        // FCM prediction
        final long fcmPredicted = fcmPredictor.predictNext();
        fcmPredictor.update(bits);
        final long fcmDiff = bits ^ fcmPredicted;
        final int fcmTrailingZeros;
        if (fcmDiff == 0) {
          fcmTrailingZeros = 64;
        } else {
          fcmTrailingZeros = Long.numberOfTrailingZeros(fcmDiff);
        }

        // decide a prediction between FCM and DFCM then encode its XOR
        // difference with the original bits
        int nbytes;
        final byte flag;
        final long diff;
        if (fcmTrailingZeros >= dfcmTrailingZeros) {
          diff = fcmDiff;
          nbytes = 8 - (fcmTrailingZeros / 8);
          if (nbytes > 3) {
            nbytes--;
          }
          // zeroed 4th bit indicates FCM
          flag = (byte) (nbytes & 7);
        } else {
          diff = dfcmDiff;
          nbytes = 8 - (dfcmTrailingZeros / 8);
          if (nbytes > 3) {
            nbytes--;
          }
          // 4th bit indicates DFCM
          flag = (byte) (8 | (nbytes & 7));
        }

        if (ix % 2 == 0) {
          prevDiff = diff;
          prevFlag = flag;
        } else {
          // we're storing values by pairs to save space
          byte flags = (byte) ((prevFlag << 4) | flag);
          bytes[ix >>> 1] = flags;
          encodeValue(prevDiff, prevFlag);
          encodeValue(diff, flag);
        }
      }
      if (numLiterals % 2 != 0) {
        // `input` contains odd number of values so we should use
        // empty second value that will take one byte in output
        byte flags = (byte) (prevFlag << 4);
        bytes[numLiterals >>> 1] = flags;
        encodeValue(prevDiff, prevFlag);
        encodeValue(0L, (byte) 0);
      }

      // write the number of doubles
      writeThreeBytes(numLiterals);

      // write the number of bytes
      final int numberOfBytes = position;
      writeThreeBytes(numberOfBytes);

      // write the byte buffer
      output.write(bytes, 0, numberOfBytes);
    }

    public void flush() throws IOException {
      output.flush();
    }

    private void writeThreeBytes(int value) throws IOException {
      output.write((byte) (value & 0xFF));
      value >>>= 8;
      output.write((byte) (value & 0xFF));
      value >>>= 8;
      output.write((byte) (value & 0xFF));
    }

    interface Predictor {
      void update(long bits);
      long predictNext();
      void clear();
    }

    /**
     * Ported from Akumuli FcmPredictor.
     * Visit http://akumuli.org for more information.
     */
    static class FcmPredictor implements Predictor {
      private final long[] table;
      private final int mask;
      private int lastHash;

      FcmPredictor(int tableSize) {
        lastHash = 0;
        mask = tableSize - 1;
        assert((tableSize & mask) == 0);
        table = new long[tableSize];
      }

      @Override
      public long predictNext() {
        return table[lastHash];
      }

      @Override
      public void update(long value) {
        table[lastHash] = value;
        lastHash = (int) (((lastHash << 6) ^ (value >>> 48)) & mask);
      }

      @Override
      public void clear() {
        Arrays.fill(table, 0);
        lastHash = 0;
      }
    }

    /**
     * Ported from Akumuli DfcmPredictor.
     * Visit http://akumuli.org for more information.
     */
    static class DfcmPredictor implements Predictor {
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

      @Override
      public long predictNext() {
        return table[lastHash] + lastValue;
      }

      @Override
      public void update(long value) {
        table[lastHash] = value - lastValue;
        lastHash = (int) (((lastHash << 2) ^ ((value - lastValue) >>> 40))
            & mask);
        lastValue = value;
      }

      @Override
      public void clear() {
        Arrays.fill(table, 0);
        lastHash = 0;
        lastValue = 0;
      }
    }
  }

  public static class FpcExtractor {
    private final InputStream input;
    private FpcCompressor.FcmPredictor fcmPredictor;
    private FpcCompressor.DfcmPredictor dfcmPredictor;
    private double[] literals;
    private byte[] bytes;
    private int tableSize;
    private int position;
    int numLiterals;

    public FpcExtractor(InputStream input) throws IOException {
      this.input = input;
    }

    public double[] extract() throws IOException {
      // ensure table size
      final int newTableSize = (1 << input.read());
      if (tableSize != newTableSize) {
        tableSize = newTableSize;
        fcmPredictor = new FpcCompressor.FcmPredictor(tableSize);
        dfcmPredictor = new FpcCompressor.DfcmPredictor(tableSize);
      } else {
        fcmPredictor.clear();
        dfcmPredictor.clear();
      }

      // read an exponent
      final int exponent = input.read();
      final double powerOfTen = Math.pow(10, exponent);

      // read the number of doubles and the number of bytes
      numLiterals = readThreeBytes();
      final int numberOfBytes = readThreeBytes();

      // ensure buffer sizes
      if (literals == null) {
        literals = new double[numLiterals];
      } else if (literals.length < numLiterals) {
        literals = new double[numLiterals * 2];
      }

      if (bytes == null) {
        bytes = new byte[numberOfBytes];
      } else if (bytes.length < numberOfBytes) {
        bytes = new byte[numberOfBytes * 2];
      }

      int remainingBytes = numberOfBytes;
      while (remainingBytes > 0) {
        final int readBytes = input.read(bytes,
            numberOfBytes - remainingBytes, remainingBytes);
        if (readBytes == -1) {
          throw new IOException("Cannot read");
        }
        remainingBytes -= readBytes;
      }

      // read floating values
      final int numLiterals_2 = numLiterals / 2;
      position = numLiterals_2;
      for (int i = 0; i < numLiterals_2; i++) {
        final byte flagPair = bytes[i];
        final byte firstFlags = (byte) (flagPair >> 4);
        final byte secondFlags = (byte) (flagPair & 0xF);
        final double firstValue = decodeValue(firstFlags) / powerOfTen;
        literals[i * 2] = firstValue;
        final double secondValue = decodeValue(secondFlags) / powerOfTen;
        literals[i * 2 + 1] = secondValue;
      }

      return literals;
    }

    private double decodeValue(byte flags) throws IOException {
      final long bits;
      final long fcmPredicted = fcmPredictor.predictNext();
      final long dfcmPredicted = dfcmPredictor.predictNext();
      int nbytes = flags & 7;
      long xorDiff = readBytes(nbytes);
      if ((flags >>> 3) == 0) {
        // FCM
        bits = xorDiff ^ fcmPredicted;
      } else {
        // DFCM
        bits = xorDiff ^ dfcmPredicted;
      }
      fcmPredictor.update(bits);
      dfcmPredictor.update(bits);
      return Double.longBitsToDouble(bits);
    }

    private long readBytes(int nbytes) throws IOException {
      long value = 0;
      switch (nbytes) {
        case 7: // 8 bytes
          value |= (bytes[position++] & 0xFFL);
        case 6: // 7 bytes
          value |= (bytes[position++] & 0xFFL) << 8;
        case 5: // 6 bytes
          value |= (bytes[position++] & 0xFFL) << 16;
        case 4: // 5 bytes
          value |= (bytes[position++] & 0xFFL) << 24;
        case 3: // 4, 3 bytes
          value |= (bytes[position++] & 0xFFL) << 32;
          value |= (bytes[position++] & 0xFFL) << 40;
        case 2: // 2 bytes
          value |= (bytes[position++] & 0xFFL) << 48;
        case 1: // 1 bytes
          value |= (bytes[position++] & 0xFFL) << 56;
        case 0: // 0 bytes
          break;
        default:
          throw new IllegalArgumentException();
      }
      return value;
    }

    private int readThreeBytes() throws IOException {
      int value = input.read() & 0xFF;
      value |= (input.read() & 0xFF) << 8;
      value |= (input.read() & 0xFF) << 16;
      return value;
    }
  }
}
