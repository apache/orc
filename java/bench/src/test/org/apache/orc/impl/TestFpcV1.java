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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestFpcV1 {
  private static final int SIZE = 1024 * 1024;

  @Test
  public void testFpcPowerOfTen() throws IOException {
    for (int i = 1; i < 10; i++) {
      testFpcPowerOfTen(i);
    }
  }

  @Test
  public void testFpcRandom() throws IOException {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final FpcV1.FpcCompressor fpcCompressor =
        new FpcV1.FpcCompressor(output);
    final double[] toCompress = randomDoubles(SIZE);
    fpcCompressor.compress(toCompress, SIZE);

    final ByteArrayInputStream input = new ByteArrayInputStream(
        output.toByteArray());
    final FpcV1.FpcExtractor fpcExtractor =
        new FpcV1.FpcExtractor(input);
    final double[] extracted = fpcExtractor.extract();

    for (int i = 0; i < SIZE; i++) {
      final double toCompressValue = toCompress[i];
      final double extractedValue = extracted[i];
      Assert.assertEquals("Failed " +
              "(expected = " +
              Long.toHexString(Double.doubleToLongBits(toCompressValue)) +
              ", actual = " +
              Long.toHexString(Double.doubleToLongBits(extractedValue)) + ")",
          toCompressValue, extractedValue, 0);
    }
  }

  private void testFpcPowerOfTen(int exponent) throws IOException {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final FpcV1.FpcCompressor fpcCompressor =
        new FpcV1.FpcCompressor(output);
    final double[] toCompress = randomPowerOfTenDoubles(SIZE, exponent);
    fpcCompressor.compress(toCompress, SIZE);

    final ByteArrayInputStream input = new ByteArrayInputStream(
        output.toByteArray());
    final FpcV1.FpcExtractor fpcExtractor =
        new FpcV1.FpcExtractor(input);
    final double[] extracted = fpcExtractor.extract();

    final int comparisionSize = SIZE;
    for (int i = 0; i < comparisionSize; i++) {
      final double toCompressValue = toCompress[i];
      final double extractedValue = extracted[i];
      Assert.assertEquals("Failed at 10^" + exponent +
          "(expected = " +
          Long.toHexString(Double.doubleToLongBits(toCompressValue)) +
          ", actual = " +
          Long.toHexString(Double.doubleToLongBits(extractedValue)) + ")",
          toCompressValue, extractedValue, 0);
    }
  }

  private double[] randomDoubles(int size) {
    final Random random = new Random(0);
    final double[] literals = new double[size];
    for (int i = 0; i < size; i++) {
      literals[i] = Double.longBitsToDouble(random.nextLong());
    }
    return literals;
  }

  private double[] randomPowerOfTenDoubles(int size, int exponent) {
    final Random random = new Random(0);
    final double[] literals = new double[size];
    int powerOfTen = 1;
    for (int i = 0; i < exponent; i++) {
      powerOfTen *= 10;
    }
    for (int i = 0; i < size; i++) {
      literals[i] = random.nextInt(powerOfTen) / (double) powerOfTen;
    }
    return literals;
  }
}
