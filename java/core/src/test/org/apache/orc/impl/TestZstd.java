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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdException;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

public class TestZstd {

  /**
   * Test that Zstandard compression does not overflow nor throw an
   * exception when the allocated output array matches
   * the estimated upper bound ZSTD_compressBound. Random byte inputs are
   * used, which are a worst-case for output
   * compression sizes.
   */
  @Test
  public void testZstdCodecNoOverflow() {
    Random rd = new Random();
    ArrayList<Integer> testInputDataSizes =
        new ArrayList<>(Arrays.asList(8, 27, 182, 818, 28459));

    testInputDataSizes.forEach(inputSize -> {
          ByteBuffer in = ByteBuffer.allocate(inputSize);
          ByteBuffer out =
              ByteBuffer.allocate((int) Zstd.compressBound((long) inputSize));

          byte[] arr = new byte[inputSize];
          rd.nextBytes(arr);

          in.put(arr);
          in.flip();
          CompressionCodec codec = new ZstdCodec();
          boolean overflow;
          try {
            overflow = codec.compress(in, out, null, codec.getDefaultOptions());
          } catch (IOException e) {
            overflow = true;
          }
          assertFalse(overflow);
        }
    );
  }

  @Test
  public void testCorrupt() throws Exception {
    ByteBuffer buf = ByteBuffer.allocate(1000);
    buf.put(new byte[] {127, 125, 1, 99, 98, 1});
    buf.flip();
    CompressionCodec codec = new ZstdCodec();
    ByteBuffer out = ByteBuffer.allocate(1000);
    try {
      codec.decompress(buf, out);
      fail();
    } catch (ZstdException ioe) {
      // EXPECTED
    }
  }

  /**
   * Test compatibility of zstd-jni and aircompressor Zstd implementations
   * by checking that bytes compressed with one can be decompressed by the
   * other when using the default options.
   */
  @Test
  public void testZstdAircompressorJniCompressDecompress() throws Exception {
    int inputSize = 27182;
    Random rd = new Random();

    CompressionCodec zstdAircompressorCodec = new AircompressorCodec(
        CompressionKind.ZSTD, new ZstdCompressor(), new ZstdDecompressor());
    CompressionCodec zstdJniCodec = new ZstdCodec();

    ByteBuffer sourceCompressorIn = ByteBuffer.allocate(inputSize);
    ByteBuffer sourceCompressorOut =
        ByteBuffer.allocate((int) Zstd.compressBound(inputSize));
    ByteBuffer destCompressorOut = ByteBuffer.allocate(inputSize);

    // Use an array half filled with a constant value & half filled with
    // random values.
    byte[] constantBytes = new byte[inputSize / 2];
    java.util.Arrays.fill(constantBytes, 0, inputSize / 2, (byte) 2);
    sourceCompressorIn.put(constantBytes);
    byte[] randomBytes = new byte[inputSize - inputSize / 2];
    rd.nextBytes(randomBytes);
    sourceCompressorIn.put(randomBytes);
    sourceCompressorIn.flip();

    // Verify that input -> aircompressor compresson -> zstd-jni
    // decompression returns the input.
    // Note: This function returns false if the bytes get larger. But why is
    // that a problem? sourceCompressorOut has the
    // capacity.
    zstdAircompressorCodec.compress(sourceCompressorIn, sourceCompressorOut,
        null, zstdAircompressorCodec.getDefaultOptions());
    sourceCompressorOut.flip();

    zstdJniCodec.decompress(sourceCompressorOut, destCompressorOut);
    assertEquals(sourceCompressorIn, destCompressorOut,
        "aircompressor compression with zstd-jni decompression did not return"
            + " the input!");

    sourceCompressorIn.rewind();
    sourceCompressorOut.clear();
    destCompressorOut.clear();

    // Verify that input -> zstd-jni compresson -> aircompressor
    // decompression returns the input.
    zstdJniCodec.compress(sourceCompressorIn, sourceCompressorOut, null,
        zstdJniCodec.getDefaultOptions());
    sourceCompressorOut.flip();
    zstdAircompressorCodec.decompress(sourceCompressorOut, destCompressorOut);
    assertEquals(sourceCompressorIn, destCompressorOut,
        "zstd-jni compression with aircompressor decompression did not return"
            + " the input!");
  }
}
