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

import net.jpountz.lz4.LZ4Exception;
import org.apache.orc.CompressionCodec;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestLz4 {

  @Test
  public void testNoOverflow() throws Exception {
    ByteBuffer in = ByteBuffer.allocate(10);
    ByteBuffer out = ByteBuffer.allocate(10);
    in.put(new byte[]{1, 2, 3, 4, 5, 6, 7, 10});
    in.flip();
    CompressionCodec codec = new Lz4Codec();
    assertFalse(codec.compress(in, out, null,
            codec.getDefaultOptions()));
  }

  @Test
  public void testCorrupt() throws Exception {
    ByteBuffer buf = ByteBuffer.allocate(1000);
    buf.put(new byte[] {127, 125, 1, 99, 98, 1});
    buf.flip();
    CompressionCodec codec = new Lz4Codec();
    ByteBuffer out = ByteBuffer.allocate(1000);
    try {
      codec.decompress(buf, out);
      fail();
    } catch (LZ4Exception ioe) {
      // EXPECTED
    }
  }

  @Test
  public void testLz4CompressDecompress() throws Exception {
    int inputSize = 10000;
    CompressionCodec codec = new Lz4Codec();

    ByteBuffer in = ByteBuffer.allocate(inputSize);
    ByteBuffer out = ByteBuffer.allocate(inputSize);
    ByteBuffer compressed = ByteBuffer.allocate(inputSize * 2); // Ample space for compressed data
    ByteBuffer decompressed = ByteBuffer.allocate(inputSize);

    for (int i = 0; i < inputSize; i++) {
      in.put((byte) i);
    }
    in.flip();

    // Compress
    assertTrue(codec.compress(in, compressed, null, codec.getDefaultOptions()));
    compressed.flip();

    // Decompress
    codec.decompress(compressed, decompressed);

    assertArrayEquals(in.array(), decompressed.array());
  }

  @Test
  public void testLz4DirectDecompress() {
    ByteBuffer in = ByteBuffer.allocate(10000);
    ByteBuffer out = ByteBuffer.allocate(10000); // Heap buffer for initial compression
    ByteBuffer directOut = ByteBuffer.allocateDirect(10000);
    ByteBuffer directResult = ByteBuffer.allocateDirect(10000);
    for (int i = 0; i < 10000; i++) {
      in.put((byte) i);
    }
    in.flip();
    try (Lz4Codec codec = new Lz4Codec()) {
      assertTrue(codec.compress(in, out, null, codec.getDefaultOptions()));
      out.flip();
      directOut.put(out);
      directOut.flip();

      codec.decompress(directOut, directResult);

      // copy result from direct buffer to heap.
      byte[] heapBytes = new byte[in.array().length];
      directResult.get(heapBytes, 0, directResult.limit());

      assertArrayEquals(in.array(), heapBytes);
    } catch (Exception e) {
      fail(e);
    }
  }
}
