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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class TestBrotli {
  @Test
  public void testOutputLargerThanBefore() {
    ByteBuffer in = ByteBuffer.allocate(10);
    ByteBuffer out = ByteBuffer.allocate(10);
    in.put(new byte[]{1, 2, 3, 4, 5, 6, 7, 10});
    in.flip();
    try (BrotliCodec brotliCodec = new BrotliCodec()) {
      // The compressed data length is larger than the original data.
      assertFalse(brotliCodec.compress(in, out, null,
          brotliCodec.getDefaultOptions()));
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testCompress() {
    ByteBuffer in = ByteBuffer.allocate(10000);
    ByteBuffer out = ByteBuffer.allocate(500);
    ByteBuffer result = ByteBuffer.allocate(10000);
    for (int i = 0; i < 10000; i++) {
      in.put((byte) i);
    }
    in.flip();
    try (BrotliCodec brotliCodec = new BrotliCodec()) {
      assertTrue(brotliCodec.compress(in, out, null,
          brotliCodec.getDefaultOptions()));
      out.flip();
      brotliCodec.decompress(out, result);
      assertArrayEquals(result.array(), in.array());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testCompressNotFromStart() {
    ByteBuffer in = ByteBuffer.allocate(10000);
    ByteBuffer out = ByteBuffer.allocate(10000);
    ByteBuffer result = ByteBuffer.allocate(10000);
    for (int i = 0; i < 10000; i++) {
      in.put((byte) i);
    }
    in.flip();
    in.get();

    ByteBuffer slice = in.slice();
    byte[] originalBytes = new byte[slice.remaining()];
    slice.get(originalBytes);

    try (BrotliCodec brotliCodec = new BrotliCodec()) {
      // The compressed data length is larger than the original data.
      assertTrue(brotliCodec.compress(in, out, null,
          brotliCodec.getDefaultOptions()));

      out.flip();
      brotliCodec.decompress(out, result);

      byte[] resultArray = new byte[result.remaining()];
      result.get(resultArray);
      assertArrayEquals(resultArray, originalBytes);
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testCompressWithOverflow() {
    ByteBuffer in = ByteBuffer.allocate(10000);
    ByteBuffer out = ByteBuffer.allocate(1);
    ByteBuffer overflow = ByteBuffer.allocate(10000);
    ByteBuffer result = ByteBuffer.allocate(10000);
    for (int i = 0; i < 10000; i++) {
      in.put((byte) i);
    }
    in.flip();
    try (BrotliCodec brotliCodec = new BrotliCodec()) {
      assertTrue(brotliCodec.compress(in, out, overflow,
          brotliCodec.getDefaultOptions()));
      out.flip();
      overflow.flip();

      // copy out, overflow to compressed
      byte[] compressed = new byte[out.remaining() + overflow.remaining()];
      System.arraycopy(out.array(), out.arrayOffset() + out.position(), compressed, 0, out.remaining());
      System.arraycopy(overflow.array(), overflow.arrayOffset() + overflow.position(), compressed, out.remaining(), overflow.remaining());
      // decompress compressedBuffer and check the result.
      ByteBuffer compressedBuffer = ByteBuffer.allocate(compressed.length);
      compressedBuffer.put(compressed);
      compressedBuffer.flip();
      brotliCodec.decompress(compressedBuffer, result);
      assertArrayEquals(result.array(), in.array());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testDirectDecompress() {
    ByteBuffer in = ByteBuffer.allocate(10000);
    ByteBuffer out = ByteBuffer.allocate(10000);
    ByteBuffer directOut = ByteBuffer.allocateDirect(10000);
    ByteBuffer directResult = ByteBuffer.allocateDirect(10000);
    for (int i = 0; i < 10000; i++) {
      in.put((byte) i);
    }
    in.flip();
    try (BrotliCodec brotliCodec = new BrotliCodec()) {
      // write bytes to heap buffer.
      assertTrue(brotliCodec.compress(in, out, null,
          brotliCodec.getDefaultOptions()));
      out.flip();
      // copy heap buffer to direct buffer.
      directOut.put(out);
      directOut.flip();

      brotliCodec.decompress(directOut, directResult);

      // copy result from direct buffer to heap.
      byte[] heapBytes = new byte[in.array().length];
      directResult.get(heapBytes, 0, directResult.limit());

      assertArrayEquals(in.array(), heapBytes);
    } catch (Exception e) {
      fail(e);
    }
  }
}
