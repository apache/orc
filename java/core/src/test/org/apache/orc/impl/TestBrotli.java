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
  public void testNoOverflow() {
    ByteBuffer in = ByteBuffer.allocate(10);
    ByteBuffer out = ByteBuffer.allocate(10);
    in.put(new byte[]{1,2,3,4,5,6,7,10});
    in.flip();
    try (BrotliCodec brotliCodec = new BrotliCodec()) {
      assertFalse(brotliCodec.compress(in, out, null,
          brotliCodec.getDefaultOptions()));
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testCompress() {
    ByteBuffer in = ByteBuffer.allocate(100);
    ByteBuffer out = ByteBuffer.allocate(100);
    ByteBuffer result = ByteBuffer.allocate(100);
    in.put(new byte[]{1,2,3,4,5,6,7,7,7,7,7,7,7,4,4,4});
    in.flip();
    try (BrotliCodec brotliCodec = new BrotliCodec()) {
      assertFalse(brotliCodec.compress(in, out, null,
          brotliCodec.getDefaultOptions()));
      out.flip();
      brotliCodec.decompress(out, result);
      assertArrayEquals(result.array(), in.array());
    } catch (Exception e) {
      fail(e);
    }
  }

  @Test
  public void testDirectDecompress() {
    ByteBuffer in = ByteBuffer.allocate(100);
    ByteBuffer out = ByteBuffer.allocate(100);
    ByteBuffer directOut = ByteBuffer.allocateDirect(100);
    ByteBuffer directResult = ByteBuffer.allocateDirect(100);
    in.put(new byte[]{1,2,3,4,5,6,7,7,7,7,7,7,7,4,4,4});
    in.flip();
    try (BrotliCodec brotliCodec = new BrotliCodec()) {
      // write bytes to heap buffer.
      assertFalse(brotliCodec.compress(in, out, null,
          brotliCodec.getDefaultOptions()));
      out.flip();
      // copy heap buffer to direct buffer.
      directOut.put(out.array());
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
