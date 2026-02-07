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

import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class XerialSnappyCodec implements CompressionCodec, DirectDecompressionCodec {
  private static final ThreadLocal<byte[]> threadBuffer = ThreadLocal.withInitial(() -> null);

  static {
    Snappy.getNativeLibraryVersion();
  }

  public XerialSnappyCodec() {}

  protected static byte[] getBuffer(int size) {
    byte[] result = threadBuffer.get();
    if (result == null || result.length < size || result.length > size * 2) {
      result = new byte[size];
      threadBuffer.set(result);
    }
    return result;
  }

  @Override
  public Options getDefaultOptions() {
    return CompressionCodec.NullOptions.INSTANCE;
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out,
                          ByteBuffer overflow,
                          Options options) throws IOException {
    int inBytes = in.remaining();
    // Skip with minimum size check similar to ZstdCodec
    if (inBytes < 10) return false;

    int maxOutputLength = Snappy.maxCompressedLength(inBytes);
    byte[] compressed = getBuffer(maxOutputLength);

    int outBytes = Snappy.compress(in.array(), in.arrayOffset() + in.position(), inBytes,
        compressed, 0);

    if (outBytes < inBytes) {
      int remaining = out.remaining();
      if (remaining >= outBytes) {
        System.arraycopy(compressed, 0, out.array(), out.arrayOffset() +
                out.position(), outBytes);
        out.position(out.position() + outBytes);
      } else {
        System.arraycopy(compressed, 0, out.array(), out.arrayOffset() +
                out.position(), remaining);
        out.position(out.limit());
        System.arraycopy(compressed, remaining, overflow.array(),
                overflow.arrayOffset(), outBytes - remaining);
        overflow.position(outBytes - remaining);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    if (in.isDirect() && out.isDirect()) {
      directDecompress(in, out);
      return;
    }

    int srcOffset = in.arrayOffset() + in.position();
    int srcSize = in.remaining();
    int dstOffset = out.arrayOffset() + out.position();

    int decompressedBytes = Snappy.uncompress(in.array(), srcOffset, srcSize, out.array(),
        dstOffset);

    in.position(in.limit());
    out.position(dstOffset + decompressedBytes);
    out.flip();
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public void directDecompress(ByteBuffer in, ByteBuffer out) throws IOException {
    // Xerial Snappy supports direct ByteBuffer decompression
    int outPos = out.position();
    int decompressedBytes = Snappy.uncompress(in, out);
    out.position(outPos + decompressedBytes);
    out.flip();
  }

  @Override
  public void reset() {
  }

  @Override
  public void destroy() {
  }

  @Override
  public CompressionKind getKind() {
    return CompressionKind.SNAPPY;
  }

  @Override
  public void close() {
    OrcCodecPool.returnCodec(CompressionKind.SNAPPY, this);
  }
}
