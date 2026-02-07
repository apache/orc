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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Lz4Codec implements CompressionCodec, DirectDecompressionCodec {
  private static final LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
  private static final ThreadLocal<byte[]> threadBuffer = ThreadLocal.withInitial(() -> null);

  public Lz4Codec() {}

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

    LZ4Compressor compressor = lz4Factory.fastCompressor();
    int maxOutputLength = compressor.maxCompressedLength(inBytes);
    byte[] compressed = getBuffer(maxOutputLength);

    int outBytes = compressor.compress(in.array(), in.arrayOffset() + in.position(), inBytes,
        compressed, 0, maxOutputLength);

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
    int dstSize = out.remaining();

    LZ4SafeDecompressor decompressor = lz4Factory.safeDecompressor();
    int decompressedBytes = decompressor.decompress(in.array(), srcOffset, srcSize, out.array(),
        dstOffset, dstSize);

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
    LZ4SafeDecompressor decompressor = lz4Factory.safeDecompressor();
    decompressor.decompress(in, out);
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
    return CompressionKind.LZ4;
  }

  @Override
  public void close() {
    OrcCodecPool.returnCodec(CompressionKind.LZ4, this);
  }
}
