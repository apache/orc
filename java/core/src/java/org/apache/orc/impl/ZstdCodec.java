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

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.orc.CompressionKind;
import org.apache.orc.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public class ZstdCodec implements DirectDecompressionCodec {
  private static final HadoopShims SHIMS = HadoopShimsFactory.get();
  // Note: shim path does not care about levels and strategies (only used for decompression).
  private HadoopShims.DirectDecompressor decompressShim = null;
  private Boolean direct = null;

  private final org.apache.hadoop.io.compress.CompressionCodec zstdCodec;

  public ZstdCodec() {
    // create a default zstd codec
    zstdCodec = SHIMS.createZstdCodec();
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow)
          throws IOException {
    int length = in.remaining();
    int outSize = 0;
    Compressor compressor = zstdCodec.createCompressor();
    try {
      compressor.setInput(in.array(), in.arrayOffset() + in.position(), length);
      compressor.finish();
      int offset = out.arrayOffset() + out.position();
      while (!compressor.finished() && (length > outSize)) {
        int size = compressor.compress(out.array(), offset, out.remaining());
        out.position(size + out.position());
        outSize += size;
        offset += size;
        // if we run out of space in the out buffer, use the overflow
        if (out.remaining() == 0) {
          if (overflow == null) {
            return false;
          }
          out = overflow;
          offset = out.arrayOffset() + out.position();
        }
      }
    } finally {
      compressor.end();
    }
    return length > outSize;
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out)
          throws IOException {
    if (in.isDirect() && out.isDirect()) {
      directDecompress(in, out);
      return;
    }

    Decompressor decompressor = zstdCodec.createDecompressor();
    try {
      decompressor.setInput(in.array(), in.arrayOffset() + in.position(),
              in.remaining());
      while (!(decompressor.finished() || decompressor.needsDictionary() ||
              decompressor.needsInput())) {
        int count = decompressor.decompress(out.array(),
                out.arrayOffset() + out.position(),
                out.remaining());
        out.position(count + out.position());
      }
      out.flip();
    } finally {
      decompressor.end();
    }
    in.position(in.limit());
  }

  @Override
  public boolean isAvailable() {
    if (direct == null) {
      // see nowrap option in new Inflater(boolean) which disables zlib headers
      try {
        ensureShim();
        direct = (decompressShim != null);
      } catch (UnsatisfiedLinkError ule) {
        direct = Boolean.valueOf(false);
      }
    }
    return direct.booleanValue();
  }

  private void ensureShim() {
    if (decompressShim == null) {
      decompressShim = SHIMS.getDirectDecompressor(
              HadoopShims.DirectCompressionType.ZSTD);
    }
  }

  @Override
  public void directDecompress(ByteBuffer in, ByteBuffer out) throws IOException {
    ensureShim();
    decompressShim.decompress(in, out);
    out.flip(); // flip for read
  }

  @Override
  public void reset() {
    if (decompressShim != null) {
      decompressShim.reset();
    }
  }

  @Override
  public void close() {
    if (decompressShim != null) {
      decompressShim.end();
    }
  }

  @Override
  public CompressionCodec modify(EnumSet<Modifier> modifiers) {
    // zstd allows no modifications
    return this;
  }

  @Override
  public CompressionKind getKind() {
    return CompressionKind.ZSTD;
  }
}
