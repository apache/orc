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

import org.apache.hadoop.io.compress.brotli.BrotliCompressor;
import org.apache.hadoop.io.compress.brotli.BrotliDecompressor;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.meteogroup.jbrotli.Brotli;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class BrotliCodec implements CompressionCodec, DirectDecompressionCodec{
  // load jni library.
  private final org.apache.hadoop.io.compress.BrotliCodec hadoopCodec = new org.apache.hadoop.io.compress.BrotliCodec();
  private static final HadoopShims SHIMS = HadoopShimsFactory.get();
  private Boolean direct = null;

  // Note: shim path does not care about levels and strategies (only used for decompression).
  private HadoopShims.DirectDecompressor decompressShim = null;

  public BrotliCodec() {
  }

  static class BrotliOptions implements Options {

    private Brotli.Mode mode;
    private int quality;
    private int lgwin = 22;
    private int lgblock = 0;

    @Override
    public Options copy() {
      return new BrotliOptions();
    }

    @Override
    public Options setSpeed(SpeedModifier newValue) {
      switch (newValue) {
        case FAST:
          // best quality + 1.
          quality = 1;
          break;
        case DEFAULT:
          // best quality. Keep default with default value.
          quality = 11;
          break;
        case FASTEST:
          // best speed.
          quality = 0;
          break;
        default:
          break;
      }
      return this;
    }

    @Override
    public Options setData(DataKind newValue) {
      switch (newValue) {
        case BINARY:
          mode = Brotli.Mode.GENERIC;
          break;
        case TEXT:
          mode = Brotli.Mode.TEXT;
          break;
        default:
          break;
      }
      return this;
    }

    public Brotli.Parameter brotliParameter() {
      return new Brotli.Parameter();
    }
  }

  private static final BrotliCodec.BrotliOptions DEFAULT_OPTIONS = new BrotliOptions();

  @Override
  public Options getDefaultOptions() {
    return DEFAULT_OPTIONS;
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow, Options options) throws IOException {
    int length = in.remaining();
    int outSize = 0;
    BrotliCompressor compressor = new BrotliCompressor(null);
    applyOptions(compressor, options);
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
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {
    if(in.isDirect() && out.isDirect()) {
      directDecompress(in, out);
      return;
    }

    BrotliDecompressor decompressor = new BrotliDecompressor();
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
      try {
        ensureShim();
        direct = (decompressShim != null);
      } catch (UnsatisfiedLinkError ule) {
        direct = false;
      }
    }
    return direct;
  }

  @Override
  public CompressionKind getKind() {
    return CompressionKind.BROTLI;
  }


  private void ensureShim() {
    if (decompressShim == null) {
      decompressShim = SHIMS.getDirectDecompressor(
          HadoopShims.DirectCompressionType.BROTLI);
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
  public void destroy() {
    if (decompressShim != null) {
      decompressShim.end();
    }
  }

  @Override
  public void close() {
    OrcCodecPool.returnCodec(CompressionKind.BROTLI, this);
  }

  public void applyOptions(BrotliCompressor compressor, Options options) {
    try {
      BrotliOptions brotliOptions = (BrotliOptions)options;
      Brotli.Parameter parameter = brotliOptions.brotliParameter();
      Field parameterField = BrotliCompressor.class.getDeclaredField("parameter");
      parameterField.setAccessible(true);
      parameterField.set(compressor, parameter);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to apply brotli options.", e);
    }
  }
}
