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

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.DecoderJNI;
import com.aayushatharva.brotli4j.encoder.Encoder;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BrotliCodec implements CompressionCodec, DirectDecompressionCodec {
  // load jni library.
  static {
    Brotli4jLoader.ensureAvailability();
  }

  public BrotliCodec() {
  }

  static class BrotliOptions implements Options {

    private Encoder.Mode mode = Encoder.Mode.GENERIC;
    private int quality = -1;
    private int lgwin = -1;

    BrotliOptions() {

    }

    BrotliOptions(int quality, int lgwin, Encoder.Mode mode) {
      this.quality = quality;
      this.lgwin = lgwin;
      this.mode = mode;
    }

    @Override
    public Options copy() {
      return new BrotliOptions(quality, lgwin, mode);
    }

    @Override
    public Options setSpeed(SpeedModifier newValue) {
      switch (newValue) {
        case FAST:
          // best speed + 1.
          quality = 1;
          break;
        case DEFAULT:
          // best quality. Keep default with default value.
          quality = -1;
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
          mode = Encoder.Mode.GENERIC;
          break;
        case TEXT:
          mode = Encoder.Mode.TEXT;
          break;
        default:
          break;
      }
      return this;
    }

    public Encoder.Parameters brotli4jParameter() {
      return new Encoder.Parameters()
          .setQuality(quality).setWindow(lgwin).setMode(mode);
    }
  }

  private static final BrotliCodec.BrotliOptions DEFAULT_OPTIONS = new BrotliOptions();

  @Override
  public Options getDefaultOptions() {
    return DEFAULT_OPTIONS;
  }

  @Override
  public boolean compress(
      ByteBuffer in,
      ByteBuffer out,
      ByteBuffer overflow,
      Options options) throws IOException {
    BrotliOptions brotliOptions = (BrotliOptions) options;
    int inBytes = in.remaining();
    byte[] compressed = Encoder.compress(
        in.array(), in.arrayOffset() + in.position(), inBytes, brotliOptions.brotli4jParameter());
    int outBytes = compressed.length;
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
    int compressedBytes = in.remaining();
    DecoderJNI.Wrapper decoder = new DecoderJNI.Wrapper(compressedBytes);
    try {
      decoder.getInputBuffer().put(in);
      decoder.push(compressedBytes);
      while (decoder.getStatus() != DecoderJNI.Status.DONE) {
        switch (decoder.getStatus()) {
          case OK:
            decoder.push(0);
            break;

          case NEEDS_MORE_OUTPUT:
            ByteBuffer buffer = decoder.pull();
            out.put(buffer);
            break;

          case NEEDS_MORE_INPUT:
            // Give decoder a chance to process the remaining of the buffered byte.
            decoder.push(0);
            // If decoder still needs input, this means that stream is truncated.
            if (decoder.getStatus() == DecoderJNI.Status.NEEDS_MORE_INPUT) {
              return;
            }
            break;

          default:
            return;
        }
      }
    } finally {
      out.flip();
      decoder.destroy();
    }
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public CompressionKind getKind() {
    return CompressionKind.BROTLI;
  }


  @Override
  public void directDecompress(ByteBuffer in, ByteBuffer out) throws IOException {
    // decompress work well for both direct and heap.
    decompress(in, out);
  }

  @Override
  public void reset() {
  }

  @Override
  public void destroy() {
  }

  @Override
  public void close() {
    OrcCodecPool.returnCodec(CompressionKind.BROTLI, this);
  }
}
