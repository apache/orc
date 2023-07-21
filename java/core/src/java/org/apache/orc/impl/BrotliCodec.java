package org.apache.orc.impl;

import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.meteogroup.jbrotli.BrotliCompressor;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BrotliCodec implements CompressionCodec, DirectDecompressionCodec{

  static class BrotliOptions implements Options {

    @Override
    public Options copy() {
      return null;
    }

    @Override
    public Options setSpeed(SpeedModifier newValue) {
      return null;
    }

    @Override
    public Options setData(DataKind newValue) {
      return null;
    }
  }

  private static final BrotliCodec.BrotliOptions DEFAULT_OPTIONS = new BrotliOptions();

  @Override
  public Options getDefaultOptions() {
    return DEFAULT_OPTIONS;
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow, Options options) throws IOException {
    BrotliCompressor compressor = new BrotliCompressor();
    compressor.compress()

    return false;
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {

  }

  @Override
  public boolean isAvailable() {
    return false;
  }

  @Override
  public void directDecompress(ByteBuffer in, ByteBuffer out) throws IOException {

  }

  @Override
  public void reset() {

  }

  @Override
  public void destroy() {

  }

  @Override
  public CompressionKind getKind() {
    return CompressionKind.BROTLI;
  }

  @Override
  public void close() {

  }
}
