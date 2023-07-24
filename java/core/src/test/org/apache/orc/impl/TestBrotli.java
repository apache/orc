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
}
