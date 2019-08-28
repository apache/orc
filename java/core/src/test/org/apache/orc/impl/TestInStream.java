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

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Key;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionCodec;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.writer.StreamOptions;
import org.junit.Test;

import javax.crypto.spec.SecretKeySpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestInStream {

  public static class OutputCollector implements PhysicalWriter.OutputReceiver {
    public DynamicByteArray buffer = new DynamicByteArray();

    @Override
    public void output(ByteBuffer buffer) {
      this.buffer.add(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }

    @Override
    public void suppress() {
      // PASS
    }
  }

  static class PositionCollector
      implements PositionProvider, PositionRecorder {
    private List<Long> positions = new ArrayList<>();
    private int index = 0;

    @Override
    public long getNext() {
      return positions.get(index++);
    }

    @Override
    public void addPosition(long offset) {
      positions.add(offset);
    }

    public void reset() {
      index = 0;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder("position: ");
      for(int i=0; i < positions.size(); ++i) {
        if (i != 0) {
          builder.append(", ");
        }
        builder.append(positions.get(i));
      }
      return builder.toString();
    }
  }

  static byte[] getUncompressed(PositionCollector[] positions) throws IOException {
    OutputCollector collect = new OutputCollector();
    OutStream out = new OutStream("test", new StreamOptions(100), collect);
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();
    assertEquals(1024, collect.buffer.size());
    for(int i=0; i < 1024; ++i) {
      assertEquals((byte) i, collect.buffer.get(i));
    }
    return collect.buffer.get();
  }

  @Test
  public void testUncompressed() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getUncompressed(positions);
    for(int i=0; i < 1024; ++i) {
      assertEquals((byte) i, bytes[i]);
    }
    ByteBuffer inBuf = ByteBuffer.wrap(bytes);
    InStream in = InStream.create("test", new BufferChunk(inBuf, 0),
        0, inBuf.remaining());
    assertEquals("uncompressed stream test position: 0 length: 1024" +
                 " range: 0 offset: 0 position: 0 limit: 1024",
                 in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testUncompressedPartial() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getUncompressed(positions);
    ByteBuffer inBuf = ByteBuffer.allocate(3 * 1024);
    inBuf.position(123);
    inBuf.put(bytes);
    inBuf.clear();
    InStream in = InStream.create("test", new BufferChunk(inBuf, 33),
        156, 1024);
    assertEquals("uncompressed stream test position: 0 length: 1024" +
                     " range: 0 offset: 33 position: 123 limit: 1147",
        in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals("value " + i, i & 0xff, x);
    }
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals("value " + i, i & 0xff, in.read());
    }
  }

  static byte[] getEncrypted(PositionCollector[] positions,
                             byte[] key,
                             byte[] iv,
                             EncryptionAlgorithm algorithm,
                             int ROW_COUNT,
                             long DATA_CONST) throws IOException {
    OutputCollector collect = new OutputCollector();
    for(int i=0; i < key.length; ++i) {
      key[i] = (byte) i;
    }
    Key decryptKey = new SecretKeySpec(key, algorithm.getAlgorithm());
    StreamOptions writerOptions = new StreamOptions(100)
                                      .withEncryption(algorithm, decryptKey);
    writerOptions.modifyIv(CryptoUtils.modifyIvForStream(0,
        OrcProto.Stream.Kind.DATA, 1));
    System.arraycopy(writerOptions.getIv(), 0, iv, 0, iv.length);
    OutStream out = new OutStream("test", writerOptions, collect);
    DataOutputStream outStream = new DataOutputStream(out);
    for(int i=0; i < ROW_COUNT; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      outStream.writeLong(i * DATA_CONST);
    }
    out.flush();
    byte[] result = collect.buffer.get();
    assertEquals(ROW_COUNT * 8, result.length);
    return result;
  }

  @Test
  public void testEncrypted() throws Exception {
    final long DATA_CONST = 0x1_0000_0003L;
    final int ROW_COUNT = 1024;
    PositionCollector[] positions = new PositionCollector[ROW_COUNT];
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] rawKey = new byte[algorithm.keyLength()];
    byte[] iv = new byte[algorithm.getIvLength()];
    byte[] bytes = getEncrypted(positions, rawKey, iv, algorithm, ROW_COUNT,
        DATA_CONST);

    // Allocate the stream into three ranges. making sure that they don't fall
    // on the 16 byte aes boundaries.
    int[] rangeSizes = {1965, ROW_COUNT * 8 - 1965 - 15, 15};
    int offset = 0;
    BufferChunkList list = new BufferChunkList();
    for(int size: rangeSizes) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.put(bytes, offset, size);
      buffer.flip();
      list.add(new BufferChunk(buffer, offset));
      offset += size;
    }

    InStream in = InStream.create("test", list.get(), 0, bytes.length,
        InStream.options().withEncryption(EncryptionAlgorithm.AES_CTR_128,
            new SecretKeySpec(rawKey, algorithm.getAlgorithm()), iv));
    assertEquals("encrypted uncompressed stream test position: 0 length: 8192" +
            " range: 0 offset: 0 position: 0 limit: 1965",
        in.toString());
    DataInputStream inputStream = new DataInputStream(in);
    for(int i=0; i < ROW_COUNT; ++i) {
      assertEquals("row " + i, i * DATA_CONST, inputStream.readLong());
    }
    for(int i=ROW_COUNT - 1; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals("row " + i, i * DATA_CONST, inputStream.readLong());
    }
  }

  @Test
  public void testEncryptedPartial() throws Exception {
    final long DATA_CONST = 0x1_0000_0003L;
    final int ROW_COUNT = 1024;
    PositionCollector[] positions = new PositionCollector[ROW_COUNT];
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] rawKey = new byte[algorithm.keyLength()];
    byte[] iv = new byte[algorithm.getIvLength()];
    byte[] bytes = getEncrypted(positions, rawKey, iv, algorithm, ROW_COUNT,
        DATA_CONST);

    // Allocate the stream into three ranges. making sure that they don't fall
    // on the 16 byte aes boundaries.
    BufferChunkList list = new BufferChunkList();
    ByteBuffer buffer = ByteBuffer.allocate(2000);
    buffer.position(35);
    buffer.put(bytes, 0, 1965);
    buffer.clear();
    list.add(new BufferChunk(buffer, 0));

    int SECOND_SIZE = ROW_COUNT * 8 - 1965 - 15;
    buffer = ByteBuffer.allocate(SECOND_SIZE);
    buffer.put(bytes, 1965, buffer.remaining());
    buffer.clear();
    list.add(new BufferChunk(buffer, 2000));

    buffer = ByteBuffer.allocate(2000);
    buffer.put(bytes, 1965 + SECOND_SIZE, 15);
    buffer.clear();
    list.add(new BufferChunk(buffer, 2000 + SECOND_SIZE));

    InStream in = InStream.create("test", list.get(), 35, bytes.length,
        InStream.options().withEncryption(EncryptionAlgorithm.AES_CTR_128,
            new SecretKeySpec(rawKey, algorithm.getAlgorithm()), iv));
    assertEquals("encrypted uncompressed stream test position: 0 length: 8192" +
                     " range: 0 offset: 0 position: 0 limit: 1965",
        in.toString());
    DataInputStream inputStream = new DataInputStream(in);
    for(int i=0; i < ROW_COUNT; ++i) {
      assertEquals("row " + i, i * DATA_CONST, inputStream.readLong());
    }
    for(int i=ROW_COUNT - 1; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals("row " + i, i * DATA_CONST, inputStream.readLong());
    }
  }

  static byte[] getCompressedEncrypted(byte[] key,
                                       byte[] iv,
                                       PositionCollector[] positions,
                                       EncryptionAlgorithm algorithm,
                                       int ROW_COUNT,
                                       long DATA_CONST) throws IOException {
    OutputCollector collect = new OutputCollector();
    for(int i=0; i < key.length; ++i) {
      key[i] = (byte) i;
    }
    Key decryptKey = new SecretKeySpec(key, algorithm.getAlgorithm());
    CompressionCodec codec = new ZlibCodec();
    StreamOptions writerOptions = new StreamOptions(500)
                                      .withCodec(codec, codec.getDefaultOptions())
                                      .withEncryption(algorithm, decryptKey);
    writerOptions.modifyIv(CryptoUtils.modifyIvForStream(0,
        OrcProto.Stream.Kind.DATA, 1));
    System.arraycopy(writerOptions.getIv(), 0, iv, 0, iv.length);
    OutStream out = new OutStream("test", writerOptions, collect);
    DataOutputStream outStream = new DataOutputStream(out);
    for(int i=0; i < ROW_COUNT; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      outStream.writeLong(i * DATA_CONST);
    }
    out.flush();
    return collect.buffer.get();
  }

  @Test
  public void testCompressedEncrypted() throws Exception {
    final long DATA_CONST = 0x1_0000_0003L;
    final int ROW_COUNT = 1024;
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] key = new byte[algorithm.keyLength()];
    byte[] iv = new byte[algorithm.getIvLength()];
    PositionCollector[] positions = new PositionCollector[ROW_COUNT];
    byte[] bytes= getCompressedEncrypted(key, iv, positions, algorithm, ROW_COUNT, DATA_CONST);

    // currently 3957 bytes
    assertEquals(3957, bytes.length);

    // Allocate the stream into three ranges. making sure that they don't fall
    // on the 16 byte aes boundaries.
    int[] rangeSizes = {1998, bytes.length - 1998 - 15, 15};
    int offset = 0;
    BufferChunkList list = new BufferChunkList();
    for(int size: rangeSizes) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      buffer.put(bytes, offset, size);
      buffer.flip();
      list.add(new BufferChunk(buffer, offset));
      offset += size;
    }

    InStream in = InStream.create("test", list.get(), 0, bytes.length,
        InStream.options()
            .withCodec(new ZlibCodec()).withBufferSize(500)
            .withEncryption(algorithm, new SecretKeySpec(key, algorithm.getAlgorithm()), iv));
    assertEquals("encrypted compressed stream test position: 0 length: " +
            bytes.length + " range: 0 offset: 0 limit: 1998 range 0 = 0 to" +
            " 1998;  range 1 = 1998 to " + (bytes.length - 15) +
            ";  range 2 = " +
            (bytes.length - 15) + " to " + bytes.length,
        in.toString());
    DataInputStream inputStream = new DataInputStream(in);
    for(int i=0; i < ROW_COUNT; ++i) {
      assertEquals("row " + i, i * DATA_CONST, inputStream.readLong());
    }
    for(int i=ROW_COUNT - 1; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals("row " + i, i * DATA_CONST, inputStream.readLong());
    }
  }

  @Test
  public void testCompressedEncryptedPartial() throws Exception {
    final long DATA_CONST = 0x1_0000_0003L;
    final int ROW_COUNT = 1024;
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] key = new byte[algorithm.keyLength()];
    byte[] iv = new byte[algorithm.getIvLength()];
    PositionCollector[] positions = new PositionCollector[ROW_COUNT];
    byte[] bytes= getCompressedEncrypted(key, iv, positions, algorithm, ROW_COUNT, DATA_CONST);

    // currently 3957 bytes
    assertEquals(3957, bytes.length);

    // Allocate the stream into three ranges. making sure that they don't fall
    // on the 16 byte aes boundaries.
    BufferChunkList list = new BufferChunkList();
    ByteBuffer buffer = ByteBuffer.allocate(2000);
    buffer.position(2);
    buffer.put(bytes, 0 , 1998);
    buffer.clear();
    list.add(new BufferChunk(buffer, 100));

    int SECOND_SIZE = bytes.length - 1998 - 15;
    buffer = ByteBuffer.allocate(SECOND_SIZE);
    buffer.put(bytes, 1998, SECOND_SIZE);
    buffer.clear();
    list.add(new BufferChunk(buffer, 2100));

    buffer = ByteBuffer.allocate(1000);
    buffer.put(bytes, 1998 + SECOND_SIZE, 15);
    buffer.clear();
    list.add(new BufferChunk(buffer, 2100 + SECOND_SIZE));

    InStream in = InStream.create("test", list.get(), 102, bytes.length,
        InStream.options()
            .withCodec(new ZlibCodec()).withBufferSize(500)
            .withEncryption(algorithm, new SecretKeySpec(key, algorithm.getAlgorithm()), iv));
    assertEquals("encrypted compressed stream test position: 0 length: " +
                     bytes.length + " range: 0 offset: 0 limit: 1998 range 0 = 100 to 2100;" +
                     "  range 1 = 2100 to 4044;  range 2 = 4044 to 5044",
        in.toString());
    DataInputStream inputStream = new DataInputStream(in);
    for(int i=0; i < ROW_COUNT; ++i) {
      assertEquals("row " + i, i * DATA_CONST, inputStream.readLong());
    }
    for(int i=ROW_COUNT - 1; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals("row " + i, i * DATA_CONST, inputStream.readLong());
    }
  }

  byte[] getCompressed(PositionCollector[] positions) throws IOException {
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(300)
                                .withCodec(codec, codec.getDefaultOptions());
    OutputCollector collect = new OutputCollector();
    OutStream out = new OutStream("test", options, collect);
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();
    assertEquals("test", out.toString());
    return collect.buffer.get();
  }

  @Test
  public void testCompressed() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getCompressed(positions);

    assertEquals(961, bytes.length);
    InStream in = InStream.create("test", new BufferChunk(ByteBuffer.wrap(bytes), 0), 0,
        bytes.length, InStream.options().withCodec(new ZlibCodec()).withBufferSize(300));
    assertEquals("compressed stream test position: 0 length: 961 range: 0" +
                 " offset: 0 limit: 961 range 0 = 0 to 961",
                 in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCompressedPartial() throws Exception {
    PositionCollector[] positions = new PositionCollector[1024];
    byte[] bytes = getCompressed(positions);

    assertEquals(961, bytes.length);
    ByteBuffer buffer = ByteBuffer.allocate(1500);
    buffer.position(39);
    buffer.put(bytes, 0, bytes.length);
    buffer.clear();

    InStream in = InStream.create("test", new BufferChunk(buffer, 100), 139,
        bytes.length, InStream.options().withCodec(new ZlibCodec()).withBufferSize(300));
    assertEquals("compressed stream test position: 0 length: 961 range: 0" +
                     " offset: 39 limit: 1000 range 0 = 100 to 1600",
        in.toString());
    for(int i=0; i < 1024; ++i) {
      int x = in.read();
      assertEquals(i & 0xff, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i & 0xff, in.read());
    }
  }

  @Test
  public void testCorruptStream() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(500)
                                .withCodec(codec, codec.getDefaultOptions());
    OutStream out = new OutStream("test", options, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      out.write(i);
    }
    out.flush();

    // now try to read the stream with a buffer that is too small
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream in = InStream.create("test", new BufferChunk(inBuf, 0), 0,
        inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(100));
    byte[] contents = new byte[1024];
    try {
      in.read(contents);
      fail();
    } catch(IllegalArgumentException iae) {
      // EXPECTED
    }

    // make a corrupted header
    inBuf.clear();
    inBuf.put((byte) 32);
    inBuf.put((byte) 0);
    inBuf.flip();
    in = InStream.create("test2", new BufferChunk(inBuf, 0), 0,
        inBuf.remaining(),
        InStream.options().withCodec(codec).withBufferSize(300));
    try {
      in.read();
      fail();
    } catch (IllegalStateException ise) {
      // EXPECTED
    }
  }

  @Test
  public void testDisjointBuffers() throws Exception {
    OutputCollector collect = new OutputCollector();
    CompressionCodec codec = new ZlibCodec();
    StreamOptions options = new StreamOptions(400)
                                .withCodec(codec, codec.getDefaultOptions());
    OutStream out = new OutStream("test", options, collect);
    PositionCollector[] positions = new PositionCollector[1024];
    DataOutput stream = new DataOutputStream(out);
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      stream.writeInt(i);
    }
    out.flush();
    assertEquals(1674, collect.buffer.size());
    ByteBuffer[] inBuf = new ByteBuffer[3];
    inBuf[0] = ByteBuffer.allocate(500);
    inBuf[1] = ByteBuffer.allocate(1200);
    inBuf[2] = ByteBuffer.allocate(500);
    collect.buffer.setByteBuffer(inBuf[0], 0, 483);
    collect.buffer.setByteBuffer(inBuf[1], 483, 1625 - 483);
    collect.buffer.setByteBuffer(inBuf[2], 1625, 1674 - 1625);

    BufferChunkList buffers = new BufferChunkList();
    int offset = 0;
    for(ByteBuffer buffer: inBuf) {
      buffer.flip();
      buffers.add(new BufferChunk(buffer, offset));
      offset += buffer.remaining();
    }
    InStream.StreamOptions inOptions = InStream.options()
        .withCodec(codec).withBufferSize(400);
    InStream in = InStream.create("test", buffers.get(), 0, 1674, inOptions);
    assertEquals("compressed stream test position: 0 length: 1674 range: 0" +
                 " offset: 0 limit: 483 range 0 = 0 to 483;" +
                 "  range 1 = 483 to 1625;  range 2 = 1625 to 1674",
                 in.toString());
    DataInputStream inStream = new DataInputStream(in);
    for(int i=0; i < 1024; ++i) {
      int x = inStream.readInt();
      assertEquals(i, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i, inStream.readInt());
    }

    buffers.clear();
    buffers.add(new BufferChunk(inBuf[1], 483));
    buffers.add(new BufferChunk(inBuf[2], 1625));
    in = InStream.create("test", buffers.get(), 0, 1674, inOptions);
    inStream = new DataInputStream(in);
    positions[303].reset();
    in.seek(positions[303]);
    for(int i=303; i < 1024; ++i) {
      assertEquals(i, inStream.readInt());
    }

    buffers.clear();
    buffers.add(new BufferChunk(inBuf[0], 0));
    buffers.add(new BufferChunk(inBuf[2], 1625));
    in = InStream.create("test", buffers.get(), 0, 1674, inOptions);
    inStream = new DataInputStream(in);
    positions[1001].reset();
    for(int i=0; i < 300; ++i) {
      assertEquals(i, inStream.readInt());
    }
    in.seek(positions[1001]);
    for(int i=1001; i < 1024; ++i) {
      assertEquals(i, inStream.readInt());
    }
  }

  @Test
  public void testUncompressedDisjointBuffers() throws Exception {
    OutputCollector collect = new OutputCollector();
    OutStream out = new OutStream("test", new StreamOptions(400), collect);
    PositionCollector[] positions = new PositionCollector[1024];
    DataOutput stream = new DataOutputStream(out);
    for(int i=0; i < 1024; ++i) {
      positions[i] = new PositionCollector();
      out.getPosition(positions[i]);
      stream.writeInt(i);
    }
    out.flush();
    assertEquals(4096, collect.buffer.size());
    ByteBuffer[] inBuf = new ByteBuffer[3];
    inBuf[0] = ByteBuffer.allocate(1100);
    inBuf[1] = ByteBuffer.allocate(2200);
    inBuf[2] = ByteBuffer.allocate(1100);
    collect.buffer.setByteBuffer(inBuf[0], 0, 1024);
    collect.buffer.setByteBuffer(inBuf[1], 1024, 2048);
    collect.buffer.setByteBuffer(inBuf[2], 3072, 1024);

    for(ByteBuffer buffer: inBuf) {
      buffer.flip();
    }
    BufferChunkList buffers = new BufferChunkList();
    buffers.add(new BufferChunk(inBuf[0], 0));
    buffers.add(new BufferChunk(inBuf[1], 1024));
    buffers.add(new BufferChunk(inBuf[2], 3072));
    InStream in = InStream.create("test", buffers.get(), 0, 4096);
    assertEquals("uncompressed stream test position: 0 length: 4096" +
                 " range: 0 offset: 0 position: 0 limit: 1024",
                 in.toString());
    DataInputStream inStream = new DataInputStream(in);
    for(int i=0; i < 1024; ++i) {
      int x = inStream.readInt();
      assertEquals(i, x);
    }
    assertEquals(0, in.available());
    for(int i=1023; i >= 0; --i) {
      in.seek(positions[i]);
      assertEquals(i, inStream.readInt());
    }

    buffers.clear();
    buffers.add(new BufferChunk(inBuf[1], 1024));
    buffers.add(new BufferChunk(inBuf[2], 3072));
    in = InStream.create("test", buffers.get(), 0, 4096);
    inStream = new DataInputStream(in);
    positions[256].reset();
    in.seek(positions[256]);
    for(int i=256; i < 1024; ++i) {
      assertEquals(i, inStream.readInt());
    }

    buffers.clear();
    buffers.add(new BufferChunk(inBuf[0], 0));
    buffers.add(new BufferChunk(inBuf[2], 3072));
    in = InStream.create("test", buffers.get(), 0, 4096);
    inStream = new DataInputStream(in);
    positions[768].reset();
    for(int i=0; i < 256; ++i) {
      assertEquals(i, inStream.readInt());
    }
    in.seek(positions[768]);
    for(int i=768; i < 1024; ++i) {
      assertEquals(i, inStream.readInt());
    }
  }

  @Test
  public void testEmptyDiskRange() throws IOException {
    DiskRangeList range = new BufferChunk(ByteBuffer.allocate(0), 0);
    InStream stream = new InStream.UncompressedStream("test", range, 0, 0);
    assertEquals(0, stream.available());
    stream.seek(new PositionProvider() {
      @Override
      public long getNext() {
        return 0;
      }
    });
    assertEquals(0, stream.available());
  }
}
