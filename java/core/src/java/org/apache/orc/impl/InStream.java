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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;

import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionCodec;
import org.apache.orc.EncryptionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.CodedInputStream;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;

public abstract class InStream extends InputStream {

  private static final Logger LOG = LoggerFactory.getLogger(InStream.class);
  public static final int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20; // 1GB

  protected final String name;
  protected long length;

  public InStream(String name, long length) {
    this.name = name;
    this.length = length;
  }

  public String getStreamName() {
    return name;
  }

  public long getStreamLength() {
    return length;
  }

  @Override
  public abstract void close();

  static int getRangeNumber(DiskRangeList list, DiskRangeList current) {
    int result = 0;
    DiskRangeList range = list;
    while (range != null && range != current) {
      result += 1;
      range = range.next;
    }
    return result;
  }

  /**
   * Implements a stream over an uncompressed stream.
   */
  public static class UncompressedStream extends InStream {
    private DiskRangeList bytes;
    private long length;
    protected long currentOffset;
    protected ByteBuffer decrypted;
    protected DiskRangeList currentRange;

    /**
     * Create the stream without calling reset on it.
     * This is used for the subclass that needs to do more setup.
     * @param name name of the stream
     * @param length the number of bytes for the stream
     */
    public UncompressedStream(String name, long length) {
      super(name, length);
    }

    public UncompressedStream(String name,
                              DiskRangeList input,
                              long length) {
      super(name, length);
      reset(input, length);
    }

    protected void reset(DiskRangeList input, long length) {
      this.bytes = input;
      this.length = length;
      currentOffset = input == null ? 0 : input.getOffset();
      setCurrent(input, true);
    }

    @Override
    public int read() {
      if (decrypted == null || decrypted.remaining() == 0) {
        if (currentOffset == length) {
          return -1;
        }
        setCurrent(currentRange.next, false);
      }
      currentOffset += 1;
      return 0xff & decrypted.get();
    }

    protected void setCurrent(DiskRangeList newRange, boolean isJump) {
      currentRange = newRange;
      if (newRange != null) {
        decrypted = newRange.getData().slice();
        // Move the position in the ByteBuffer to match the currentOffset,
        // which is relative to the stream.
        decrypted.position((int) (currentOffset - newRange.getOffset()));
      }
    }

    @Override
    public int read(byte[] data, int offset, int length) {
      if (decrypted == null || decrypted.remaining() == 0) {
        if (currentOffset == this.length) {
          return -1;
        }
        setCurrent(currentRange.next, false);
      }
      int actualLength = Math.min(length, decrypted.remaining());
      decrypted.get(data, offset, actualLength);
      currentOffset += actualLength;
      return actualLength;
    }

    @Override
    public int available() {
      if (decrypted != null && decrypted.remaining() > 0) {
        return decrypted.remaining();
      }
      return (int) (length - currentOffset);
    }

    @Override
    public void close() {
      currentRange = null;
      currentOffset = length;
      // explicit de-ref of bytes[]
      decrypted = null;
      bytes = null;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      seek(index.getNext());
    }

    public void seek(long desired) throws IOException {
      if (desired == 0 && bytes == null) {
        return;
      }
      // If we are seeking inside of the current range, just reposition.
      if (currentRange != null && desired >= currentRange.getOffset() &&
          desired < currentRange.getEnd()) {
        decrypted.position((int) (desired - currentRange.getOffset()));
        currentOffset = desired;
      } else {
        for (DiskRangeList curRange = bytes; curRange != null;
             curRange = curRange.next) {
          if (curRange.getOffset() <= desired &&
              (curRange.next == null ? desired <= curRange.getEnd() :
                  desired < curRange.getEnd())) {
            currentOffset = desired;
            setCurrent(curRange, true);
            return;
          }
        }
        throw new IllegalArgumentException("Seek in " + name + " to " +
            desired + " is outside of the data");
      }
    }

    @Override
    public String toString() {
      return "uncompressed stream " + name + " position: " + currentOffset +
          " length: " + length + " range: " + getRangeNumber(bytes, currentRange) +
          " offset: " + (decrypted == null ? 0 : decrypted.position()) +
          " limit: " + (decrypted == null ? 0 : decrypted.limit());
    }
  }

  private static ByteBuffer allocateBuffer(int size, boolean isDirect) {
    // TODO: use the same pool as the ORC readers
    if (isDirect) {
      return ByteBuffer.allocateDirect(size);
    } else {
      return ByteBuffer.allocate(size);
    }
  }

  /**
   * Manage the state of the decryption, including the ability to seek.
   */
  static class EncryptionState {
    private final String name;
    private final EncryptionAlgorithm algorithm;
    private final Key key;
    private final byte[] iv;
    private final Cipher cipher;
    private ByteBuffer decrypted;

    EncryptionState(String name, StreamOptions options) {
      this.name = name;
      algorithm = options.algorithm;
      key = options.key;
      iv = options.iv;
      cipher = algorithm.createCipher();
    }

    /**
     * We are seeking to a new range, so update the cipher to change the IV
     * to match. This code assumes that we only support encryption in CTR mode.
     * @param offset where we are seeking to in the stream
     */
    void changeIv(long offset) {
      int blockSize = cipher.getBlockSize();
      long encryptionBlocks = offset / blockSize;
      long extra = offset % blockSize;
      byte[] advancedIv;
      if (encryptionBlocks == 0) {
        advancedIv = iv;
      } else {
        // Add the encryption blocks into the initial iv, to compensate for
        // skipping over decrypting those bytes.
        advancedIv = new byte[iv.length];
        System.arraycopy(iv, 0, advancedIv, 0, iv.length);
        int posn = iv.length - 1;
        while (encryptionBlocks > 0) {
          long sum = (advancedIv[posn] & 0xff) + encryptionBlocks;
          advancedIv[posn--] = (byte) sum;
          encryptionBlocks =  sum / 0x100;
        }
      }
      try {
        cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(advancedIv));
        // If the range starts at an offset that doesn't match the encryption
        // block, we need to advance some bytes within an encryption block.
        if (extra > 0) {
          byte[] wasted = new byte[(int) extra];
          cipher.update(wasted, 0, wasted.length, wasted, 0);
        }
      } catch (InvalidKeyException e) {
        throw new IllegalArgumentException("Invalid key on " + name, e);
      } catch (InvalidAlgorithmParameterException e) {
        throw new IllegalArgumentException("Invalid iv on " + name, e);
      } catch (ShortBufferException e) {
        throw new IllegalArgumentException("Short buffer in " + name, e);
      }
    }

    /**
     * Decrypt the given range into the decrypted buffer. It is assumed that
     * the cipher is correctly initialized by changeIv before this is called.
     * @param newRange the range to decrypte
     * @return a reused ByteBuffer, which is used by each call to decrypt
     */
    ByteBuffer decrypt(DiskRangeList newRange)  {
      final long offset = newRange.getOffset();
      final int length = newRange.getLength();
      if (decrypted == null || decrypted.capacity() < length) {
        decrypted = ByteBuffer.allocate(length);
      } else {
        decrypted.clear();
      }
      ByteBuffer encrypted = newRange.getData().duplicate();
      try {
        int output = cipher.update(encrypted, decrypted);
        if (output != length) {
          throw new IllegalArgumentException("Problem decrypting " + name +
              " at " + offset);
        }
      } catch (ShortBufferException e) {
        throw new IllegalArgumentException("Problem decrypting " + name +
            " at " + offset, e);
      }
      decrypted.flip();
      return decrypted;
    }

    void close() {
      decrypted = null;
    }
  }

  /**
   * Implements a stream over an encrypted, but uncompressed stream.
   */
  public static class EncryptedStream extends UncompressedStream {
    private final EncryptionState encrypt;

    public EncryptedStream(String name, DiskRangeList input, long length,
                           StreamOptions options) {
      super(name, length);
      encrypt = new EncryptionState(name, options);
      reset(input, length);
    }

    @Override
    protected void setCurrent(DiskRangeList newRange, boolean isJump) {
      currentRange = newRange;
      if (newRange != null) {
        if (isJump) {
          encrypt.changeIv(newRange.getOffset());
        }
        decrypted = encrypt.decrypt(newRange);
        decrypted.position((int) (currentOffset - newRange.getOffset()));
      }
    }

    @Override
    public void close() {
      super.close();
      encrypt.close();
    }

    @Override
    public String toString() {
      return "encrypted " + super.toString();
    }
  }

  private static class CompressedStream extends InStream {
    private DiskRangeList bytes;
    private final int bufferSize;
    private ByteBuffer uncompressed;
    private final CompressionCodec codec;
    protected ByteBuffer compressed;
    protected long currentOffset;
    protected DiskRangeList currentRange;
    private boolean isUncompressedOriginal;

    /**
     * Create the stream without resetting the input stream.
     * This is used in subclasses so they can finish initializing before
     * reset is called.
     * @param name the name of the stream
     * @param length the total number of bytes in the stream
     * @param options the options used to read the stream
     */
    public CompressedStream(String name,
                            long length,
                            StreamOptions options) {
      super(name, length);
      this.codec = options.codec;
      this.bufferSize = options.bufferSize;
    }

    /**
     * Create the stream and initialize the input for the stream.
     * @param name the name of the stream
     * @param input the input data
     * @param length the total length of the stream
     * @param options the options to read the data with
     */
    public CompressedStream(String name,
                            DiskRangeList input,
                            long length,
                            StreamOptions options) {
      super(name, length);
      this.codec = options.codec;
      this.bufferSize = options.bufferSize;
      reset(input, length);
    }

    /**
     * Reset the input to a new set of data.
     * @param input the input data
     * @param length the number of bytes in the stream
     */
    void reset(DiskRangeList input, long length) {
      bytes = input;
      this.length = length;
      currentOffset = input == null ? 0 : input.getOffset();
      setCurrent(input, true);
    }

    private void allocateForUncompressed(int size, boolean isDirect) {
      uncompressed = allocateBuffer(size, isDirect);
    }

    protected void setCurrent(DiskRangeList newRange,
                              boolean isJump) {
      currentRange = newRange;
      if (newRange != null) {
        compressed = newRange.getData().slice();
        compressed.position((int) (currentOffset - newRange.getOffset()));
      }
    }

    private void readHeader() throws IOException {
      if (compressed == null || compressed.remaining() <= 0) {
        setCurrent(currentRange.next, false);
      }
      if (compressed.remaining() > OutStream.HEADER_SIZE) {
        int b0 = compressed.get() & 0xff;
        int b1 = compressed.get() & 0xff;
        int b2 = compressed.get() & 0xff;
        boolean isOriginal = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >> 1);

        if (chunkLength > bufferSize) {
          throw new IllegalArgumentException("Buffer size too small. size = " +
              bufferSize + " needed = " + chunkLength);
        }
        // read 3 bytes, which should be equal to OutStream.HEADER_SIZE always
        assert OutStream.HEADER_SIZE == 3 : "The Orc HEADER_SIZE must be the same in OutStream and InStream";
        currentOffset += OutStream.HEADER_SIZE;

        ByteBuffer slice = this.slice(chunkLength);

        if (isOriginal) {
          uncompressed = slice;
          isUncompressedOriginal = true;
        } else {
          if (isUncompressedOriginal) {
            allocateForUncompressed(bufferSize, slice.isDirect());
            isUncompressedOriginal = false;
          } else if (uncompressed == null) {
            allocateForUncompressed(bufferSize, slice.isDirect());
          } else {
            uncompressed.clear();
          }
          codec.decompress(slice, uncompressed);
         }
      } else {
        throw new IllegalStateException("Can't read header at " + this);
      }
    }

    @Override
    public int read() throws IOException {
      if (!ensureUncompressed()) {
        return -1;
      }
      return 0xff & uncompressed.get();
    }

    @Override
    public int read(byte[] data, int offset, int length) throws IOException {
      if (!ensureUncompressed()) {
        return -1;
      }
      int actualLength = Math.min(length, uncompressed.remaining());
      uncompressed.get(data, offset, actualLength);
      return actualLength;
    }

    private boolean ensureUncompressed() throws IOException {
      while (uncompressed == null || uncompressed.remaining() == 0) {
        if (currentOffset == this.length) {
          return false;
        }
        readHeader();
      }
      return true;
    }

    @Override
    public int available() throws IOException {
      if (!ensureUncompressed()) {
        return 0;
      }
      return uncompressed.remaining();
    }

    @Override
    public void close() {
      uncompressed = null;
      compressed = null;
      currentRange = null;
      currentOffset = length;
      bytes = null;
    }

    @Override
    public void seek(PositionProvider index) throws IOException {
      seek(index.getNext());
      long uncompressedBytes = index.getNext();
      if (uncompressedBytes != 0) {
        readHeader();
        uncompressed.position(uncompressed.position() +
                              (int) uncompressedBytes);
      } else if (uncompressed != null) {
        // mark the uncompressed buffer as done
        uncompressed.position(uncompressed.limit());
      }
    }

    /* slices a read only contiguous buffer of chunkLength */
    private ByteBuffer slice(int chunkLength) throws IOException {
      int len = chunkLength;
      final DiskRangeList oldRange = currentRange;
      final long oldOffset = currentOffset;
      ByteBuffer slice;
      if (compressed.remaining() >= len) {
        slice = compressed.slice();
        // simple case
        slice.limit(len);
        currentOffset += len;
        compressed.position(compressed.position() + len);
        return slice;
      } else if (currentRange.next == null) {
        // nothing has been modified yet
        throw new IOException("EOF in " + this + " while trying to read " +
            chunkLength + " bytes");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format(
            "Crossing into next BufferChunk because compressed only has %d bytes (needs %d)",
            compressed.remaining(), len));
      }

      // we need to consolidate 2 or more buffers into 1
      // first copy out compressed buffers
      ByteBuffer copy = allocateBuffer(chunkLength, compressed.isDirect());
      currentOffset += compressed.remaining();
      len -= compressed.remaining();
      copy.put(compressed);

      while (currentRange.next != null) {
        setCurrent(currentRange.next, false);
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Read slow-path, >1 cross block reads with %s", this.toString()));
        }
        if (compressed.remaining() >= len) {
          slice = compressed.slice();
          slice.limit(len);
          copy.put(slice);
          currentOffset += len;
          compressed.position(compressed.position() + len);
          copy.flip();
          return copy;
        }
        currentOffset += compressed.remaining();
        len -= compressed.remaining();
        copy.put(compressed);
      }

      // restore offsets for exception clarity
      currentOffset = oldOffset;
      setCurrent(oldRange, true);
      throw new IOException("EOF in " + this + " while trying to read " +
          chunkLength + " bytes");
    }

    void seek(long desired) throws IOException {
      if (desired == 0 && bytes == null) {
        return;
      }
      for (DiskRangeList range = bytes; range != null; range = range.next) {
        if (range.getOffset() <= desired &&
            (range.next == null ? desired <= range.getEnd() :
              desired < range.getEnd())) {
          currentOffset = desired;
          setCurrent(range, true);
          return;
        }
      }
      throw new IOException("Seek outside of data in " + this + " to " + desired);
    }

    private String rangeString() {
      StringBuilder builder = new StringBuilder();
      int i = 0;
      for (DiskRangeList range = bytes; range != null; range = range.next){
        if (i != 0) {
          builder.append("; ");
        }
        builder.append(" range ");
        builder.append(i);
        builder.append(" = ");
        builder.append(range.getOffset());
        builder.append(" to ");
        builder.append(range.getEnd());
        ++i;
      }
      return builder.toString();
    }

    @Override
    public String toString() {
      return "compressed stream " + name + " position: " + currentOffset +
          " length: " + length + " range: " + getRangeNumber(bytes, currentRange) +
          " offset: " + (compressed == null ? 0 : compressed.position()) +
          " limit: " + (compressed == null ? 0 : compressed.limit()) +
          rangeString() +
          (uncompressed == null ? "" :
              " uncompressed: " + uncompressed.position() + " to " +
                  uncompressed.limit());
    }
  }

  private static class EncryptedCompressedStream extends CompressedStream {
    private final EncryptionState encrypt;

    public EncryptedCompressedStream(String name,
                                     DiskRangeList input,
                                     long length,
                                     StreamOptions options) {
      super(name, length, options);
      encrypt = new EncryptionState(name, options);
      reset(input, length);
    }

    @Override
    protected void setCurrent(DiskRangeList newRange, boolean isJump) {
      currentRange = newRange;
      if (newRange != null) {
        if (isJump) {
          encrypt.changeIv(newRange.getOffset());
        }
        compressed = encrypt.decrypt(newRange);
        compressed.position((int) (currentOffset - newRange.getOffset()));
      }
    }

    @Override
    public void close() {
      super.close();
      encrypt.close();
    }

    @Override
    public String toString() {
      return "encrypted " + super.toString();
    }
  }

  public abstract void seek(PositionProvider index) throws IOException;

  public static class StreamOptions implements Cloneable {
    private CompressionCodec codec;
    private int bufferSize;
    private EncryptionAlgorithm algorithm;
    private Key key;
    private byte[] iv;

    public StreamOptions withCodec(CompressionCodec value) {
      this.codec = value;
      return this;
    }

    public StreamOptions withBufferSize(int value) {
      bufferSize = value;
      return this;
    }

    public StreamOptions withEncryption(EncryptionAlgorithm algorithm,
                                        Key key,
                                        byte[] iv) {
      this.algorithm = algorithm;
      this.key = key;
      this.iv = iv;
      return this;
    }

    public CompressionCodec getCodec() {
      return codec;
    }

    @Override
    public StreamOptions clone() {
      try {
        StreamOptions clone = (StreamOptions) super.clone();
        if (clone.codec != null) {
          // Make sure we don't share the same codec between two readers.
          clone.codec = OrcCodecPool.getCodec(codec.getKind());
        }
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException("uncloneable", e);
      }
    }
  }

  public static StreamOptions options() {
    return new StreamOptions();
  }

  /**
   * Create an input stream from a list of disk ranges with data.
   * @param name the name of the stream
   * @param input the list of ranges of bytes for the stream; from disk or cache
   * @param length the length in bytes of the stream
   * @param options the options to read with
   * @return an input stream
   */
  public static InStream create(String name,
                                DiskRangeList input,
                                long length,
                                StreamOptions options) {
    if (options == null || options.codec == null) {
      if (options == null || options.key == null) {
        return new UncompressedStream(name, input, length);
      } else {
        return new EncryptedStream(name, input, length, options);
      }
    } else if (options.key == null) {
      return new CompressedStream(name, input, length, options);
    } else {
      return new EncryptedCompressedStream(name, input, length, options);
    }
  }

  /**
   * Create an input stream from a list of disk ranges with data.
   * @param name the name of the stream
   * @param input the list of ranges of bytes for the stream; from disk or cache
   * @param length the length in bytes of the stream
   * @return an input stream
   */
  public static InStream create(String name,
                                DiskRangeList input,
                                long length) throws IOException {
    return create(name, input, length, null);
  }

  /**
   * Creates coded input stream (used for protobuf message parsing) with higher
   * message size limit.
   *
   * @param inStream   the stream to wrap.
   * @return coded input stream
   */
  public static CodedInputStream createCodedInputStream(InStream inStream) {
    CodedInputStream codedInputStream = CodedInputStream.newInstance(inStream);
    codedInputStream.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
    return codedInputStream;
  }
}
