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

import org.apache.hadoop.fs.FileRange;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntFunction;

public class MockDFSDataInputStream extends InputStream implements Seekable, PositionedReadable, HasEnhancedByteBufferAccess {

  private ByteBuffer hdfsBlockBuffer;
  private int startPosition;
  private int currentPosition;
  private IdentityHashMap<ByteBuffer, ByteBufferPool> bufferStore = new IdentityHashMap(0);
  // Tracks FileRange objects issued via readVectored for test assertions.
  private final List<FileRange> lastVectoredRanges = new ArrayList<>();
  // Counters distinguishing the sync (single-range short-circuit) path from
  // the readVectored path. Tests assert that batches coalescing to a single
  // range do NOT invoke readVectored.
  private int vectoredCallCount = 0;
  private int readFullyCallCount = 0;
  // When true, readVectored hands back a ByteBuffer with position() == some
  // non-zero pre-set value, mimicking filesystems that pre-slice (e.g.
  // ChecksumFileSystem). This drives the position()/remaining() code path
  // in RecordReaderUtils.
  private boolean handBackPreSlicedBuffer = false;

  public MockDFSDataInputStream(ByteBuffer hdfsBlockBuffer, int startPosition) {
    this.hdfsBlockBuffer = hdfsBlockBuffer;
    this.startPosition = startPosition;
    this.currentPosition = startPosition;
  }

  /**
   * Toggle a mode where {@link #readVectored} hands back a ByteBuffer whose
   * {@code position()} is non-zero (mimicking pre-slicing filesystems such
   * as ChecksumFileSystem). Used to exercise the position-aware slice path
   * in {@code RecordReaderUtils.readDiskRangesVectored}.
   */
  public MockDFSDataInputStream withPreSlicedBufferMode() {
    this.handBackPreSlicedBuffer = true;
    return this;
  }

  @Override
  public int read() throws IOException {
    currentPosition++;
    return hdfsBlockBuffer.get();
  }

  @Override
  public ByteBuffer read(ByteBufferPool byteBufferPool, int i, EnumSet<ReadOption> enumSet) throws IOException, UnsupportedOperationException {
    ByteBuffer copy = hdfsBlockBuffer.duplicate();
    copy.limit(copy.position() + i);
    currentPosition += i;
    hdfsBlockBuffer.position(currentPosition - startPosition);
    bufferStore.put(copy, byteBufferPool);
    return copy;
  }

  @Override
  public void releaseBuffer(ByteBuffer byteBuffer) {
    Object val = bufferStore.remove(byteBuffer);
    if (val == null) {
      throw new IllegalArgumentException("tried to release a buffer that was not created by this stream, " + byteBuffer);
    }
  }

  @Override
  public void seek(long l) throws IOException {
    currentPosition = (int) l;
    hdfsBlockBuffer.position(currentPosition - startPosition);
  }

  @Override
  public long getPos() throws IOException {
    return currentPosition;
  }

  @Override
  public boolean seekToNewSource(long l) throws IOException {
    throw new RuntimeException("unsupported");
  }

  public boolean isAllReleased() {
    return bufferStore.isEmpty();
  }

  @Override
  public int read(long l, byte[] bytes, int i, int i1) throws IOException {
    throw new RuntimeException("unsupported");
  }

  /**
   * Synchronous positioned read used by the single-range short-circuit in
   * {@code RecordReaderUtils.readDiskRangesVectored}. Copies bytes from the
   * backing buffer into {@code bytes} starting at offset {@code i}, for
   * {@code i1} bytes. Mirrors the semantics of FSDataInputStream.readFully
   * for the contract the short-circuit depends on (throws on short read).
   */
  @Override
  public void readFully(long offset, byte[] bytes, int dstArrayOffset, int length)
      throws IOException {
    readFullyCallCount++;
    int srcPos = (int) (offset - startPosition);
    if (srcPos < 0 || length < 0
        || srcPos + length > hdfsBlockBuffer.capacity()) {
      throw new IOException("readFully out of bounds: offset=" + offset
          + " length=" + length + " backing capacity=" + hdfsBlockBuffer.capacity());
    }
    ByteBuffer dup = hdfsBlockBuffer.duplicate();
    dup.position(srcPos);
    dup.limit(srcPos + length);
    dup.get(bytes, dstArrayOffset, length);
    currentPosition = srcPos + length + startPosition;
  }

  @Override
  public void readFully(long l, byte[] bytes) throws IOException {
    readFully(l, bytes, 0, bytes.length);
  }

  /**
   * Vectored read test entry point: for every FileRange in {@code ranges}
   * synchronously copy the requested bytes from the backing buffer into a
   * freshly allocated ByteBuffer (per the {@code allocate} factory) and
   * complete the FileRange's future with that buffer. Also records the ranges
   * for {@link #getLastVectoredRanges()}.
   */
  @Override
  public void readVectored(List<? extends FileRange> ranges,
                          IntFunction<ByteBuffer> allocate) throws IOException {
    vectoredCallCount++;
    lastVectoredRanges.clear();
    for (FileRange range : ranges) {
      lastVectoredRanges.add(range);
      ByteBuffer buf = allocate.apply(range.getLength());
      // Position the backing buffer at the FileRange's offset.
      int srcPos = (int) (range.getOffset() - startPosition);
      buf.put(hdfsBlockBuffer.duplicate().position(srcPos).limit(srcPos + range.getLength()));
      buf.flip();
      if (handBackPreSlicedBuffer && buf.capacity() > 1) {
        // Simulate a pre-sliced view (e.g. ChecksumFileSystem): leave some
        // bytes before position() and trim the limit. We pre-allocate a
        // larger buffer, set position to a non-zero value, then put the
        // bytes and flip.
        int pre = Math.min(buf.position(), 7);
        int extra = pre;
        ByteBuffer larger = allocate.apply(range.getLength() + extra);
        larger.position(extra);
        larger.put(buf);
        larger.flip();
        // The buffer's "data" now starts at position() == extra; the caller
        // must respect position() to find the bytes.
        buf = larger;
      }
      CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
      future.complete(buf);
      range.setData(future);
    }
  }

  /** Returns the list of FileRanges most recently passed to readVectored. */
  public List<FileRange> getLastVectoredRanges() {
    return Collections.unmodifiableList(lastVectoredRanges);
  }

  /** Number of times {@link #readVectored} has been invoked on this stream. */
  public int getVectoredCallCount() {
    return vectoredCallCount;
  }

  /** Number of times {@link #readFully(long, byte[], int, int)} has been invoked. */
  public int getReadFullyCallCount() {
    return readFullyCallCount;
  }
}
