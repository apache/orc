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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

class TestRecordReaderUtils {

  private final BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
    .range(1000, 1000)
    .range(2000, 1000)
    .range(4000, 1000)
    .range(4100, 100)
    .range(8000, 1000).build();

  private static void assertChunkEquals(BufferChunk expected, BufferChunk actual) {
    assertTrue(Objects.equals(expected, actual) &&
               expected.getOffset() == actual.getOffset() &&
               expected.getLength() == actual.getLength());
  }

  @Test
  public void testDeterminationOfSingleRead() {
    BufferChunk toChunk = RecordReaderUtils.ChunkReader.create(rangeList.get(), 0).getTo();
    assertChunkEquals(rangeList.get(1), toChunk);
    assertTrue(RecordReaderUtils.ChunkReader.create(rangeList.get(), toChunk)
                 .getExtraBytesFraction()
               < 0.001);

    toChunk = RecordReaderUtils.ChunkReader.create(rangeList.get(), 1000).getTo();
    assertChunkEquals(rangeList.get(3), toChunk);
    assertTrue(RecordReaderUtils.ChunkReader.create(rangeList.get(), toChunk)
                 .getExtraBytesFraction()
               >= .2);

    toChunk = RecordReaderUtils.ChunkReader.create(rangeList.get(), 999).getTo();
    assertChunkEquals(rangeList.get(1), toChunk);
    assertTrue(RecordReaderUtils.ChunkReader.create(rangeList.get(), toChunk)
                 .getExtraBytesFraction()
               < 0.001);
  }

  @Test
  public void testNoGapCombine() {
    BufferChunk toChunk = RecordReaderUtils.findSingleRead(rangeList.get());
    assertChunkEquals(rangeList.get(1), toChunk);
  }

  @Test
  public void testReadExtraBytes() {
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(3), chunkReader.getTo());
    populateAndValidateChunks(chunkReader, false);
  }

  @Test
  public void testRemoveBytes() {
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(3), chunkReader.getTo());
    populateAndValidateChunks(chunkReader, true);
  }

  @Test
  public void testRemoveBytesSmallerOverlapFirst() {
    BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
      .range(1000, 1000)
      .range(2000, 1000)
      .range(4000, 100)
      .range(4000, 1000)
      .range(8000, 1000).build();
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(3), chunkReader.getTo());
    populateAndValidateChunks(chunkReader, true);
  }

  @Test
  public void testRemoveBytesWithOverlap() {
    BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
      .range(1000, 1000)
      .range(1800, 400)
      .range(2000, 1000)
      .range(4000, 100)
      .range(4000, 1000)
      .range(8000, 1000).build();
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(4), chunkReader.getTo());
    populateAndValidateChunks(chunkReader, true);
  }

  @Test
  public void testExtraBytesReadWithinThreshold() {
    BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
      .range(1000, 1000)
      .range(1800, 400)
      .range(2000, 1000)
      .range(4000, 100)
      .range(4000, 1000)
      .range(8000, 1000).build();
    RecordReaderUtils.ChunkReader chunkReader =
      RecordReaderUtils.ChunkReader.create(rangeList.get(),
                                           1000);
    assertChunkEquals(rangeList.get(4), chunkReader.getTo());
    chunkReader.populateChunks(makeByteBuffer(chunkReader.getReadBytes(),
                                              chunkReader.getFrom().getOffset()),
                               false,
                               1.0);
    validateChunks(chunkReader);
    assertNotEquals(chunkReader.getReadBytes(), chunkReader.getReqBytes());
    assertEquals(chunkReader.getReadBytes(), chunkReader.getFrom().getData().array().length);
  }

  @Test
  public void testZeroCopyReadAndRelease() throws IOException {
    int blockSize = 4096;
    ByteBuffer hdfsBlockMMapBuffer = makeByteBuffer(blockSize, 0);
    int blockStartPosition = 4096;
    MockDFSDataInputStream dis = new MockDFSDataInputStream(hdfsBlockMMapBuffer, blockStartPosition);
    FSDataInputStream fis = new FSDataInputStream(dis);
    RecordReaderUtils.ByteBufferAllocatorPool pool = new RecordReaderUtils.ByteBufferAllocatorPool();
    HadoopShims.ZeroCopyReaderShim zrc = RecordReaderUtils.createZeroCopyShim(fis, null, pool);
    BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
            .range(5000, 1000)
            .range(6000, 1000)
            .range(7000, 500).build();
    RecordReaderUtils.zeroCopyReadRanges(fis, zrc, rangeList.get(0), rangeList.get(2), false);

    assertArrayEquals(Arrays.copyOfRange(hdfsBlockMMapBuffer.array(), 5000 - blockStartPosition, 5000 - blockStartPosition + 1000), byteBufferToArray(rangeList.get(0).getData()));
    assertArrayEquals(Arrays.copyOfRange(hdfsBlockMMapBuffer.array(), 6000 - blockStartPosition, 6000 - blockStartPosition + 1000), byteBufferToArray(rangeList.get(1).getData()));
    assertArrayEquals(Arrays.copyOfRange(hdfsBlockMMapBuffer.array(), 7000 - blockStartPosition, 7000 - blockStartPosition + 500), byteBufferToArray(rangeList.get(2).getData()));

    assertThrowsExactly(IllegalArgumentException.class, new Executable() {
      @Override
      public void execute() throws Throwable {
        zrc.releaseBuffer(rangeList.get(0).getData());
      }
    });

    zrc.releaseAllBuffers();

    assertTrue(dis.isAllReleased());
  }

  @Test
  public void testChunkReaderCreateOffsetExceedsMaxInt() {
    List<long[]> mockData = Arrays.asList(
      new long[]{15032282586L, 15032298848L},
      new long[]{15032298848L, 15032299844L},
      new long[]{15032299844L, 15032377804L},
      new long[]{15058260587L, 15058261632L},
      new long[]{15058261632L, 15058288409L},
      new long[]{15058288409L, 15058288862L},
      new long[]{15058339730L, 15058340775L},
      new long[]{15058340775L, 15058342439L},
      new long[]{15058449794L, 15058449982L},
      new long[]{15058449982L, 15058451700L},
      new long[]{15058451700L, 15058451749L},
      new long[]{15058484358L, 15058484422L},
      new long[]{15058484422L, 15058484862L},
      new long[]{15058484862L, 15058484878L}
    );
    TestOrcLargeStripe.RangeBuilder rangeBuilder = new TestOrcLargeStripe.RangeBuilder();
    mockData.forEach(e -> rangeBuilder.range(e[0], (int) (e[1] - e[0])));
    BufferChunkList rangeList = rangeBuilder.build();

    RecordReaderUtils.ChunkReader chunkReader = RecordReaderUtils.ChunkReader.create(rangeList.get(), 134217728);
    long readBytes = mockData.get(mockData.size() - 1)[1] - mockData.get(0)[0];
    long reqBytes = mockData.stream().mapToLong(e -> (int) (e[1] - e[0])).sum();
    assertEquals(chunkReader.getReadBytes(), readBytes);
    assertEquals(chunkReader.getReqBytes(), reqBytes);
  }

  @Test
  public void testChunkReaderCreateReqBytesAndReadBytesValidation() {
    int MAX_ARRAY_SIZE = RecordReaderUtils.MAX_ARRAY_SIZE;
    BufferChunkList rangeList = new TestOrcLargeStripe.RangeBuilder()
      .range(0, MAX_ARRAY_SIZE)
      .range(1L + MAX_ARRAY_SIZE, MAX_ARRAY_SIZE + 1)
      .range(2L * MAX_ARRAY_SIZE, MAX_ARRAY_SIZE - 4)
      .range(3L * MAX_ARRAY_SIZE, 2)
      .build();

    // reqBytes,readBytes boundary value
    RecordReaderUtils.ChunkReader chunkReader = RecordReaderUtils.ChunkReader.create(rangeList.get(0), 0);
    assertEquals(chunkReader.getReadBytes(), MAX_ARRAY_SIZE);
    assertEquals(chunkReader.getReqBytes(), MAX_ARRAY_SIZE);

    // reqBytes > MAX_ARRAY_SIZE validation
    assertThrowsExactly(IllegalArgumentException.class,
      () -> RecordReaderUtils.ChunkReader.create(rangeList.get(1), 0),
      () -> String.format("invalid reqBytes value %d,out of bounds %d",
                          rangeList.get(1).getLength(), MAX_ARRAY_SIZE)
    );

    // readBytes > MAX_ARRAY_SIZE validation
    assertThrowsExactly(IllegalArgumentException.class,
      () -> RecordReaderUtils.ChunkReader.create(rangeList.get(2), 100),
      () -> String.format("invalid readBytes value %d,out of bounds %d",
                          rangeList.get(3).getEnd() - rangeList.get(2).getOffset(), MAX_ARRAY_SIZE)
    );
  }

  private static byte[] byteBufferToArray(ByteBuffer buf) {
    byte[] resultArray = new byte[buf.remaining()];
    ByteBuffer buffer = buf.slice();
    buffer.get(resultArray);
    return resultArray;
  }

  private ByteBuffer makeByteBuffer(int length, long offset) {
    byte[] readBytes = new byte[length];
    for (int i = 0; i < readBytes.length; i++) {
      readBytes[i] = (byte) ((i + offset) % Byte.MAX_VALUE);
    }
    return ByteBuffer.wrap(readBytes);
  }

  private void populateAndValidateChunks(RecordReaderUtils.ChunkReader chunkReader,
                                         boolean withRemove) {
    if (withRemove) {
      assertTrue(chunkReader.getReadBytes() > chunkReader.getReqBytes());
    }
    ByteBuffer bytes = makeByteBuffer(chunkReader.getReadBytes(),
                                      chunkReader.getFrom().getOffset());
    if (withRemove) {
      chunkReader.populateChunksReduceSize(bytes, false);
      assertEquals(chunkReader.getReqBytes(), chunkReader.getFrom().getData().array().length);
    } else {
      chunkReader.populateChunksAsIs(bytes);
      assertEquals(chunkReader.getReadBytes(), chunkReader.getFrom().getData().array().length);
    }

    validateChunks(chunkReader);
  }

  private void validateChunks(RecordReaderUtils.ChunkReader chunkReader) {
    BufferChunk current = chunkReader.getFrom();
    while (current != chunkReader.getTo().next) {
      assertTrue(current.hasData());
      assertEquals(current.getOffset() % Byte.MAX_VALUE, current.getData().get(),
                   String.format("Failed for %s", current));
      current = (BufferChunk) current.next;
    }
  }

  // ---------------------------------------------------------------------
  // Phase 1 vectored-IO coalescing tests (ORC-2223).
  // These cover the readDiskRangesVectored path. We invoke the private
  // static method directly via reflection so we can pass our own
  // MockDFSDataInputStream that records the FileRanges it receives.
  // ---------------------------------------------------------------------

  private static void invokeReadDiskRangesVectored(FSDataInputStream in,
                                                   BufferChunkList ranges,
                                                   boolean doForceDirect)
      throws Exception {
    invokeReadDiskRangesVectored(in, ranges, doForceDirect, 0, 0.0);
  }

  private static void invokeReadDiskRangesVectored(FSDataInputStream in,
                                                   BufferChunkList ranges,
                                                   boolean doForceDirect,
                                                   int minSeekSize)
      throws Exception {
    invokeReadDiskRangesVectored(in, ranges, doForceDirect, minSeekSize, 0.0);
  }

  private static void invokeReadDiskRangesVectored(FSDataInputStream in,
                                                   BufferChunkList ranges,
                                                   boolean doForceDirect,
                                                   int minSeekSize,
                                                   double minSeekSizeTolerance)
      throws Exception {
    Method m = RecordReaderUtils.class.getDeclaredMethod(
        "readDiskRangesVectored", FSDataInputStream.class, BufferChunkList.class,
        boolean.class, int.class, double.class);
    m.setAccessible(true);
    m.invoke(null, in, ranges, doForceDirect, minSeekSize, minSeekSizeTolerance);
  }

  private static byte[] makePatternArray(int size, long baseOffset) {
    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) ((i + baseOffset) % Byte.MAX_VALUE);
    }
    return bytes;
  }

  private static MockDFSDataInputStream newBackingStream(byte[] backing) {
    return new MockDFSDataInputStream(ByteBuffer.wrap(backing), 0);
  }

  /**
   * Zero-gap adjacent ranges must be coalesced into a single FileRange and
   * each resulting BufferChunk must hold the correct slice of the underlying
   * bytes.
   */
  @Test
  public void testVectoredCoalesceAdjacentGapZero() throws Exception {
    // Backing file is 4096 bytes; pattern is byte i == (i % 127).
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 200)        // [100, 300)
        .range(300, 200)        // [300, 500)  - touches, gap = 0
        .range(500, 200).build(); // [500, 700)  - touches, gap = 0

    invokeReadDiskRangesVectored(in, ranges, false);

    // Single-range short-circuit: the batch coalesces to one FileRange so
    // readVectored is NOT invoked; instead readFully is used.
    assertEquals(0, dis.getVectoredCallCount(),
        "Single-range batch must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount(),
        "Single-range batch must invoke readFully exactly once");

    // All three BufferChunks now have data and contain the right bytes.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 100, 300),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 300, 500),
        byteBufferToArray(ranges.get(1).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 500, 700),
        byteBufferToArray(ranges.get(2).getData()));

    // Each slice must cover exactly the requested length.
    for (int i = 0; i < 3; i++) {
      assertEquals(ranges.get(i).getLength(), ranges.get(i).getData().remaining(),
                   "slice length mismatch for chunk " + i);
    }
  }

  /**
   * Overlapping ranges (gap < 0 effectively) must also coalesce without
   * expanding the FileRange beyond the union.
   */
  @Test
  public void testVectoredCoalesceOverlapping() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(200, 300)        // [200, 500)
        .range(450, 200)        // [450, 650)  - overlaps the prior end
        .range(650, 100).build(); // [650, 750)  - touches

    invokeReadDiskRangesVectored(in, ranges, false);

    // Single-range short-circuit: readVectored is NOT invoked.
    assertEquals(0, dis.getVectoredCallCount(),
        "Single-range batch must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount(),
        "Single-range batch must invoke readFully exactly once");

    assertArrayEquals(
        Arrays.copyOfRange(backing, 200, 500),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 450, 650),
        byteBufferToArray(ranges.get(1).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 650, 750),
        byteBufferToArray(ranges.get(2).getData()));
  }

  /**
   * A non-zero gap between ranges must keep them as separate FileRanges. No
   * gap over-read is permitted.
   */
  @Test
  public void testVectoredGapNotMerged() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)         // [100, 200)
        .range(400, 100)         // [400, 500) - gap of 200
        .range(700, 100).build(); // [700, 800) - gap of 200

    invokeReadDiskRangesVectored(in, ranges, false);

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(3, issued.size(), "Non-zero gap must NOT coalesce");
    assertEquals(100L, issued.get(0).getOffset());
    assertEquals(100, issued.get(0).getLength());
    assertEquals(400L, issued.get(1).getOffset());
    assertEquals(100, issued.get(1).getLength());
    assertEquals(700L, issued.get(2).getOffset());
    assertEquals(100, issued.get(2).getLength());

    for (int i = 0; i < 3; i++) {
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) ranges.get(i).getOffset(),
              (int) (ranges.get(i).getOffset() + ranges.get(i).getLength())),
          byteBufferToArray(ranges.get(i).getData()));
    }
  }

  /**
   * A BufferChunk that already has data is an immovable boundary; even if it
   * is contiguous with surrounding pending chunks it must not be merged into
   * them.
   */
  @Test
  public void testVectoredFilledChunkIsBoundary() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)
        // Pre-filled chunk: same span as the empty one would request.
        .range(400, 100)
        .range(500, 100).build();

    // Mark the middle chunk as already filled with stub bytes; coalescing must
    // never touch it.
    byte[] preFilled = makePatternArray(100, 9999);
    ranges.get(1).setChunk(ByteBuffer.wrap(preFilled));

    invokeReadDiskRangesVectored(in, ranges, false);

    List<FileRange> issued = dis.getLastVectoredRanges();
    // Two ranges: [100, 200) and [500, 600). The filled [400, 500) is its
    // own non-issued block.
    assertEquals(2, issued.size(),
                 "Filled chunk must be a hard boundary between pending ranges");
    assertEquals(100L, issued.get(0).getOffset());
    assertEquals(100, issued.get(0).getLength());
    assertEquals(500L, issued.get(1).getOffset());
    assertEquals(100, issued.get(1).getLength());

    // The pre-filled chunk must be untouched.
    assertArrayEquals(preFilled, byteBufferToArray(ranges.get(1).getData()));
    // The two pending chunks have their correct data.
    assertArrayEquals(Arrays.copyOfRange(backing, 100, 200),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(Arrays.copyOfRange(backing, 500, 600),
        byteBufferToArray(ranges.get(2).getData()));
  }

  /**
   * Mixed pattern: some ranges coalesce, others do not; ensures the loop
   * correctly handles transitions between mergeable and non-mergeable chunks.
   */
  @Test
  public void testVectoredMixedCoalesceAndGap() throws Exception {
    byte[] backing = makePatternArray(8192, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)         // group A start [100, 200)
        .range(200, 100)         // A [200, 300) merge
        .range(300, 100)         // A [300, 400) merge
        .range(800, 100)         // group B start [800, 900)
        .range(900, 100)         // B [900, 1000) merge
        .range(2000, 100).build(); // group C [2000, 2100)

    invokeReadDiskRangesVectored(in, ranges, false);

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(3, issued.size(), "Mixed pattern must produce 3 ranges");
    assertEquals(100L, issued.get(0).getOffset());
    assertEquals(300, issued.get(0).getLength());
    assertEquals(800L, issued.get(1).getOffset());
    assertEquals(200, issued.get(1).getLength());
    assertEquals(2000L, issued.get(2).getOffset());
    assertEquals(100, issued.get(2).getLength());

    for (int i = 0; i < 6; i++) {
      long off = ranges.get(i).getOffset();
      int len = ranges.get(i).getLength();
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) off, (int) (off + len)),
          byteBufferToArray(ranges.get(i).getData()));
    }
  }

  /**
   * With doForceDirect=true the allocated buffers must be direct. The same
   * coalescing and slicing behaviour must hold.
   */
  @Test
  public void testVectoredCoalesceDirectBuffer() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 200)
        .range(300, 200)
        .range(500, 200).build();

    invokeReadDiskRangesVectored(in, ranges, true);

    // Single-range short-circuit: readVectored is NOT invoked.
    assertEquals(0, dis.getVectoredCallCount(),
        "Single-range batch must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount(),
        "Single-range batch must invoke readFully exactly once");
    // Each chunk's slice must be direct and contain correct bytes.
    for (int i = 0; i < 3; i++) {
      assertTrue(ranges.get(i).getData().isDirect(),
          "Chunk slice must be direct for chunk " + i);
      long off = ranges.get(i).getOffset();
      int len = ranges.get(i).getLength();
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) off, (int) (off + len)),
          byteBufferToArray(ranges.get(i).getData()));
    }
  }

  /**
   * An empty BufferChunkList must be a no-op (no FileRange issued, no
   * exception).
   */
  @Test
  public void testVectoredEmptyRangeList() throws Exception {
    byte[] backing = makePatternArray(64, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    invokeReadDiskRangesVectored(in, new BufferChunkList(), false);
    assertTrue(dis.getLastVectoredRanges().isEmpty(),
        "Empty input must not produce any FileRange");
  }

  // ---------------------------------------------------------------------
  // Phase 1 vectored-IO: long-safe span upper bound (no giant allocation).
  //
  // We exercise the package-private coalesceRanges helper directly so we can
  // drive chunk shapes with multi-gigabyte lengths / offsets without
  // materialising a backing buffer of that size.
  // ---------------------------------------------------------------------

  private static List<RecordReaderUtils.CoalesceEntry> entries(
      long[][] offsetsAndLengths, boolean[] hasData) {
    List<RecordReaderUtils.CoalesceEntry> out =
        new java.util.ArrayList<>(offsetsAndLengths.length);
    for (int i = 0; i < offsetsAndLengths.length; i++) {
      out.add(new RecordReaderUtils.CoalesceEntry(
          offsetsAndLengths[i][0], (int) offsetsAndLengths[i][1], hasData[i]));
    }
    return out;
  }

  /** Convert a list of FileRanges to a long[][] of {offset, length}. */
  private static long[][] fileRangesAsPairs(List<FileRange> ranges) {
    long[][] pairs = new long[ranges.size()][];
    for (int i = 0; i < ranges.size(); i++) {
      pairs[i] = new long[]{ ranges.get(i).getOffset(),
                              ranges.get(i).getLength() };
    }
    return pairs;
  }

  /**
   * Two adjacent chunks whose merged span equals exactly
   * RecordReaderUtils.MAX_ARRAY_SIZE must coalesce into a single FileRange.
   */
  @Test
  public void testCoalesceRangesAtMaxArraySize() {
    int max = RecordReaderUtils.MAX_ARRAY_SIZE;
    // First chunk of length max - 100, second chunk of length 100 that
    // touches it. Merged span = max exactly.
    long[][] in = new long[][]{
        {0L, max - 100L},
        {(long) (max - 100), 100L}};
    List<FileRange> out = RecordReaderUtils.coalesceRanges(
        entries(in, new boolean[]{false, false}));
    assertEquals(1, out.size(),
        "Merged span equal to MAX_ARRAY_SIZE must stay in a single FileRange");
    assertEquals(0L, out.get(0).getOffset());
    assertEquals(max, out.get(0).getLength());
    assertArrayEquals(new long[][]{{0L, max}}, fileRangesAsPairs(out));
  }

  /**
   * Two adjacent chunks whose merged span would push the FileRange length
   * past RecordReaderUtils.MAX_ARRAY_SIZE must NOT silently truncate: the in-flight
   * range must be flushed and a new range started for the over-cap chunk.
   */
  @Test
  public void testCoalesceRangesFlushesAtMaxArraySize() {
    int max = RecordReaderUtils.MAX_ARRAY_SIZE;
    long maxL = (long) max;
    // Three adjacent small chunks. The cumulative span between chunk 1
    // and chunk 2 would push merged length past max, so chunk 2 must start
    // a fresh range. Chunk 3 is adjacent to chunk 2 and merges into R2,
    // giving R2 a final length of exactly max.
    long[][] in = new long[][]{
        {0L, maxL - 100L},
        {maxL - 100L, 200L},                       // cap flush -> new R2
        {maxL + 100L, maxL - 200L}};               // grows R2 to length max
    List<FileRange> out = RecordReaderUtils.coalesceRanges(
        entries(in, new boolean[]{false, false, false}));
    assertEquals(2, out.size(),
        "Span exceeding MAX_ARRAY_SIZE must split into multiple FileRanges");
    long[][] expected = new long[][]{
        {0L, maxL - 100L},
        {maxL - 100L, maxL}};
    assertArrayEquals(expected, fileRangesAsPairs(out));
    for (FileRange r : out) {
      assertTrue(r.getLength() > 0 && r.getLength() <= max,
          "FileRange length out of bounds: " + r.getLength());
    }
  }

  /**
   * Offsets themselves larger than Integer.MAX_VALUE must be supported: a
   * {@code long}-offset chunk followed by an adjacent chunk must coalesce
   * without overflow when computing the merged length.
   */
  @Test
  public void testCoalesceRangesWithLargeLongOffset() {
    long hugeOffset = (long) Integer.MAX_VALUE + 1_000_000L;
    long[][] in = new long[][]{
        {hugeOffset, 100L},
        {hugeOffset + 100L, 50L}};
    List<FileRange> out = RecordReaderUtils.coalesceRanges(
        entries(in, new boolean[]{false, false}));
    assertEquals(1, out.size(),
        "Adjacent chunks past Integer.MAX_VALUE must coalesce");
    assertEquals(hugeOffset, out.get(0).getOffset());
    assertEquals(150, out.get(0).getLength());
  }

  /**
   * A coalesce where adding the next chunk would push the merged length
   * past {@code MAX_FILE_RANGE_LENGTH} must flush the current range and
   * start a new one, with the cap enforced in {@code long} arithmetic.
   */
  @Test
  public void testCoalesceRangesSplitAcrossCapBoundary() {
    int max = RecordReaderUtils.MAX_ARRAY_SIZE;
    long maxL = (long) max;
    // Group A: three adjacent chunks whose total cumulative span is
    // 2*max - 50. The first three fit in a single capped span (max).
    // Chunk 4 (adjacent to chunk 3) would push the merged span past max,
    // so it must start a new range; chunk 5 then grows that second range.
    long[][] in = new long[][]{
        {0L, maxL - 100L},                                  // [0, max - 100)
        {maxL - 100L, 50L},                                 // -> [0, max - 50)
        {maxL - 50L, 50L},                                  // -> fills R1 to max
        {maxL, maxL - 100L},                                // adjacent, fits cap
        {2L * maxL - 100L, 50L}};                           // grows R2 by 50
    List<FileRange> out = RecordReaderUtils.coalesceRanges(
        entries(in, new boolean[]{false, false, false, false, false}));
    long[][] expected = new long[][]{
        {0L, maxL},
        {maxL, maxL - 50L}};
    assertArrayEquals(expected, fileRangesAsPairs(out));
    // Every emitted length must remain positive and within the cap.
    for (FileRange r : out) {
      assertTrue(r.getLength() > 0 && r.getLength() <= max,
          "FileRange length out of bounds: " + r.getLength());
    }
  }

  /**
   * Filled-chunk boundaries inside an over-cap region still split ranges
   * correctly. Verifies the boundary reset coexists with the size-cap rule.
   */
  @Test
  public void testCoalesceRangesFilledBoundaryAndCap() {
    int max = RecordReaderUtils.MAX_ARRAY_SIZE;
    long maxL = (long) max;
    // Two pending chunks that fill R1 to length max - 50, then a filled
    // boundary chunk, then two adjacent pending chunks that grow R2 from
    // initial length 200 up to 250.
    long[][] in = new long[][]{
        {0L, maxL - 100L},                                  // R1 starts
        {maxL - 100L, 50L},                                 // -> fills R1 to max-50
        {maxL - 50L, 0L},                                   // zero-length boundary chunk
        {maxL - 50L, 200L},                                 // starts R2
        {maxL + 150L, 50L}};                                // adjacent to last, grows R2
    boolean[] hasData = new boolean[]{false, false, true, false, false};
    List<FileRange> out = RecordReaderUtils.coalesceRanges(entries(in, hasData));
    long[][] expected = new long[][]{
        {0L, maxL - 50L},
        {maxL - 50L, 250L}};
    assertArrayEquals(expected, fileRangesAsPairs(out));
  }

  // ---------------------------------------------------------------------
  // Phase 1 vectored-IO: existing backfill / byte-slice correctness with
  // many small chunks per coalesced range. This exercises the monotonic
  // pointer that walks BufferChunks and fileRanges in O(chunks + ranges).
  // ---------------------------------------------------------------------

  /**
   * Many small chunks spread across multiple coalesced groups. Each chunk
   * must receive the correct byte-slice from its owning fileRange; the
   * monotonic pointer must correctly advance across groups and within a
   * group even with a long tail of chunk-per-byte-style requests.
   */
  @Test
  public void testVectoredManySmallChunksAcrossGroups() throws Exception {
    int totalLen = 8192;
    byte[] backing = makePatternArray(totalLen, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    TestOrcLargeStripe.RangeBuilder rb = new TestOrcLargeStripe.RangeBuilder();
    // Group A: 10 adjacent 16-byte chunks at [0, 16), [16, 32), ...
    for (int i = 0; i < 10; i++) {
      rb.range(i * 16L, 16);
    }
    // Group B: 10 adjacent 16-byte chunks at [1000, 1016), [1016, 1032), ...
    for (int i = 0; i < 10; i++) {
      rb.range(1000L + i * 16L, 16);
    }
    // Group C: 5 adjacent 8-byte chunks at [4000, 4008), [4008, 4016), ...
    for (int i = 0; i < 5; i++) {
      rb.range(4000L + i * 8L, 8);
    }
    BufferChunkList ranges = rb.build();

    invokeReadDiskRangesVectored(in, ranges, false);

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(3, issued.size(),
        "Three contiguous groups should yield exactly three FileRanges");
    assertEquals(0L, issued.get(0).getOffset());
    assertEquals(160, issued.get(0).getLength());
    assertEquals(1000L, issued.get(1).getOffset());
    assertEquals(160, issued.get(1).getLength());
    assertEquals(4000L, issued.get(2).getOffset());
    assertEquals(40, issued.get(2).getLength());

    // Every chunk must carry the bytes it requested.
    int total = 25;
    for (int i = 0; i < total; i++) {
      long off = ranges.get(i).getOffset();
      int len = ranges.get(i).getLength();
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) off, (int) (off + len)),
          byteBufferToArray(ranges.get(i).getData()),
          "slice mismatch for chunk " + i);
      assertEquals(len, ranges.get(i).getData().remaining(),
          "slice length mismatch for chunk " + i);
    }
  }

  /**
   * Alternating long and short chunks across a non-trivial number of
   * groups verify the backfill pointer correctly skips fileRange entries
   * between groups while still matching each chunk to the right range.
   */
  @Test
  public void testVectoredInterleavedGroupMembership() throws Exception {
    int totalLen = 8192;
    byte[] backing = makePatternArray(totalLen, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    // 4 groups, each with 3 adjacent chunks of varying length, separated by
    // gaps.
    TestOrcLargeStripe.RangeBuilder rb = new TestOrcLargeStripe.RangeBuilder();
    long base = 0L;
    int[][] lens = new int[][]{
        {50, 100, 200},       // group 1
        {30, 70, 90},         // group 2
        {200, 25, 25},        // group 3
        {10, 1000, 5}};       // group 4
    long[] gaps = new long[]{300L, 500L, 200L};  // gap between groups
    long[][] specs = new long[12][];
    int idx = 0;
    for (int g = 0; g < 4; g++) {
      long pos = base;
      for (int j = 0; j < 3; j++) {
        specs[idx++] = new long[]{pos, lens[g][j]};
        pos += lens[g][j];
      }
      if (g < 3) base = pos + gaps[g];
    }
    for (long[] s : specs) {
      rb.range(s[0], (int) s[1]);
    }
    BufferChunkList ranges = rb.build();

    invokeReadDiskRangesVectored(in, ranges, false);

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(4, issued.size(),
        "Four groups separated by gaps should yield four FileRanges");
    // Each group's emitted FileRange covers the union of its three chunks.
    int[] expectedTotalLen = new int[4];
    for (int g = 0; g < 4; g++) {
      int sum = 0;
      for (int j = 0; j < 3; j++) sum += lens[g][j];
      expectedTotalLen[g] = sum;
    }
    for (int g = 0; g < 4; g++) {
      assertEquals(specs[g * 3][0], issued.get(g).getOffset(),
          "group " + g + " offset");
      assertEquals(expectedTotalLen[g], issued.get(g).getLength(),
          "group " + g + " length");
    }

    // Every chunk must carry the bytes it requested, regardless of group.
    for (int i = 0; i < 12; i++) {
      long off = ranges.get(i).getOffset();
      int len = ranges.get(i).getLength();
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) off, (int) (off + len)),
          byteBufferToArray(ranges.get(i).getData()),
          "slice mismatch for chunk " + i);
    }
  }

  /**
   * When {@code readVectored} returns a ByteBuffer whose {@code position()}
   * is non-zero (e.g. ChecksumFileSystem hands back a pre-sliced view),
   * the per-chunk slice handed to {@link BufferChunk#setChunk} must
   * still start at {@code position() == 0} and have
   * {@code remaining() == chunk.getLength()}, and its bytes must match
   * the requested window of the file.
   */
  @Test
  public void testVectoredRespectsBufferPositionAndRemaining() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing).withPreSlicedBufferMode();
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 200)        // [100, 300)
        .range(300, 200)        // [300, 500) - coalesces with above
        .range(900, 50).build(); // [900, 950) - separate group

    invokeReadDiskRangesVectored(in, ranges, false);

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(2, issued.size());

    // Each chunk must have a clean [0, chunkLen) slice with the right bytes.
    for (int i = 0; i < 3; i++) {
      BufferChunk c = ranges.get(i);
      ByteBuffer data = c.getData();
      assertNotNull(data, "chunk " + i + " should have data");
      assertEquals(0, data.position(),
          "chunk " + i + " slice must start at position 0");
      assertEquals(c.getLength(), data.remaining(),
          "chunk " + i + " slice must have remaining() == chunk length");
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) c.getOffset(),
              (int) (c.getOffset() + c.getLength())),
          byteBufferToArray(data),
          "bytes mismatch for chunk " + i);
    }
  }

  /**
   * Regression for ORC-2223 three-chunk case: a pending chunk
   * {@code [0, 10)}, a pre-filled chunk {@code [5, 15)}, and a pending
   * chunk {@code [6, 16)}. The filled chunk is an immovable boundary so
   * coalescing emits two FileRanges: {@code [0, 10)} and {@code [6, 16)}.
   * The backfill for the third chunk must skip the first (which ends at
   * 10, before the chunk end 16) and locate the second; the filled
   * chunk's data must be left untouched.
   */
  @Test
  public void testVectoredThreeChunkFilledBoundaryRegression() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(0, 10)            // [0, 10) pending
        .range(5, 10)            // [5, 15) pre-filled (boundary)
        .range(6, 10).build();   // [6, 16) pending

    byte[] preFilled = makePatternArray(10, 9999);
    ranges.get(1).setChunk(ByteBuffer.wrap(preFilled));

    invokeReadDiskRangesVectored(in, ranges, false);

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(2, issued.size(),
        "Filled boundary must split pending chunks into two FileRanges");
    assertEquals(0L, issued.get(0).getOffset());
    assertEquals(10, issued.get(0).getLength());
    assertEquals(6L, issued.get(1).getOffset());
    assertEquals(10, issued.get(1).getLength());

    // The filled chunk's data must be unchanged.
    assertArrayEquals(preFilled, byteBufferToArray(ranges.get(1).getData()),
        "Pre-filled middle chunk must be left untouched");

    // The first pending chunk: bytes [0, 10) from the file.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 0, 10),
        byteBufferToArray(ranges.get(0).getData()),
        "First pending chunk must contain backing bytes [0, 10)");

    // The third pending chunk: bytes [6, 16) from the file.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 6, 16),
        byteBufferToArray(ranges.get(2).getData()),
        "Third pending chunk must contain backing bytes [6, 16)");

    // Each pending slice must be exactly the requested length.
    assertEquals(0, ranges.get(0).getData().position(),
        "First pending chunk slice must start at position 0");
    assertEquals(10, ranges.get(0).getData().remaining());
    assertEquals(0, ranges.get(2).getData().position(),
        "Third pending chunk slice must start at position 0");
    assertEquals(10, ranges.get(2).getData().remaining());
  }

  /**
   * Length cap rejection: a single CoalesceEntry whose length exceeds
   * {@code RecordReaderUtils.MAX_ARRAY_SIZE} must be rejected with an
   * {@link IllegalArgumentException} (no oversized FileRange must be
   * emitted). Verified against the package-private {@code coalesceRanges}
   * helper so the test does not allocate multi-GB backing buffers.
   */
  @Test
  public void testCoalesceRangesRejectsOverCapSingleEntry() {
    int overCap = RecordReaderUtils.MAX_ARRAY_SIZE + 1;
    long[][] in = new long[][]{
        {0L, (long) overCap}};
    IllegalArgumentException ex = assertThrowsExactly(
        IllegalArgumentException.class,
        () -> RecordReaderUtils.coalesceRanges(
            entries(in, new boolean[]{false})),
        "A single CoalesceEntry longer than MAX_ARRAY_SIZE must be rejected");
    assertTrue(ex.getMessage().contains(String.valueOf(overCap)),
        "Exception message must surface the offending length: " + ex.getMessage());
  }

  // ---------------------------------------------------------------------
  // Single-range short-circuit: when coalesceRanges produces exactly one
  // FileRange, readDiskRangesVectored must NOT invoke readVectored; instead
  // it must perform a synchronous positioned read and backfill the
  // BufferChunks identically. Multi-range batches must still go through
  // readVectored.
  // ---------------------------------------------------------------------

  /**
   * A batch that coalesces to a single range must NOT invoke readVectored
   * and must populate each chunk with the correct bytes via the sync path.
   */
  @Test
  public void testVectoredSingleRangeSyncShortCircuit() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 200)        // [100, 300)
        .range(300, 200)        // [300, 500)  - touches
        .range(500, 200).build(); // [500, 700)  - touches

    int vectoredBefore = dis.getVectoredCallCount();
    int readFullyBefore = dis.getReadFullyCallCount();
    invokeReadDiskRangesVectored(in, ranges, false);

    assertEquals(0, dis.getVectoredCallCount() - vectoredBefore,
        "Single-range batch must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount() - readFullyBefore,
        "Single-range batch must invoke readFully exactly once");
    assertTrue(dis.getLastVectoredRanges().isEmpty(),
        "readVectored must not record any ranges on the sync path");

    // Each chunk must hold the bytes it requested.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 100, 300),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 300, 500),
        byteBufferToArray(ranges.get(1).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 500, 700),
        byteBufferToArray(ranges.get(2).getData()));

    // Each slice must be a clean [0, chunkLen) view.
    for (int i = 0; i < 3; i++) {
      assertEquals(0, ranges.get(i).getData().position(),
          "chunk " + i + " slice must start at position 0");
      assertEquals(ranges.get(i).getLength(),
          ranges.get(i).getData().remaining(),
          "chunk " + i + " slice must have remaining() == chunk length");
      // Sync path uses heap allocation; buffers must NOT be direct.
      assertTrue(!ranges.get(i).getData().isDirect(),
          "Heap allocation path must produce non-direct slices");
    }
  }

  /**
   * Single-range short-circuit must also work with direct buffers. The
   * sync path must read into a temp byte[] then put into the direct
   * buffer (mirroring the readRanges pattern).
   */
  @Test
  public void testVectoredSingleRangeSyncShortCircuitDirect() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(50, 100)
        .range(150, 100)
        .range(250, 100).build();

    int vectoredBefore = dis.getVectoredCallCount();
    invokeReadDiskRangesVectored(in, ranges, true);

    assertEquals(0, dis.getVectoredCallCount() - vectoredBefore,
        "Single-range batch with doForceDirect=true must NOT invoke readVectored");

    for (int i = 0; i < 3; i++) {
      BufferChunk c = ranges.get(i);
      long off = c.getOffset();
      int len = c.getLength();
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) off, (int) (off + len)),
          byteBufferToArray(c.getData()),
          "bytes mismatch for chunk " + i);
      assertTrue(c.getData().isDirect(),
          "Direct path must yield direct slices for chunk " + i);
      assertEquals(0, c.getData().position());
      assertEquals(len, c.getData().remaining());
    }
  }

  /**
   * A batch that coalesces to a single range spanning multiple chunks
   * must produce correct slices for each chunk (offsets/lengths verified
   * per chunk). This is the chunk-slicing correctness case for the
   * sync path.
   */
  @Test
  public void testVectoredSingleRangeSyncSlicingCorrectness() throws Exception {
    // Build a 4-chunk batch where all four coalesce into one range.
    // Mix chunk sizes to exercise offset/limit arithmetic across the
    // shared ByteBuffer.
    int totalLen = 8192;
    byte[] backing = makePatternArray(totalLen, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(1000, 250)        // [1000, 1250)
        .range(1250, 500)        // [1250, 1750)
        .range(1750, 75)         // [1750, 1825)
        .range(1825, 1000).build(); // [1825, 2825)

    invokeReadDiskRangesVectored(in, ranges, false);

    assertEquals(0, dis.getVectoredCallCount(),
        "Single-range batch must NOT invoke readVectored");

    for (int i = 0; i < 4; i++) {
      BufferChunk c = ranges.get(i);
      long off = c.getOffset();
      int len = c.getLength();
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) off, (int) (off + len)),
          byteBufferToArray(c.getData()),
          "slice bytes mismatch for chunk " + i);
      assertEquals(0, c.getData().position(),
          "chunk " + i + " must start at position 0");
      assertEquals(len, c.getData().remaining(),
          "chunk " + i + " must have remaining() == length");
    }
  }

  /**
   * A batch with 2+ non-contiguous (gap > 0) ranges must still invoke
   * readVectored and must NOT take the sync short-circuit. readFully must
   * NOT be invoked.
   */
  @Test
  public void testVectoredMultiRangeStillUsesVectoredIO() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)        // [100, 200)
        .range(400, 100)        // [400, 500) - gap of 200 -> separate range
        .range(700, 100).build(); // [700, 800) - gap of 200 -> separate range

    int readFullyBefore = dis.getReadFullyCallCount();
    invokeReadDiskRangesVectored(in, ranges, false);

    assertEquals(1, dis.getVectoredCallCount(),
        "Multi-range batch MUST invoke readVectored exactly once");
    assertEquals(0, dis.getReadFullyCallCount() - readFullyBefore,
        "Multi-range batch must NOT invoke readFully");

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(3, issued.size(), "Non-contiguous ranges stay separate");
    assertEquals(100L, issued.get(0).getOffset());
    assertEquals(400L, issued.get(1).getOffset());
    assertEquals(700L, issued.get(2).getOffset());

    // Each chunk still receives correct bytes.
    for (int i = 0; i < 3; i++) {
      long off = ranges.get(i).getOffset();
      int len = ranges.get(i).getLength();
      assertArrayEquals(
          Arrays.copyOfRange(backing, (int) off, (int) (off + len)),
          byteBufferToArray(ranges.get(i).getData()));
    }
  }

  /**
   * A batch of 2 ranges (the boundary case for the size check) must still
   * invoke readVectored: the sync short-circuit is strictly size==1.
   */
  @Test
  public void testVectoredTwoRangeBatchStillUsesVectoredIO() throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)        // [100, 200)
        .range(400, 100)        // [400, 500) - separate range
        .build();

    invokeReadDiskRangesVectored(in, ranges, false);

    assertEquals(1, dis.getVectoredCallCount(),
        "Two-range batch MUST still use readVectored (size==1 only short-circuit)");
    assertEquals(0, dis.getReadFullyCallCount(),
        "Two-range batch must NOT use readFully");
  }

  // ---------------------------------------------------------------------
  // Phase 3 vectored-IO: bounded small-gap merge honouring minSeekSize.
  // Mirrors the sync-path semantics of findSingleRead (RecordReaderUtils:500).
  // With minSeekSize > 0, a gap of at most minSeekSize bytes is treated as
  // contiguous: the gap bytes are over-read into a single FileRange that
  // covers both chunks. With minSeekSize == 0 the behaviour is exactly the
  // Phase 1 gap==0/overlap-only merge (no extra bytes read).
  // ---------------------------------------------------------------------

  /**
   * A gap of exactly {@code minSeekSize} bytes (and any smaller gap) must
   * merge two pending chunks into a single FileRange whose span covers both
   * chunks AND the gap bytes between them. Each merged chunk must still
   * receive its exact byte window.
   */
  @Test
  public void testVectoredGapMergedWithinMinSeekSize() throws Exception {
    byte[] backing = makePatternArray(8192, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    int minSeekSize = 500;
    // First chunk ends at 200, second starts at 700 -> gap = 500 == minSeekSize.
    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)         // [100, 200)
        .range(700, 100).build(); // [700, 800) - gap of 500 = minSeekSize

    invokeReadDiskRangesVectored(in, ranges, false, minSeekSize);

    // The sync short-circuit fires because gap merge reduces two FileRanges
    // to one; readVectored must NOT be invoked.
    assertEquals(0, dis.getVectoredCallCount(),
        "Gap-merged batch coalescing to one FileRange must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount(),
        "Gap-merged single range must invoke readFully exactly once");

    // The over-read consumed [100, 800) inclusive of the 500-byte gap.
    // Each chunk must hold its exact window of the backing bytes.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 100, 200),
        byteBufferToArray(ranges.get(0).getData()),
        "First chunk must contain backing bytes [100, 200)");
    assertArrayEquals(
        Arrays.copyOfRange(backing, 700, 800),
        byteBufferToArray(ranges.get(1).getData()),
        "Second chunk must contain backing bytes [700, 800)");
    assertEquals(0, ranges.get(0).getData().position());
    assertEquals(100, ranges.get(0).getData().remaining());
    assertEquals(0, ranges.get(1).getData().position());
    assertEquals(100, ranges.get(1).getData().remaining());
  }

  /**
   * A gap strictly greater than {@code minSeekSize} must keep the chunks as
   * separate FileRanges - the over-read is bounded by minSeekSize alone.
   */
  @Test
  public void testVectoredGapNotMergedBeyondMinSeekSize() throws Exception {
    byte[] backing = makePatternArray(8192, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    int minSeekSize = 100;
    // Gap = 200 > minSeekSize=100 must remain split.
    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)         // [100, 200)
        .range(400, 100)         // [400, 500) - gap = 200 > minSeekSize
        .build();

    invokeReadDiskRangesVectored(in, ranges, false, minSeekSize);

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(1, dis.getVectoredCallCount(),
        "Split batch must still invoke readVectored");
    assertEquals(2, issued.size(),
        "Gap > minSeekSize must NOT be merged");
    assertEquals(100L, issued.get(0).getOffset());
    assertEquals(100, issued.get(0).getLength());
    assertEquals(400L, issued.get(1).getOffset());
    assertEquals(100, issued.get(1).getLength());

    assertArrayEquals(
        Arrays.copyOfRange(backing, 100, 200),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 400, 500),
        byteBufferToArray(ranges.get(1).getData()));
  }

  /**
   * With {@code minSeekSize == 0} the Phase 3 path must behave exactly like
   * Phase 1 - touching/overlap-only merge, no extra bytes read across gaps.
   * A non-zero gap must NOT be merged.
   */
  @Test
  public void testVectoredMinSeekSizeZeroMatchesPhase1() throws Exception {
    byte[] backing = makePatternArray(8192, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    // Gap of 1 byte > minSeekSize=0 must NOT merge; touching (gap==0) does.
    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)         // [100, 200)
        .range(200, 100)         // [200, 300) - touches, gap = 0 -> merges
        .range(301, 100).build(); // [301, 401) - gap = 1 > 0, stays split

    invokeReadDiskRangesVectored(in, ranges, false, 0);

    // Two FileRanges emitted (touching merge for chunks 0+1, then chunk 2
    // stays split because gap=1 > minSeekSize=0). The batch is therefore
    // multi-range; it goes through readVectored, NOT readFully. This is the
    // exact Phase 1 behaviour - just exercising the vectored path with the
    // minSeekSize=0 code path through coalesceRanges.
    assertEquals(1, dis.getVectoredCallCount(),
        "minSeekSize=0 must match Phase 1: touching merge keeps gap>0 split");
    assertEquals(0, dis.getReadFullyCallCount(),
        "Multi-range batch must NOT invoke readFully");

    List<FileRange> issued = dis.getLastVectoredRanges();
    assertEquals(2, issued.size(),
        "minSeekSize=0 must match Phase 1: exactly two FileRanges");
    assertEquals(100L, issued.get(0).getOffset());
    assertEquals(200, issued.get(0).getLength());
    assertEquals(301L, issued.get(1).getOffset());
    assertEquals(100, issued.get(1).getLength());

    // All three chunks still receive correct byte windows.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 100, 200),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 200, 300),
        byteBufferToArray(ranges.get(1).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 301, 401),
        byteBufferToArray(ranges.get(2).getData()));
  }

  /**
   * A filled chunk is an immovable boundary even when a gap inside the
   * minSeekSize tolerance exists on either side; the over-read must NOT
   * cross the filled chunk.
   */
  @Test
  public void testVectoredFilledChunkBlocksGapMerge() throws Exception {
    byte[] backing = makePatternArray(8192, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    int minSeekSize = 1000;
    // Layout:
    //   chunk 0: [100, 200) pending
    //   chunk 1: [400, 500) pre-filled (boundary)
    //   chunk 2: [700, 800) pending - gap of 200 to chunk 1, gap of 200 to chunk 2
    // minSeekSize would otherwise merge 0<->1<->2 across the gap because
    // 200 <= 1000, BUT chunk 1 is hasData() and must be an immovable boundary.
    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)
        .range(400, 100)
        .range(700, 100).build();

    byte[] preFilled = makePatternArray(100, 9999);
    ranges.get(1).setChunk(ByteBuffer.wrap(preFilled));

    invokeReadDiskRangesVectored(in, ranges, false, minSeekSize);

    List<FileRange> issued = dis.getLastVectoredRanges();
    // chunk 0 stands alone (no merge possible to the right past chunk 1
    // boundary); chunk 2 stands alone (no merge possible to the left past
    // chunk 1 boundary). Two FileRanges total, filled chunk untouched.
    assertEquals(2, issued.size(),
        "Filled chunk must remain an immovable boundary under gap merge");
    assertEquals(100L, issued.get(0).getOffset(),
        "Pre-fill must NOT be crossed: left chunk stays at offset 100");
    assertEquals(100, issued.get(0).getLength());
    assertEquals(700L, issued.get(1).getOffset(),
        "Pre-fill must NOT be crossed: right chunk stays at offset 700");
    assertEquals(100, issued.get(1).getLength());

    assertArrayEquals(preFilled, byteBufferToArray(ranges.get(1).getData()),
        "Filled chunk must remain untouched");
    assertArrayEquals(Arrays.copyOfRange(backing, 100, 200),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(Arrays.copyOfRange(backing, 700, 800),
        byteBufferToArray(ranges.get(2).getData()));
  }

  /**
   * {@link RecordReaderUtils#MAX_FILE_RANGE_LENGTH} cap still applies to
   * the gap-merged FileRange: if merging across a small gap would push the
   * resulting span past the cap, the in-flight range must be flushed and a
   * new one started for the over-cap chunk. Verified via the package-private
   * {@code coalesceRanges} helper so we can drive the inputs without
   * materialising a multi-GB backing buffer.
   */
  @Test
  public void testVectoredGapMergeRespectsMaxFileRangeLength() {
    int max = RecordReaderUtils.MAX_ARRAY_SIZE;
    long maxL = (long) max;
    int minSeekSize = 1024;
    // R1 starts as [0, max-100) (length max-100). Chunk 1 begins at max-50
    // (gap 50 <= minSeekSize) so it would normally merge; the merged span is
    // (max - 0) = max, exactly at the cap, so the merge is allowed (the cap
    // is ">", not ">="). After the merge R1 has length max. Chunk 2 is far
    // away so it starts R2.
    //
    // Then exercise the cap-flush branch: chunk 3 sits a small gap inside
    // R2's tail but merging would overflow. R2 starts as [2*max-100, 2*max)
    // (length 100). Chunk 3 begins at 2*max-50 (gap 50 <= minSeekSize).
    // Merged span would be 2*max - (2*max-100) = max, exceeding the cap, so
    // the cap-flush branch must fire and chunk 3 starts R3.
    long[][] in = new long[][]{
        {0L, maxL - 100L},                  // R1 starts at 0, ends at max-100
        {maxL - 50L, 50L},                  // gap 50 -> merges into R1, R1 length = max
        {2L * maxL + 100L, 100L},           // far away -> starts R2
        {2L * maxL + 200L, maxL - 50L}};    // gap 100 <= minSeekSize, merge would
                                            // exceed cap -> flush, starts R3
    List<FileRange> out = RecordReaderUtils.coalesceRanges(
        entries(in, new boolean[]{false, false, false, false}), minSeekSize);
    long[][] expected = new long[][]{
        {0L, maxL},
        {2L * maxL + 100L, 100L},
        {2L * maxL + 200L, maxL - 50L}};
    assertArrayEquals(expected, fileRangesAsPairs(out));
    for (FileRange r : out) {
      assertTrue(r.getLength() > 0 && r.getLength() <= max,
          "FileRange length out of bounds: " + r.getLength());
    }
  }

  /**
   * {@code coalesceRanges(entries, minSeekSize)} must reject negative
   * minSeekSize values to keep the gap-arithmetic well-defined.
   */
  @Test
  public void testCoalesceRangesRejectsNegativeMinSeekSize() {
    long[][] in = new long[][]{
        {0L, 100L},
        {200L, 100L}};
    IllegalArgumentException ex = assertThrowsExactly(
        IllegalArgumentException.class,
        () -> RecordReaderUtils.coalesceRanges(
            entries(in, new boolean[]{false, false}), -1),
        "Negative minSeekSize must be rejected");
    assertTrue(ex.getMessage().contains("minSeekSize"),
        "Exception message must mention minSeekSize: " + ex.getMessage());
  }

  // ---------------------------------------------------------------------
  // Vectored read path: minSeekSizeTolerance memory compaction parity
  // with the sync path's ChunkReader.populateChunksReduceSize semantics.
  //
  // The vectored path coalesces pending BufferChunks into one or more
  // FileRanges (with bounded small-gap merge via minSeekSize). When the
  // gap bytes included in a merged range push the extra-bytes fraction
  // (gap / reqBytes) above minSeekSizeTolerance, the over-read bytes must
  // be dropped from memory by allocating a tightly-sized buffer and
  // copying only the requested chunk bytes contiguously in chunk order.
  // ---------------------------------------------------------------------

  /**
   * When a small-gap merge produces a single FileRange whose extra-bytes
   * fraction exceeds {@code minSeekSizeTolerance}, the vectored path must
   * compact the merged buffer to exactly the requested chunk bytes. The
   * sync-path short-circuit fires (gap-merged batch coalesces to one
   * FileRange, so readVectored is NOT invoked) and the over-read must be
   * dropped from memory. Chunk contents must remain correct.
   */
  @Test
  public void testVectoredGapMergedAndCompactedWhenToleranceExceeded()
      throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    int minSeekSize = 100;
    // First chunk ends at 100, second starts at 200 -> gap = 100 == minSeekSize.
    // reqBytes = 100 + 100 = 200, rangeLength = 300, fraction = 100/200 = 0.5.
    // With tolerance = 0.0 the fraction (0.5) > 0.0 so compaction must fire.
    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(0, 100)          // [0, 100)
        .range(200, 100).build(); // [200, 300) - gap of 100 = minSeekSize

    invokeReadDiskRangesVectored(in, ranges, false, minSeekSize, 0.0);

    // Sync short-circuit: readVectored must NOT be invoked.
    assertEquals(0, dis.getVectoredCallCount(),
        "Gap-merged batch must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount(),
        "Gap-merged single range must invoke readFully exactly once");

    // Chunk contents must remain correct after compaction.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 0, 100),
        byteBufferToArray(ranges.get(0).getData()),
        "First chunk contents must be preserved across compaction");
    assertArrayEquals(
        Arrays.copyOfRange(backing, 200, 300),
        byteBufferToArray(ranges.get(1).getData()),
        "Second chunk contents must be preserved across compaction");

    // The compacted buffer holds exactly the requested chunk bytes (200 B),
    // not the 300-byte over-read range. Verify the chunk's backing buffer
    // capacity is bounded to 200, and that the two chunk slices are
    // contiguous views of a single 200-byte backing array.
    ByteBuffer c0 = ranges.get(0).getData();
    ByteBuffer c1 = ranges.get(1).getData();
    assertEquals(100, c0.capacity(),
        "Compacted chunk 0 capacity must equal its length, not the range length");
    assertEquals(100, c1.capacity(),
        "Compacted chunk 1 capacity must equal its length, not the range length");
    assertEquals(0, c0.position(),
        "Compacted chunk 0 must start at position 0");
    assertEquals(0, c1.position(),
        "Compacted chunk 1 must start at position 0");
    assertEquals(100, c0.remaining(),
        "Compacted chunk 0 must have remaining() == 100");
    assertEquals(100, c1.remaining(),
        "Compacted chunk 1 must have remaining() == 100");
    // The two slices must be backed by the same underlying array; that
    // array's length must be exactly reqBytes = 200, not the range's 300.
    assertEquals(c0.array(), c1.array(),
        "Compacted chunks must share a single backing array");
    assertEquals(200, c0.array().length,
        "Compacted backing array length must equal reqBytes (200), not the range length (300)");
  }

  /**
   * When the extra-bytes fraction is within the tolerance, the merged
   * range buffer is kept as-is: chunks remain slices of the over-read
   * range buffer (no compaction).
   */
  @Test
  public void testVectoredGapMergedButNotCompactedWithinTolerance()
      throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    int minSeekSize = 100;
    // Same layout as the compaction test: reqBytes = 200, rangeLength = 300,
    // fraction = 0.5. With tolerance = 1.0 the fraction (0.5) <= 1.0 so
    // compaction must NOT fire; chunk slices must still point into the
    // 300-byte range buffer.
    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(0, 100)
        .range(200, 100).build();

    invokeReadDiskRangesVectored(in, ranges, false, minSeekSize, 1.0);

    // Sync short-circuit (single coalesced range).
    assertEquals(0, dis.getVectoredCallCount(),
        "Gap-merged batch must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount(),
        "Gap-merged single range must invoke readFully exactly once");

    // Chunk contents must still be correct.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 0, 100),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 200, 300),
        byteBufferToArray(ranges.get(1).getData()));

    // The two slices must NOT be backed by a 200-byte compacted array;
    // they must remain slices of the 300-byte range buffer.
    ByteBuffer c0 = ranges.get(0).getData();
    ByteBuffer c1 = ranges.get(1).getData();
    assertEquals(100, c0.capacity(),
        "Non-compacted chunk 0 capacity must equal its length");
    assertEquals(100, c1.capacity(),
        "Non-compacted chunk 1 capacity must equal its length");
    assertEquals(c0.array(), c1.array(),
        "Non-compacted chunks must share the range backing array");
    assertEquals(300, c0.array().length,
        "Non-compacted backing array must be the full 300-byte range buffer, not compacted to 200");
  }

  /**
   * The compaction pass also applies on the single-range short-circuit
   * path. A coalesced single range with an internal gap (e.g. two chunks
   * merging into one range because the gap is within minSeekSize) must
   * still have its over-read dropped when the fraction exceeds tolerance.
   * readVectored is NOT invoked (the batch coalesces to one range); the
   * sync readFully path runs, then the post-read compaction pass trims
   * the over-read from memory.
   */
  @Test
  public void testVectoredSingleRangeShortCircuitIsAlsoCompacted()
      throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    int minSeekSize = 100;
    // Two chunks with a 100-byte gap that minSeekSize covers; batch
    // coalesces to one FileRange, sync short-circuit fires. With
    // tolerance = 0.0 the compaction must still fire on the single-range
    // path because (300 - 200) / 200 = 0.5 > 0.0.
    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)         // [100, 200)
        .range(300, 100).build(); // [300, 400) - gap of 100 = minSeekSize

    invokeReadDiskRangesVectored(in, ranges, false, minSeekSize, 0.0);

    assertEquals(0, dis.getVectoredCallCount(),
        "Single-range short-circuit must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount(),
        "Single-range short-circuit must invoke readFully exactly once");

    // Contents must be correct.
    assertArrayEquals(
        Arrays.copyOfRange(backing, 100, 200),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 300, 400),
        byteBufferToArray(ranges.get(1).getData()));

    // Backing array must be the 200-byte compacted buffer, not the
    // 300-byte range buffer read off disk.
    ByteBuffer c0 = ranges.get(0).getData();
    ByteBuffer c1 = ranges.get(1).getData();
    assertEquals(c0.array(), c1.array(),
        "Compacted chunks must share a single backing array");
    assertEquals(200, c0.array().length,
        "Single-range short-circuit backing array must be compacted to reqBytes (200), "
            + "not the 300-byte over-read range");
  }

  /**
   * Regression guard: existing minSeekSize=0 default (Phase 1) behaviour
   * must not change. Two adjacent chunks coalesce to a single range with
   * no over-read (reqBytes == rangeLength), so the extra-bytes fraction
   * is 0, which is <= any non-negative tolerance - compaction is a no-op
   * and chunk slices remain the original 200-byte range buffer.
   */
  @Test
  public void testVectoredToleranceCompactionNoOpOnContiguousMerge()
      throws Exception {
    byte[] backing = makePatternArray(4096, 0);
    MockDFSDataInputStream dis = newBackingStream(backing);
    FSDataInputStream in = new FSDataInputStream(dis);

    // minSeekSize = 0 (default), tolerance = 0.0; adjacent chunks touch,
    // reqBytes = rangeLength = 200, so fraction = 0 <= 0.0 and no
    // compaction should fire.
    BufferChunkList ranges = new TestOrcLargeStripe.RangeBuilder()
        .range(100, 100)
        .range(200, 100).build();

    invokeReadDiskRangesVectored(in, ranges, false, 0, 0.0);

    assertEquals(0, dis.getVectoredCallCount(),
        "Single-range batch must NOT invoke readVectored");
    assertEquals(1, dis.getReadFullyCallCount(),
        "Single-range batch must invoke readFully exactly once");

    assertArrayEquals(
        Arrays.copyOfRange(backing, 100, 200),
        byteBufferToArray(ranges.get(0).getData()));
    assertArrayEquals(
        Arrays.copyOfRange(backing, 200, 300),
        byteBufferToArray(ranges.get(1).getData()));

    // reqBytes == rangeLength so no compaction; backing array is the
    // 200-byte range buffer.
    ByteBuffer c0 = ranges.get(0).getData();
    ByteBuffer c1 = ranges.get(1).getData();
    assertEquals(c0.array(), c1.array(),
        "Contiguous-merge chunks must share a single backing array");
    assertEquals(200, c0.array().length,
        "Contiguous-merge backing array must remain the 200-byte range buffer");
  }
}
