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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.util.VersionInfo;
import org.apache.orc.CompressionCodec;
import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Stateless methods shared between RecordReaderImpl and EncodedReaderImpl.
 * @since 1.1.0
 */
public class RecordReaderUtils {
  /**
   * The maximum size of array to allocate, value being the same as {@link java.util.Hashtable}
   */
  public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  private static final HadoopShims SHIMS = HadoopShimsFactory.get();
  private static final boolean supportVectoredIO =
      SHIMS.supportVectoredIO(VersionInfo.getVersion());
  private static final Logger LOG = LoggerFactory.getLogger(RecordReaderUtils.class);

  private static class DefaultDataReader implements DataReader {
    private FSDataInputStream file;
    private ByteBufferAllocatorPool pool;
    private HadoopShims.ZeroCopyReaderShim zcr = null;
    private final Supplier<FileSystem> fileSystemSupplier;
    private final Path path;
    private final boolean useZeroCopy;
    private final int minSeekSize;
    private final double minSeekSizeTolerance;
    private InStream.StreamOptions options;
    private boolean isOpen = false;

    private DefaultDataReader(DataReaderProperties properties) {
      this.fileSystemSupplier = properties.getFileSystemSupplier();
      this.path = properties.getPath();
      this.file = properties.getFile();
      this.useZeroCopy = properties.getZeroCopy();
      this.options = properties.getCompression();
      this.minSeekSize = properties.getMinSeekSize();
      this.minSeekSizeTolerance = properties.getMinSeekSizeTolerance();
    }

    @Override
    public void open() throws IOException {
      if (file == null) {
        this.file = fileSystemSupplier.get().open(path);
      }
      if (useZeroCopy) {
        // ZCR only uses codec for boolean checks.
        pool = new ByteBufferAllocatorPool();
        zcr = RecordReaderUtils.createZeroCopyShim(file, options.getCodec(), pool);
      } else {
        zcr = null;
      }
      isOpen = true;
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
      if (!isOpen) {
        open();
      }
      long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
      int tailLength = (int) stripe.getFooterLength();

      // read the footer
      ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
      file.readFully(offset, tailBuf.array(), tailBuf.arrayOffset(), tailLength);
      return OrcProto.StripeFooter.parseFrom(
          InStream.createCodedInputStream(InStream.create("footer",
              new BufferChunk(tailBuf, 0), 0, tailLength, options)));
    }

    @Override
    public BufferChunkList readFileData(BufferChunkList range,
                                        boolean doForceDirect
                                        ) throws IOException {
      if (supportVectoredIO && zcr == null) {
        RecordReaderUtils.readDiskRangesVectored(file, range, doForceDirect, minSeekSize,
            minSeekSizeTolerance);
      } else {
        RecordReaderUtils.readDiskRanges(file, zcr, range, doForceDirect,
                                         minSeekSize, minSeekSizeTolerance);
      }
      return range;
    }

    @Override
    public void close() throws IOException {
      if (options.getCodec() != null) {
        OrcCodecPool.returnCodec(options.getCodec().getKind(), options.getCodec());
        options.withCodec(null);
      }
      if (pool != null) {
        pool.clear();
      }
      // close both zcr and file
      try (HadoopShims.ZeroCopyReaderShim myZcr = zcr) {
        if (file != null) {
          file.close();
          file = null;
        }
      }
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return zcr != null;
    }

    /**
     * @deprecated Use {@link #releaseAllBuffers()} instead. This method was
     * incorrectly used by upper level code and shouldn't be used anymore.
     */
    @Deprecated
    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      zcr.releaseBuffer(buffer);
    }

    @Override
    public void releaseAllBuffers() {
      zcr.releaseAllBuffers();
    }

    @Override
    public DataReader clone() {
      if (this.file != null) {
        // We should really throw here, but that will cause failures in Hive.
        // While Hive uses clone, just log a warning.
        LOG.warn("Cloning an opened DataReader; the stream will be reused and closed twice");
      }
      try {
        DefaultDataReader clone = (DefaultDataReader) super.clone();
        if (options.getCodec() != null) {
          // Make sure we don't share the same codec between two readers.
          clone.options = options.clone();
        }
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException("uncloneable", e);
      }
    }

    @Override
    public InStream.StreamOptions getCompressionOptions() {
      return options;
    }
  }

  public static DataReader createDefaultDataReader(DataReaderProperties properties) {
    return new DefaultDataReader(properties);
  }

  /**
   * Does region A overlap region B? The end points are inclusive on both sides.
   * @param leftA A's left point
   * @param rightA A's right point
   * @param leftB B's left point
   * @param rightB B's right point
   * @return Does region A overlap region B?
   */
  static boolean overlap(long leftA, long rightA, long leftB, long rightB) {
    if (leftA <= leftB) {
      return rightA >= leftB;
    }
    return rightB >= leftA;
  }

  public static long estimateRgEndOffset(boolean isCompressed,
                                         int bufferSize,
                                         boolean isLast,
                                         long nextGroupOffset,
                                         long streamLength) {
    // Figure out the worst case last location
    long slop = WORST_UNCOMPRESSED_SLOP;
    // Stretch the slop by a factor to safely accommodate following compression blocks.
    // We need to calculate the maximum number of blocks(stretchFactor) by bufferSize accordingly.
    if (isCompressed) {
      int stretchFactor = 2 + (MAX_VALUES_LENGTH * MAX_BYTE_WIDTH - 1) / bufferSize;
      slop = stretchFactor * (OutStream.HEADER_SIZE + bufferSize);
    }
    return isLast ? streamLength : Math.min(streamLength, nextGroupOffset + slop);
  }

  private static final int BYTE_STREAM_POSITIONS = 1;
  private static final int RUN_LENGTH_BYTE_POSITIONS = BYTE_STREAM_POSITIONS + 1;
  private static final int BITFIELD_POSITIONS = RUN_LENGTH_BYTE_POSITIONS + 1;
  private static final int RUN_LENGTH_INT_POSITIONS = BYTE_STREAM_POSITIONS + 1;

  /**
   * Get the offset in the index positions for the column that the given
   * stream starts.
   * @param columnEncoding the encoding of the column
   * @param columnType the type of the column
   * @param streamType the kind of the stream
   * @param isCompressed is the stream compressed?
   * @param hasNulls does the column have a PRESENT stream?
   * @return the number of positions that will be used for that stream
   */
  public static int getIndexPosition(OrcProto.ColumnEncoding.Kind columnEncoding,
                              TypeDescription.Category columnType,
                              OrcProto.Stream.Kind streamType,
                              boolean isCompressed,
                              boolean hasNulls) {
    if (streamType == OrcProto.Stream.Kind.PRESENT) {
      return 0;
    }
    int compressionValue = isCompressed ? 1 : 0;
    int base = hasNulls ? (BITFIELD_POSITIONS + compressionValue) : 0;
    switch (columnType) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case STRUCT:
      case MAP:
      case LIST:
      case UNION:
        return base;
      case CHAR:
      case VARCHAR:
      case STRING:
        if (columnEncoding == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            columnEncoding == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
          return base;
        } else {
          if (streamType == OrcProto.Stream.Kind.DATA) {
            return base;
          } else {
            return base + BYTE_STREAM_POSITIONS + compressionValue;
          }
        }
      case BINARY:
      case DECIMAL:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + RUN_LENGTH_INT_POSITIONS + compressionValue;
      default:
        throw new IllegalArgumentException("Unknown type " + columnType);
    }
  }

  // for uncompressed streams, what is the most overlap with the following set
  // of rows (long vint literal group).
  static final int WORST_UNCOMPRESSED_SLOP = 2 + 8 * 512;
  // the maximum number of values that need to be consumed from the run
  static final int MAX_VALUES_LENGTH = RunLengthIntegerWriterV2.MAX_SCOPE;
  // the maximum byte width for each value
  static final int MAX_BYTE_WIDTH =
      SerializationUtils.decodeBitWidth(SerializationUtils.FixedBitSizes.SIXTYFOUR.ordinal()) / 8;

  /**
   * Is this stream part of a dictionary?
   * @return is this part of a dictionary?
   */
  public static boolean isDictionary(OrcProto.Stream.Kind kind,
                              OrcProto.ColumnEncoding encoding) {
    assert kind != OrcProto.Stream.Kind.DICTIONARY_COUNT;
    OrcProto.ColumnEncoding.Kind encodingKind = encoding.getKind();
    return kind == OrcProto.Stream.Kind.DICTIONARY_DATA ||
        (kind == OrcProto.Stream.Kind.LENGTH &&
            (encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            encodingKind == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2));
  }

  /**
   * Build a string representation of a list of disk ranges.
   * @param range ranges to stringify
   * @return the resulting string
   */
  public static String stringifyDiskRanges(DiskRangeList range) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("[");
    boolean isFirst = true;
    while (range != null) {
      if (!isFirst) {
        buffer.append(", {");
      } else {
        buffer.append("{");
      }
      isFirst = false;
      buffer.append(range);
      buffer.append("}");
      range = range.next;
    }
    buffer.append("]");
    return buffer.toString();
  }

  static long computeEnd(BufferChunk first, BufferChunk last) {
    long end = 0;
    for(BufferChunk ptr=first; ptr != last.next; ptr = (BufferChunk) ptr.next) {
      end = Math.max(ptr.getEnd(), end);
    }
    return end;
  }

  /**
   * Zero-copy read the data from the file based on a list of ranges in a
   * single read.
   *
   * As a side note, the HDFS zero copy API really sucks from a user's point of
   * view.
   *
   * @param file the file we're reading from
   * @param zcr the zero copy shim
   * @param first the first range to read
   * @param last the last range to read
   * @param allocateDirect if we need to allocate buffers, should we use direct
   * @throws IOException
   */
  static void zeroCopyReadRanges(FSDataInputStream file,
                                 HadoopShims.ZeroCopyReaderShim zcr,
                                 BufferChunk first,
                                 BufferChunk last,
                                 boolean allocateDirect) throws IOException {
    // read all of the bytes that we need
    final long offset = first.getOffset();
    int length = (int)(computeEnd(first, last) - offset);
    file.seek(offset);
    List<ByteBuffer> bytes = new ArrayList<>();
    while (length > 0) {
      ByteBuffer read= zcr.readBuffer(length, false);
      bytes.add(read);
      length -= read.remaining();
    }
    long currentOffset = offset;

    // iterate and fill each range
    BufferChunk current = first;
    Iterator<ByteBuffer> buffers = bytes.iterator();
    ByteBuffer currentBuffer = buffers.next();
    while (current != last.next) {

      // if we are past the start of the range, restart the iterator
      if (current.getOffset() < offset) {
        buffers = bytes.iterator();
        currentBuffer = buffers.next();
        currentOffset = offset;
      }

      // walk through the buffers to find the start of the buffer
      while (currentOffset + currentBuffer.remaining() <= current.getOffset()) {
        currentOffset += currentBuffer.remaining();
        // We assume that buffers.hasNext is true because we know we read
        // enough data to cover the last range.
        currentBuffer = buffers.next();
      }

      // did we get the current range in a single read?
      if (currentOffset + currentBuffer.remaining() >= current.getEnd()) {
        ByteBuffer copy = currentBuffer.slice();
        copy.position((int) (current.getOffset() - currentOffset));
        copy.limit(copy.position() + current.getLength());
        current.setChunk(copy);

      } else {
        // otherwise, build a single buffer that holds the entire range
        ByteBuffer result = allocateDirect
                              ? ByteBuffer.allocateDirect(current.getLength())
                              : ByteBuffer.allocate(current.getLength());
        // we know that the range spans buffers
        ByteBuffer copy = currentBuffer.slice();
        // skip over the front matter
        copy.position((int) (current.getOffset() - currentOffset));
        result.put(copy);
        // advance the buffer
        currentOffset += currentBuffer.remaining();
        currentBuffer = buffers.next();
        while (result.hasRemaining()) {
          if (result.remaining() > currentBuffer.remaining()) {
            result.put(currentBuffer.slice());
            currentOffset += currentBuffer.remaining();
            currentBuffer = buffers.next();
          } else {
            copy = currentBuffer.slice();
            copy.limit(result.remaining());
            result.put(copy);
          }
        }
        result.flip();
        current.setChunk(result);
      }
      current = (BufferChunk) current.next;
    }
  }

  /**
   * Read the data from the file based on a list of ranges in a single read.
   * @param file the file to read from
   * @param first the first range to read
   * @param last the last range to read
   * @param allocateDirect should we use direct buffers
   */
  static void readRanges(FSDataInputStream file,
                         BufferChunk first,
                         BufferChunk last,
                         boolean allocateDirect) throws IOException {
    // assume that the chunks are sorted by offset
    long offset = first.getOffset();
    int readSize = (int) (computeEnd(first, last) - offset);
    byte[] buffer = new byte[readSize];
    try {
      file.readFully(offset, buffer, 0, buffer.length);
    } catch(IOException e) {
      throw new IOException(String.format("Failed while reading %s %d:%d",
                                          file,
                                          offset,
                                          buffer.length),
                            e);
    }

    // get the data into a ByteBuffer
    ByteBuffer bytes;
    if (allocateDirect) {
      bytes = ByteBuffer.allocateDirect(readSize);
      bytes.put(buffer);
      bytes.flip();
    } else {
      bytes = ByteBuffer.wrap(buffer);
    }

    // populate each BufferChunks with the data
    BufferChunk current = first;
    while (current != last.next) {
      ByteBuffer currentBytes = current == last ? bytes : bytes.duplicate();
      currentBytes.position((int) (current.getOffset() - offset));
      currentBytes.limit((int) (current.getEnd() - offset));
      current.setChunk(currentBytes);
      current = (BufferChunk) current.next;
    }
  }

  /**
   * Find the list of ranges that should be read in a single read.
   * The read will stop when there is a gap, one of the ranges already has data,
   * or we have reached the maximum read size of 2^31.
   * @param first the first range to read
   * @return the last range to read
   */
  static BufferChunk findSingleRead(BufferChunk first) {
    return findSingleRead(first, 0);
  }

  /**
   * Find the list of ranges that should be read in a single read.
   * The read will stop when there is a gap, one of the ranges already has data,
   * or we have reached the maximum read size of 2^31.
   * @param first the first range to read
   * @param minSeekSize minimum size for seek instead of read
   * @return the last range to read
   */
  private static BufferChunk findSingleRead(BufferChunk first, long minSeekSize) {
    BufferChunk last = first;
    long currentEnd = first.getEnd();
    while (last.next != null &&
               !last.next.hasData() &&
               last.next.getOffset() <= (currentEnd + minSeekSize) &&
               last.next.getEnd() - first.getOffset() < Integer.MAX_VALUE) {
      last = (BufferChunk) last.next;
      currentEnd = Math.max(currentEnd, last.getEnd());
    }
    return last;
  }

  /**
   * Read the list of ranges from the file by updating each range in the list
   * with a buffer that has the bytes from the file.
   *
   * The ranges must be sorted, but may overlap or include holes.
   *
   * @param file the file to read
   * @param zcr the zero copy shim
   * @param list the disk ranges within the file to read
   * @param doForceDirect allocate direct buffers
   */
  static void readDiskRanges(FSDataInputStream file,
                             HadoopShims.ZeroCopyReaderShim zcr,
                             BufferChunkList list,
                             boolean doForceDirect) throws IOException {
    readDiskRanges(file, zcr, list, doForceDirect, 0, 0);
  }

  /**
   * Read the list of ranges from the file by updating each range in the list
   * with a buffer that has the bytes from the file.
   *
   * The ranges must be sorted, but may overlap or include holes.
   *
   * @param file the file to read
   * @param zcr the zero copy shim
   * @param list the disk ranges within the file to read
   * @param doForceDirect allocate direct buffers
   * @param minSeekSize the minimum gap to prefer seek vs read
   * @param minSeekSizeTolerance allowed tolerance for extra bytes in memory as a result of
   *                             minSeekSize
   */
  private static void readDiskRanges(FSDataInputStream file,
                             HadoopShims.ZeroCopyReaderShim zcr,
                             BufferChunkList list,
                             boolean doForceDirect,
                             int minSeekSize,
                             double minSeekSizeTolerance) throws IOException {
    BufferChunk current = list == null ? null : list.get();
    while (current != null) {
      while (current.hasData()) {
        current = (BufferChunk) current.next;
      }
      if (zcr != null) {
        BufferChunk last = findSingleRead(current);
        zeroCopyReadRanges(file, zcr, current, last, doForceDirect);
        current = (BufferChunk) last.next;
      } else {
        ChunkReader chunkReader = ChunkReader.create(current, minSeekSize);
        chunkReader.readRanges(file, doForceDirect, minSeekSizeTolerance);
        current = (BufferChunk) chunkReader.to.next;
      }
    }
  }

  /**
   * Maximum length of any single coalesced {@link FileRange} we are willing to
   * emit. Mirrors {@link #MAX_ARRAY_SIZE} so that downstream buffer
   * allocation never silently wraps to a negative int or to a smaller positive
   * value than requested.
   */
  private static final int MAX_FILE_RANGE_LENGTH = MAX_ARRAY_SIZE;

  /**
   * Snapshot of a {@link BufferChunk} sufficient to drive coalescing decisions
   * without holding an actual chunk or backing buffer. Used to test the
   * length-bounded coalescing logic without allocating multi-GB byte arrays.
   */
  static final class CoalesceEntry {
    final long offset;
    final int length;
    final boolean hasData;

    CoalesceEntry(long offset, int length, boolean hasData) {
      this.offset = offset;
      this.length = length;
      this.hasData = hasData;
    }

    long end() {
      return offset + length;
    }
  }

  /**
   * Compute the list of {@link FileRange}s that would be emitted for the given
   * ordered list of coalescing entries. Honours filled-chunk boundaries, the
   * gap==0/overlap merge rule, and the {@link #MAX_FILE_RANGE_LENGTH} size
   * cap; the merged span is computed in {@code long} arithmetic so very large
   * offsets never wrap.
   * <p>
   * Exposed package-private so unit tests can drive coalescing with controlled
   * inputs (including offsets near {@link Integer#MAX_VALUE}) without
   * allocating multi-gigabyte byte buffers.
   */
  static List<FileRange> coalesceRanges(List<CoalesceEntry> entries) {
    return coalesceRanges(entries, 0);
  }

  /**
   * Variant of {@link #coalesceRanges(List)} that additionally honours a
   * bounded small-gap merge: a pending chunk is folded into the in-flight
   * FileRange when {@code nextOffset - currentEnd <= minSeekSize}, mirroring
   * the sync-path semantics of {@link #findSingleRead}. Existing invariants
   * are preserved:
   * <ul>
   *   <li>a filled chunk ({@link CoalesceEntry#hasData hasData == true}) is an
   *       immovable boundary and is never crossed by a gap merge;</li>
   *   <li>the merged span must remain at most
   *       {@link #MAX_FILE_RANGE_LENGTH}, otherwise the in-flight range is
   *       flushed and a new one is started for the current chunk;</li>
   *   <li>single forward pass over the input, O(n);</li>
   *   <li>gap computation is performed in {@code long} arithmetic.</li>
   * </ul>
   * With {@code minSeekSize == 0} the behaviour is identical to the no-gap
   * variant (touching/overlap merge only). The vectored path bounds over-read
   * by {@code minSeekSize} alone; the on-disk read is never enlarged by the
   * {@code minSeekSizeTolerance} parameter. The tolerance is applied
   * <em>after</em> the read by {@link #readDiskRangesVectored} as a memory
   * bound: each merged range that exceeds the tolerance is compacted into
   * a tightly-sized buffer mirroring the sync-path
   * {@code ChunkReader.populateChunksReduceSize} semantics.
   *
   * @param entries     ordered list of coalescing entries
   * @param minSeekSize non-negative tolerance (in bytes) for small gaps that
   *                    may still be merged into a single FileRange
   */
  static List<FileRange> coalesceRanges(List<CoalesceEntry> entries, int minSeekSize) {
    if (minSeekSize < 0) {
      throw new IllegalArgumentException(
          "minSeekSize must be non-negative, got " + minSeekSize);
    }
    long minSeekSizeL = (long) minSeekSize;
    List<FileRange> fileRanges = new ArrayList<>();
    FileRange currentRange = null;
    long currentEnd = 0L;
    for (CoalesceEntry e : entries) {
      if (e.hasData) {
        // Already-filled chunks are an immovable boundary.
        currentRange = null;
        continue;
      }
      // Each individual pending chunk must fit inside a single FileRange:
      // FileRange lengths are int-typed and capped at MAX_FILE_RANGE_LENGTH
      // so a caller that requests more than the cap for a single chunk
      // cannot be served. Reject explicitly instead of letting the merged
      // FileRange or the downstream allocator silently overflow.
      if (e.length > MAX_FILE_RANGE_LENGTH) {
        throw new IllegalArgumentException(
            "BufferChunk length " + e.length + " exceeds MAX_ARRAY_SIZE "
                + MAX_FILE_RANGE_LENGTH + " for offset " + e.offset);
      }
      long chunkOffset = e.offset;
      long chunkEnd = e.end();
      // Merge into the in-flight range when either the chunk touches/overlaps
      // the current end (gap == 0, i.e. chunkOffset <= currentEnd) OR the
      // small gap is within the minSeekSize tolerance. Filling across a small
      // gap mirrors the sync path's findSingleRead semantics.
      boolean overlaps = (currentRange != null) && (chunkOffset <= currentEnd);
      boolean smallGap = (currentRange != null) &&
          (chunkOffset - currentEnd <= minSeekSizeL);
      if (overlaps || smallGap) {
        long newEnd = Math.max(currentEnd, chunkEnd);
        long mergedLength = newEnd - currentRange.getOffset();
        if (mergedLength > MAX_FILE_RANGE_LENGTH) {
          // Adding this chunk (including its bounded over-read) would
          // overflow the safe single-range length bound; flush and start a
          // fresh range for the current chunk.
          currentRange = FileRange.createFileRange(chunkOffset, e.length);
          fileRanges.add(currentRange);
          currentEnd = chunkEnd;
        } else {
          // Replace the in-flight range with one whose length is the final
          // merged span. Building a fresh FileRange keeps coalesceRanges free
          // of any FileRangeImpl-specific mutation; the cap above guarantees
          // the cast back to int is safe.
          long baseOffset = currentRange.getOffset();
          currentRange = FileRange.createFileRange(baseOffset, (int) mergedLength);
          fileRanges.set(fileRanges.size() - 1, currentRange);
          currentEnd = newEnd;
        }
      } else {
        // Non-contiguous (gap > minSeekSize) or first range: start a fresh
        // FileRange.
        currentRange = FileRange.createFileRange(chunkOffset, e.length);
        fileRanges.add(currentRange);
        currentEnd = chunkEnd;
      }
    }
    return fileRanges;
  }

  /**
   * Read the list of ranges from the file by updating each range in the list.
   * <p>
   * Phase 1 coalescing: ranges within the same batch that are contiguous or
   * overlapping (offset &le; current aggregated end, i.e. gap = 0) are merged
   * into a single {@link FileRange}. Already-filled chunks
   * ({@link BufferChunk#hasData()} returns true) are never crossed - they act
   * as a boundary. There is no over-read across gaps, and coalescing never
   * spans past the current batch because we iterate the list once in order
   * and only look at the immediately preceding pending chunk.
   * <p>
   * Phase 3 coalescing: in addition to Phase 1, a bounded small-gap merge is
   * honoured - a chunk is folded into the in-flight FileRange when
   * {@code nextOffset - currentEnd <= minSeekSize}. This mirrors the sync
   * path's {@link #findSingleRead} rule so callers using vectored I/O do not
   * regress on minSeekSize-bounded workloads (e.g. Hadoop's TestMinSeekSize
   * which expects ~106% reads with minSeekSize=1MB). The tolerance is
   * applied in {@code long} arithmetic; filled-chunk boundaries and the
   * {@link #MAX_FILE_RANGE_LENGTH} cap remain hard limits. The on-disk read
   * is bounded by {@code minSeekSize} alone; the {@code minSeekSizeTolerance}
   * parameter is applied as a post-read memory bound: for every merged
   * range whose extra-bytes fraction (gap bytes / requested bytes) exceeds
   * the tolerance, the backing buffer is compacted into a tightly-sized
   * buffer that holds only the requested chunk bytes - mirroring the sync
   * path's {@code ChunkReader.populateChunksReduceSize} semantics. With
   * {@code minSeekSize == 0} the behaviour is exactly the Phase 1
   * gap==0 / overlap-only merge.
   * <p>
   * The merged span is computed in {@code long} arithmetic; if growing the
   * in-flight range would push its length past
   * {@link #MAX_FILE_RANGE_LENGTH} the range is flushed and a new one is
   * started for the current chunk. This avoids the silent {@code (int)} wrap
   * that would otherwise produce a negative or truncated FileRange when caller
   * offsets approach or exceed {@link Integer#MAX_VALUE}.
   *
   * @param fileInputStream       the input stream to read from
   * @param range                 the ordered list of disk ranges within the
   *                              file to read
   * @param doForceDirect         allocate direct buffers
   * @param minSeekSize           non-negative minimum gap (in bytes) that is
   *                              still merged into a single FileRange.
   *                              {@code 0} disables the bounded gap merge
   *                              and restores the Phase 1 behaviour exactly.
   * @param minSeekSizeTolerance  non-negative extra-bytes fraction above
   *                              which a merged range's buffer is compacted
   *                              to exactly the requested chunk bytes. The
   *                              compaction is a post-read memory bound only;
   *                              it does NOT change how many bytes are read
   *                              from disk (the read is bounded by
   *                              {@code minSeekSize}). Mirrors the sync
   *                              path's {@code ChunkReader.populateChunks}
   *                              semantics so a vectored reader pays no
   *                              extra memory for small-gap merges.
   */
  private static void readDiskRangesVectored(
      FSDataInputStream fileInputStream,
      BufferChunkList range,
      boolean doForceDirect,
      int minSeekSize,
      double minSeekSizeTolerance) throws IOException {
    if (range == null) return;

    IntFunction<ByteBuffer> allocate =
        doForceDirect ? ByteBuffer::allocateDirect : ByteBuffer::allocate;

    // Build the snapshot of BufferChunks we want to coalesce.
    List<CoalesceEntry> entries = new ArrayList<>();
    for (BufferChunk c = range.get(); c != null; c = (BufferChunk) c.next) {
      entries.add(new CoalesceEntry(c.getOffset(), c.getLength(), c.hasData()));
    }
    List<FileRange> fileRanges = coalesceRanges(entries, minSeekSize);

    // Single-range short-circuit: a single coalesced range whose data the
    // caller will block on immediately is pure async overhead when routed
    // through readVectored (executor task + CompletableFuture with no
    // parallelism benefit). Read it synchronously via the positioned
    // readFully API and hand the backfill loop a FileRange whose future is
    // already completed. The backfill/slice logic below is source-agnostic
    // and works on either path.
    if (fileRanges.size() == 1) {
      FileRange only = fileRanges.get(0);
      long onlyOffset = only.getOffset();
      int onlyLength = only.getLength();
      ByteBuffer full = allocate.apply(onlyLength);
      try {
        if (doForceDirect) {
          // Direct buffers cannot be backed by readFully(long, ByteBuffer,
          // ...). Read into a heap byte[] then copy into the direct buffer,
          // mirroring the pattern used by readRanges().
          byte[] tmp = new byte[onlyLength];
          fileInputStream.readFully(onlyOffset, tmp, 0, onlyLength);
          full.put(tmp);
          full.flip();
        } else {
          // Heap-backed ByteBuffer: readFully can target its backing array
          // directly when there is no offset (allocate(...) returns a
          // position-0, arrayOffset-0 buffer).
          fileInputStream.readFully(onlyOffset, full.array(), full.arrayOffset(),
              onlyLength);
          full.limit(onlyLength);
        }
      } catch (IOException e) {
        throw new IOException(String.format(
            "Failed while reading %s %d:%d", fileInputStream, onlyOffset, onlyLength), e);
      }
      CompletableFuture<ByteBuffer> done = new CompletableFuture<>();
      done.complete(full);
      only.setData(done);
    } else {
      fileInputStream.readVectored(fileRanges, allocate);
    }

    // Walk the original BufferChunk list and, for every empty chunk, slice the
    // coalesced FileRange's buffer to the chunk's exact [offset, end).
    //
    // BufferChunks are visited in their original sorted order; fileRanges is
    // also strictly increasing in offset. We use a single forward-moving
    // pointer so the total backfill cost is O(chunks + ranges) instead of
    // O(chunks * ranges). We also remember which BufferChunks each FileRange
    // owns (in chunk order) so the post-slicing compaction pass can decide
    // per-range whether to drop the over-read bytes from memory.
    List<List<BufferChunk>> chunksByRange = new java.util.ArrayList<>(fileRanges.size());
    for (int i = 0; i < fileRanges.size(); i++) {
      chunksByRange.add(new java.util.ArrayList<>());
    }
    BufferChunk cur = range.get();
    int rangeIdx = 0;
    while (cur != null) {
      if (!cur.hasData()) {
        long chunkOffset = cur.getOffset();
        long chunkEnd = chunkOffset + cur.getLength();
        // Skip past any fileRange that ends at or before the chunk start.
        while (rangeIdx < fileRanges.size() &&
            (long) fileRanges.get(rangeIdx).getOffset() +
                fileRanges.get(rangeIdx).getLength() <= chunkOffset) {
          rangeIdx++;
        }
        // The current range may start at or before chunkOffset but end before
        // chunkEnd (e.g. a pending chunk on each side of an already-filled
        // boundary chunk whose spans overlap the pending ones). In that case
        // the range cannot own the chunk; keep scanning forward for a later
        // range whose start is still <= chunkOffset and whose end reaches
        // chunkEnd. Because ranges are sorted by offset and non-overlapping,
        // any such later range is the unique owner. rangeIdx only moves
        // forward, so the total work across all chunks stays O(chunks +
        // ranges).
        FileRange owner = null;
        int ownerIdx = -1;
        while (rangeIdx < fileRanges.size() &&
            fileRanges.get(rangeIdx).getOffset() <= chunkOffset) {
          FileRange candidate = fileRanges.get(rangeIdx);
          if ((long) candidate.getOffset() + candidate.getLength() >= chunkEnd) {
            owner = candidate;
            ownerIdx = rangeIdx;
            break;
          }
          rangeIdx++;
        }
        if (owner == null) {
          throw new IllegalStateException("No FileRange covers BufferChunk " + cur);
        }
        try {
          ByteBuffer full = owner.getData().get();
          // readVectored may return a buffer whose position() is non-zero
          // (e.g. ChecksumFileSystem hands back a pre-sliced view). Compute
          // the absolute slice window using both the buffer's position and
          // the in-range offset, and reset the slice's position to 0 so
          // consumers see a clean [0, chunkLen) view of the chunk bytes.
          int absPos = full.position() + (int) (chunkOffset - owner.getOffset());
          int absLimit = absPos + cur.getLength();
          if (absLimit > full.limit()) {
            throw new IllegalStateException("ByteBuffer underflow for BufferChunk " + cur
                + ": requested limit " + absLimit + " > buffer limit " + full.limit());
          }
          ByteBuffer slice = full.duplicate();
          slice.position(absPos);
          slice.limit(absLimit);
          // Hand the consumer a chunk-sized view starting at 0.
          slice = slice.slice();
          cur.setChunk(slice);
          chunksByRange.get(ownerIdx).add(cur);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
      cur = (BufferChunk) cur.next;
    }

    // Post-read memory compaction. For each merged FileRange, compute the
    // extra-bytes fraction as (rangeLength - reqBytes) / reqBytes. If the
    // fraction exceeds minSeekSizeTolerance, the over-read gap bytes are
    // dropped from memory by allocating a tightly-sized buffer and copying
    // the requested chunk bytes contiguously in chunk order; each owning
    // BufferChunk is then handed a slice of that compacted buffer. This
    // mirrors the sync path's ChunkReader.populateChunksReduceSize
    // semantics so a vectored reader pays no extra memory for small-gap
    // merges. Ranges with reqBytes == 0 cannot be compacted; ranges that
    // own exactly one chunk have no gap bytes and are skipped cheaply.
    for (int i = 0; i < fileRanges.size(); i++) {
      FileRange range_i = fileRanges.get(i);
      List<BufferChunk> owned = chunksByRange.get(i);
      if (owned.size() <= 1) {
        continue;
      }
      long reqBytesL = 0L;
      for (BufferChunk c : owned) {
        reqBytesL += c.getLength();
      }
      if (reqBytesL <= 0L) {
        continue;
      }
      long rangeLength = range_i.getLength();
      long extraBytesFractionNum = rangeLength - reqBytesL;
      if (extraBytesFractionNum <= 0L) {
        continue;
      }
      double extraBytesFraction = extraBytesFractionNum / (double) reqBytesL;
      if (extraBytesFraction <= minSeekSizeTolerance) {
        continue;
      }
      int reqBytes = (int) reqBytesL;
      ByteBuffer compacted;
      if (doForceDirect) {
        compacted = ByteBuffer.allocateDirect(reqBytes);
      } else {
        compacted = ByteBuffer.allocate(reqBytes);
      }
      for (BufferChunk c : owned) {
        // Copy exactly c.getLength() bytes from the chunk's existing slice
        // (which currently points into the full range buffer) into the next
        // position in the compacted buffer.
        ByteBuffer src = c.getData();
        int copyLen = c.getLength();
        compacted.put(src.duplicate().position(0).limit(copyLen));
      }
      compacted.flip();
      // Re-slice each BufferChunk to its window in the compacted buffer.
      int copyStart = 0;
      for (BufferChunk c : owned) {
        int copyLen = c.getLength();
        ByteBuffer slice = compacted.duplicate();
        slice.position(copyStart);
        slice.limit(copyStart + copyLen);
        c.setChunk(slice.slice());
        copyStart += copyLen;
        assert copyStart <= reqBytes;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Vectored compaction: range [{}, {}), reqBytes={}, extraFraction={}, "
                + "tolerance={}", range_i.getOffset(),
            range_i.getOffset() + rangeLength, reqBytes, extraBytesFraction,
            minSeekSizeTolerance);
      }
    }
  }

  static HadoopShims.ZeroCopyReaderShim createZeroCopyShim(FSDataInputStream file,
      CompressionCodec codec, ByteBufferAllocatorPool pool) throws IOException {
    if ((codec == null || ((codec instanceof DirectDecompressionCodec) &&
        ((DirectDecompressionCodec) codec).isAvailable()))) {
      /* codec is null or is available */
      return SHIMS.getZeroCopyReader(file, pool);
    }
    return null;
  }

  // this is an implementation copied from ElasticByteBufferPool in hadoop-2,
  // which lacks a clear()/clean() operation
  public static final class ByteBufferAllocatorPool implements HadoopShims.ByteBufferPoolShim {
    private static final class Key implements Comparable<Key> {
      private final int capacity;
      private final long insertionGeneration;

      Key(int capacity, long insertionGeneration) {
        this.capacity = capacity;
        this.insertionGeneration = insertionGeneration;
      }

      @Override
      public int compareTo(Key other) {
        final int c = Integer.compare(capacity, other.capacity);
        return (c != 0) ? c : Long.compare(insertionGeneration, other.insertionGeneration);
      }

      @Override
      public boolean equals(Object rhs) {
        if (rhs instanceof Key o) {
          return 0 == compareTo(o);
        }
        return false;
      }

      @Override
      public int hashCode() {
        // This is idential to the previous hashCode from HashCodeBuilder
        return (17 * 37 + capacity) * 37 + (int) (insertionGeneration ^ insertionGeneration >> 32);
      }
    }

    private final TreeMap<Key, ByteBuffer> buffers = new TreeMap<>();

    private final TreeMap<Key, ByteBuffer> directBuffers = new TreeMap<>();

    private long currentGeneration = 0;

    private TreeMap<Key, ByteBuffer> getBufferTree(boolean direct) {
      return direct ? directBuffers : buffers;
    }

    public void clear() {
      buffers.clear();
      directBuffers.clear();
    }

    @Override
    public ByteBuffer getBuffer(boolean direct, int length) {
      TreeMap<Key, ByteBuffer> tree = getBufferTree(direct);
      Map.Entry<Key, ByteBuffer> entry = tree.ceilingEntry(new Key(length, 0));
      if (entry == null) {
        return direct ? ByteBuffer.allocateDirect(length) : ByteBuffer
            .allocate(length);
      }
      tree.remove(entry.getKey());
      return entry.getValue();
    }

    @Override
    public void putBuffer(ByteBuffer buffer) {
      TreeMap<Key, ByteBuffer> tree = getBufferTree(buffer.isDirect());
      Key key;

      // Buffers are indexed by (capacity, generation).
      // If our key is not unique on the first try, try again
      do {
        key = new Key(buffer.capacity(), currentGeneration++);
      } while (tree.putIfAbsent(key, buffer) != null);
    }
  }

  static class ChunkReader {
    private final BufferChunk from;
    private final BufferChunk to;
    private final int readBytes;
    private final int reqBytes;

    private ChunkReader(BufferChunk from, BufferChunk to, int readSize, int reqBytes) {
      this.from = from;
      this.to = to;
      this.readBytes = readSize;
      this.reqBytes = reqBytes;
    }

    double getExtraBytesFraction() {
      return (readBytes - reqBytes) / ((double) reqBytes);
    }

    public int getReadBytes() {
      return readBytes;
    }

    public int getReqBytes() {
      return reqBytes;
    }

    public BufferChunk getFrom() {
      return from;
    }

    public BufferChunk getTo() {
      return to;
    }

    void populateChunks(ByteBuffer bytes, boolean allocateDirect, double extraByteTolerance) {
      if (getExtraBytesFraction() > extraByteTolerance) {
        LOG.debug("ExtraBytesFraction = {}, ExtraByteTolerance = {}, reducing memory size",
                  getExtraBytesFraction(),
                  extraByteTolerance);
        populateChunksReduceSize(bytes, allocateDirect);
      } else {
        LOG.debug("ExtraBytesFraction = {}, ExtraByteTolerance = {}, populating as is",
                  getExtraBytesFraction(),
                  extraByteTolerance);
        populateChunksAsIs(bytes);
      }
    }

    void populateChunksAsIs(ByteBuffer bytes) {
      // populate each BufferChunks with the data
      BufferChunk current = from;
      long offset = from.getOffset();
      while (current != to.next) {
        ByteBuffer currentBytes = current == to ? bytes : bytes.duplicate();
        currentBytes.position((int) (current.getOffset() - offset));
        currentBytes.limit((int) (current.getEnd() - offset));
        current.setChunk(currentBytes);
        current = (BufferChunk) current.next;
      }
    }

    void populateChunksReduceSize(ByteBuffer bytes, boolean allocateDirect) {
      ByteBuffer newBuffer;
      if (allocateDirect) {
        newBuffer = ByteBuffer.allocateDirect(reqBytes);
        newBuffer.position(reqBytes);
        newBuffer.flip();
      } else {
        byte[] newBytes = new byte[reqBytes];
        newBuffer = ByteBuffer.wrap(newBytes);
      }

      final long offset = from.getOffset();
      int copyStart = 0;
      int copyEnd;
      int copyLength;
      int skippedBytes = 0;
      int srcPosition;
      BufferChunk current = from;
      while (current != to.next) {
        // We can skip bytes as required, but no need to copy bytes that are already copied
        srcPosition = (int) (current.getOffset() - offset);
        skippedBytes += Math.max(0, srcPosition - copyStart);
        copyStart = Math.max(copyStart, srcPosition);
        copyEnd = (int) (current.getEnd() - offset);
        copyLength = copyStart < copyEnd ? copyEnd - copyStart : 0;
        newBuffer.put(bytes.array(), copyStart, copyLength);
        copyStart += copyLength;
        // Set up new ByteBuffer that wraps on the same backing array
        ByteBuffer currentBytes = current == to ? newBuffer : newBuffer.duplicate();
        currentBytes.position(srcPosition - skippedBytes);
        currentBytes.limit(currentBytes.position() + current.getLength());
        current.setChunk(currentBytes);
        current = (BufferChunk) current.next;
      }
    }

    /**
     * Read the data from the file based on a list of ranges in a single read.
     *
     * @param file           the file to read from
     * @param allocateDirect should we use direct buffers
     */
    void readRanges(FSDataInputStream file, boolean allocateDirect, double extraByteTolerance)
      throws IOException {
      // assume that the chunks are sorted by offset
      long offset = from.getOffset();
      int readSize = (int) (computeEnd(from, to) - offset);
      byte[] buffer = new byte[readSize];
      try {
        file.readFully(offset, buffer, 0, buffer.length);
      } catch (IOException e) {
        throw new IOException(String.format("Failed while reading %s %d:%d",
                                            file,
                                            offset,
                                            buffer.length),
                              e);
      }

      // get the data into a ByteBuffer
      ByteBuffer bytes;
      if (allocateDirect) {
        bytes = ByteBuffer.allocateDirect(readSize);
        bytes.put(buffer);
        bytes.flip();
      } else {
        bytes = ByteBuffer.wrap(buffer);
      }

      // populate each BufferChunks with the data
      populateChunks(bytes, allocateDirect, extraByteTolerance);
    }

    static ChunkReader create(BufferChunk from, BufferChunk to) {
      long start = Long.MAX_VALUE;
      long end = Long.MIN_VALUE;

      long currentStart = Long.MAX_VALUE;
      long currentEnd = Long.MIN_VALUE;
      long reqBytes = 0L;

      BufferChunk current = from;
      while (current != to.next) {
        start = Math.min(start, current.getOffset());
        end = Math.max(end, current.getEnd());
        if (currentEnd == Long.MIN_VALUE || current.getOffset() <= currentEnd) {
          currentStart = Math.min(currentStart, current.getOffset());
          currentEnd = Math.max(currentEnd, current.getEnd());
        } else {
          reqBytes += currentEnd - currentStart;
          currentStart = current.getOffset();
          currentEnd = current.getEnd();
        }
        current = (BufferChunk) current.next;
      }
      reqBytes += currentEnd - currentStart;
      if (reqBytes > MAX_ARRAY_SIZE) {
        throw new IllegalArgumentException("invalid reqBytes value " + reqBytes + ",out of bounds " + MAX_ARRAY_SIZE);
      }
      long readBytes = end - start;
      if (readBytes > MAX_ARRAY_SIZE) {
        throw new IllegalArgumentException("invalid readBytes value " + readBytes + ",out of bounds " + MAX_ARRAY_SIZE);
      }
      return new ChunkReader(from, to, (int) readBytes, (int) reqBytes);
    }

    static ChunkReader create(BufferChunk from, int minSeekSize) {
      BufferChunk to = findSingleRead(from, minSeekSize);
      return create(from, to);
    }
  }
}
