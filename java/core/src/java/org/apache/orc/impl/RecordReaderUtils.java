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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;

import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;

/**
 * Stateless methods shared between RecordReaderImpl and EncodedReaderImpl.
 */
public class RecordReaderUtils {
  private static final HadoopShims SHIMS = HadoopShims.Factory.get();

  static boolean hadBadBloomFilters(TypeDescription.Category category,
                                    OrcFile.WriterVersion version) {
    switch(category) {
      case STRING:
      case CHAR:
      case VARCHAR:
        return !version.includes(OrcFile.WriterVersion.HIVE_12055);
      case DECIMAL:
        return true;
      case TIMESTAMP:
        return !version.includes(OrcFile.WriterVersion.ORC_135);
      default:
        return false;
    }
  }

  /**
   * Plans the list of disk ranges that the given stripe needs to read the
   * indexes. All of the positions are relative to the start of the stripe.
   * @param  fileSchema the schema for the file
   * @param footer the stripe footer
   * @param ignoreNonUtf8BloomFilter should the reader ignore non-utf8
   *                                 encoded bloom filters
   * @param fileIncluded the columns (indexed by file columns) that should be
   *                     read
   * @param sargColumns true for the columns (indexed by file columns) that
   *                    we need bloom filters for
   * @param version the version of the software that wrote the file
   * @param bloomFilterKinds (output) the stream kind of the bloom filters
   * @return a list of merged disk ranges to read
   */
  static DiskRangeList planIndexReading(TypeDescription fileSchema,
                                        OrcProto.StripeFooter footer,
                                        boolean ignoreNonUtf8BloomFilter,
                                        boolean[] fileIncluded,
                                        boolean[] sargColumns,
                                        OrcFile.WriterVersion version,
                                        OrcProto.Stream.Kind[] bloomFilterKinds) {
    DiskRangeList.CreateHelper result = new DiskRangeList.CreateHelper();
    List<OrcProto.Stream> streams = footer.getStreamsList();
    // figure out which kind of bloom filter we want for each column
    // picks bloom_filter_utf8 if its available, otherwise bloom_filter
    if (sargColumns != null) {
      for (OrcProto.Stream stream : streams) {
        if (stream.hasKind() && stream.hasColumn()) {
          int column = stream.getColumn();
          if (sargColumns[column]) {
            switch (stream.getKind()) {
              case BLOOM_FILTER:
                if (bloomFilterKinds[column] == null &&
                    !(ignoreNonUtf8BloomFilter &&
                        hadBadBloomFilters(fileSchema.findSubtype(column)
                            .getCategory(), version))) {
                  bloomFilterKinds[column] = OrcProto.Stream.Kind.BLOOM_FILTER;
                }
                break;
              case BLOOM_FILTER_UTF8:
                bloomFilterKinds[column] = OrcProto.Stream.Kind.BLOOM_FILTER_UTF8;
                break;
              default:
                break;
            }
          }
        }
      }
    }
    long offset = 0;
    for(OrcProto.Stream stream: footer.getStreamsList()) {
      if (stream.hasKind() && stream.hasColumn()) {
        int column = stream.getColumn();
        if (fileIncluded == null || fileIncluded[column]) {
          boolean needStream = false;
          switch (stream.getKind()) {
            case ROW_INDEX:
              needStream = true;
              break;
            case BLOOM_FILTER:
              needStream = bloomFilterKinds[column] == OrcProto.Stream.Kind.BLOOM_FILTER;
              break;
            case BLOOM_FILTER_UTF8:
              needStream = bloomFilterKinds[column] == OrcProto.Stream.Kind.BLOOM_FILTER_UTF8;
              break;
            default:
              // PASS
              break;
          }
          if (needStream) {
            result.addOrMerge(offset, offset + stream.getLength(), true, false);
          }
        }
      }
      offset += stream.getLength();
    }
    return result.get();
  }

  private static class DefaultDataReader implements DataReader {
    private FSDataInputStream file = null;
    private final ByteBufferAllocatorPool pool;
    private HadoopShims.ZeroCopyReaderShim zcr = null;
    private final FileSystem fs;
    private final Path path;
    private final boolean useZeroCopy;
    private final CompressionCodec codec;
    private final int bufferSize;
    private final int typeCount;
    private CompressionKind compressionKind;

    private DefaultDataReader(DataReaderProperties properties) {
      this.fs = properties.getFileSystem();
      this.path = properties.getPath();
      this.useZeroCopy = properties.getZeroCopy();
      this.compressionKind = properties.getCompression();
      this.codec = OrcCodecPool.getCodec(compressionKind);
      this.bufferSize = properties.getBufferSize();
      this.typeCount = properties.getTypeCount();
      if (useZeroCopy) {
        this.pool = new ByteBufferAllocatorPool();
      } else {
        this.pool = null;
      }
    }

    @Override
    public void open() throws IOException {
      this.file = fs.open(path);
      if (useZeroCopy) {
        zcr = RecordReaderUtils.createZeroCopyShim(file, codec, pool);
      } else {
        zcr = null;
      }
    }

    @Override
    public OrcIndex readRowIndex(StripeInformation stripe,
                                 TypeDescription fileSchema,
                                 OrcProto.StripeFooter footer,
                                 boolean ignoreNonUtf8BloomFilter,
                                 boolean[] included,
                                 OrcProto.RowIndex[] indexes,
                                 boolean[] sargColumns,
                                 OrcFile.WriterVersion version,
                                 OrcProto.Stream.Kind[] bloomFilterKinds,
                                 OrcProto.BloomFilterIndex[] bloomFilterIndices
                                 ) throws IOException {
      if (file == null) {
        open();
      }
      if (footer == null) {
        footer = readStripeFooter(stripe);
      }
      if (indexes == null) {
        indexes = new OrcProto.RowIndex[typeCount];
      }
      if (bloomFilterKinds == null) {
        bloomFilterKinds = new OrcProto.Stream.Kind[typeCount];
      }
      if (bloomFilterIndices == null) {
        bloomFilterIndices = new OrcProto.BloomFilterIndex[typeCount];
      }
      DiskRangeList ranges = planIndexReading(fileSchema, footer,
          ignoreNonUtf8BloomFilter, included, sargColumns, version,
          bloomFilterKinds);
      ranges = readDiskRanges(file, zcr, stripe.getOffset(), ranges, false);
      long offset = 0;
      DiskRangeList range = ranges;
      for(OrcProto.Stream stream: footer.getStreamsList()) {
        // advance to find the next range
        while (range != null && range.getEnd() <= offset) {
          range = range.next;
        }
        // no more ranges, so we are done
        if (range == null) {
          break;
        }
        int column = stream.getColumn();
        if (stream.hasKind() && range.getOffset() <= offset) {
          switch (stream.getKind()) {
            case ROW_INDEX:
              if (included == null || included[column]) {
                ByteBuffer bb = range.getData().duplicate();
                bb.position((int) (offset - range.getOffset()));
                bb.limit((int) (bb.position() + stream.getLength()));
                indexes[column] = OrcProto.RowIndex.parseFrom(
                    InStream.createCodedInputStream("index",
                        ReaderImpl.singleton(new BufferChunk(bb, 0)),
                        stream.getLength(),
                    codec, bufferSize));
              }
              break;
            case BLOOM_FILTER:
            case BLOOM_FILTER_UTF8:
              if (sargColumns != null && sargColumns[column]) {
                ByteBuffer bb = range.getData().duplicate();
                bb.position((int) (offset - range.getOffset()));
                bb.limit((int) (bb.position() + stream.getLength()));
                bloomFilterIndices[column] = OrcProto.BloomFilterIndex.parseFrom
                    (InStream.createCodedInputStream("bloom_filter",
                        ReaderImpl.singleton(new BufferChunk(bb, 0)),
                    stream.getLength(), codec, bufferSize));
              }
              break;
            default:
              break;
          }
        }
        offset += stream.getLength();
      }
      return new OrcIndex(indexes, bloomFilterKinds, bloomFilterIndices);
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
      if (file == null) {
        open();
      }
      long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
      int tailLength = (int) stripe.getFooterLength();

      // read the footer
      ByteBuffer tailBuf = ByteBuffer.allocate(tailLength);
      file.readFully(offset, tailBuf.array(), tailBuf.arrayOffset(), tailLength);
      return OrcProto.StripeFooter.parseFrom(InStream.createCodedInputStream("footer",
          ReaderImpl.singleton(new BufferChunk(tailBuf, 0)),
          tailLength, codec, bufferSize));
    }

    @Override
    public DiskRangeList readFileData(
        DiskRangeList range, long baseOffset, boolean doForceDirect) throws IOException {
      return RecordReaderUtils.readDiskRanges(file, zcr, baseOffset, range, doForceDirect);
    }

    @Override
    public void close() throws IOException {
      if (codec != null) {
        OrcCodecPool.returnCodec(compressionKind, codec);
      }
      if (pool != null) {
        pool.clear();
      }
      // close both zcr and file
      try (HadoopShims.ZeroCopyReaderShim myZcr = zcr) {
        if (file != null) {
          file.close();
        }
      }
    }

    @Override
    public boolean isTrackingDiskRanges() {
      return zcr != null;
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      zcr.releaseBuffer(buffer);
    }

    @Override
    public DataReader clone() {
      try {
        return (DataReader) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new UnsupportedOperationException("uncloneable", e);
      }
    }

    @Override
    public CompressionCodec getCompressionCodec() {
      return codec;
    }
  }

  public static DataReader createDefaultDataReader(DataReaderProperties properties) {
    return new DefaultDataReader(properties);
  }

  public static boolean[] findPresentStreamsByColumn(
      List<OrcProto.Stream> streamList, List<OrcProto.Type> types) {
    boolean[] hasNull = new boolean[types.size()];
    for(OrcProto.Stream stream: streamList) {
      if (stream.hasKind() && (stream.getKind() == OrcProto.Stream.Kind.PRESENT)) {
        hasNull[stream.getColumn()] = true;
      }
    }
    return hasNull;
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

  public static void addEntireStreamToRanges(
      long offset, long length, DiskRangeList.CreateHelper list, boolean doMergeBuffers) {
    list.addOrMerge(offset, offset + length, doMergeBuffers, false);
  }

  public static void addRgFilteredStreamToRanges(OrcProto.Stream stream,
      boolean[] includedRowGroups, boolean isCompressed, OrcProto.RowIndex index,
      OrcProto.ColumnEncoding encoding, OrcProto.Type type, int compressionSize, boolean hasNull,
      long offset, long length, DiskRangeList.CreateHelper list, boolean doMergeBuffers) {
    for (int group = 0; group < includedRowGroups.length; ++group) {
      if (!includedRowGroups[group]) continue;
      int posn = getIndexPosition(
          encoding.getKind(), type.getKind(), stream.getKind(), isCompressed, hasNull);
      long start = index.getEntry(group).getPositions(posn);
      final long nextGroupOffset;
      boolean isLast = group == (includedRowGroups.length - 1);
      nextGroupOffset = isLast ? length : index.getEntry(group + 1).getPositions(posn);

      start += offset;
      long end = offset + estimateRgEndOffset(
          isCompressed, isLast, nextGroupOffset, length, compressionSize);
      list.addOrMerge(start, end, doMergeBuffers, true);
    }
  }

  public static long estimateRgEndOffset(boolean isCompressed, boolean isLast,
      long nextGroupOffset, long streamLength, int bufferSize) {
    // figure out the worst case last location
    // if adjacent groups have the same compressed block offset then stretch the slop
    // by factor of 2 to safely accommodate the next compression block.
    // One for the current compression block and another for the next compression block.
    long slop = isCompressed ? 2 * (OutStream.HEADER_SIZE + bufferSize) : WORST_UNCOMPRESSED_SLOP;
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
   * @param isCompressed is the file compressed
   * @param hasNulls does the column have a PRESENT stream?
   * @return the number of positions that will be used for that stream
   */
  public static int getIndexPosition(OrcProto.ColumnEncoding.Kind columnEncoding,
                              OrcProto.Type.Kind columnType,
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
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case DECIMAL:
        if (streamType == OrcProto.Stream.Kind.DATA) {
          return base;
        }
        return base + BYTE_STREAM_POSITIONS + compressionValue;
      case TIMESTAMP:
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
      buffer.append(range.toString());
      buffer.append("}");
      range = range.next;
    }
    buffer.append("]");
    return buffer.toString();
  }

  /**
   * Read the list of ranges from the file.
   * @param file the file to read
   * @param base the base of the stripe
   * @param range the disk ranges within the stripe to read
   * @return the bytes read for each disk range, which is the same length as
   *    ranges
   * @throws IOException
   */
  static DiskRangeList readDiskRanges(FSDataInputStream file,
                                      HadoopShims.ZeroCopyReaderShim zcr,
                                 long base,
                                 DiskRangeList range,
                                 boolean doForceDirect) throws IOException {
    if (range == null) return null;
    DiskRangeList prev = range.prev;
    if (prev == null) {
      prev = new DiskRangeList.MutateHelper(range);
    }
    while (range != null) {
      if (range.hasData()) {
        range = range.next;
        continue;
      }
      int len = (int) (range.getEnd() - range.getOffset());
      long off = range.getOffset();
      if (zcr != null) {
        file.seek(base + off);
        boolean hasReplaced = false;
        while (len > 0) {
          ByteBuffer partial = zcr.readBuffer(len, false);
          BufferChunk bc = new BufferChunk(partial, off);
          if (!hasReplaced) {
            range.replaceSelfWith(bc);
            hasReplaced = true;
          } else {
            range.insertAfter(bc);
          }
          range = bc;
          int read = partial.remaining();
          len -= read;
          off += read;
        }
      } else {
        // Don't use HDFS ByteBuffer API because it has no readFully, and is buggy and pointless.
        byte[] buffer = new byte[len];
        file.readFully((base + off), buffer, 0, buffer.length);
        ByteBuffer bb = null;
        if (doForceDirect) {
          bb = ByteBuffer.allocateDirect(len);
          bb.put(buffer);
          bb.position(0);
          bb.limit(len);
        } else {
          bb = ByteBuffer.wrap(buffer);
        }
        range = range.replaceSelfWith(new BufferChunk(bb, range.getOffset()));
      }
      range = range.next;
    }
    return prev.next;
  }


  static List<DiskRange> getStreamBuffers(DiskRangeList range, long offset, long length) {
    // This assumes sorted ranges (as do many other parts of ORC code.
    ArrayList<DiskRange> buffers = new ArrayList<DiskRange>();
    if (length == 0) return buffers;
    long streamEnd = offset + length;
    boolean inRange = false;
    while (range != null) {
      if (!inRange) {
        if (range.getEnd() <= offset) {
          range = range.next;
          continue; // Skip until we are in range.
        }
        inRange = true;
        if (range.getOffset() < offset) {
          // Partial first buffer, add a slice of it.
          buffers.add(range.sliceAndShift(offset, Math.min(streamEnd, range.getEnd()), -offset));
          if (range.getEnd() >= streamEnd) break; // Partial first buffer is also partial last buffer.
          range = range.next;
          continue;
        }
      } else if (range.getOffset() >= streamEnd) {
        break;
      }
      if (range.getEnd() > streamEnd) {
        // Partial last buffer (may also be the first buffer), add a slice of it.
        buffers.add(range.sliceAndShift(range.getOffset(), streamEnd, -offset));
        break;
      }
      // Buffer that belongs entirely to one stream.
      // TODO: ideally we would want to reuse the object and remove it from the list, but we cannot
      //       because bufferChunks is also used by clearStreams for zcr. Create a useless dup.
      buffers.add(range.sliceAndShift(range.getOffset(), range.getEnd(), -offset));
      if (range.getEnd() == streamEnd) break;
      range = range.next;
    }
    return buffers;
  }

  static HadoopShims.ZeroCopyReaderShim createZeroCopyShim(FSDataInputStream file,
      CompressionCodec codec, ByteBufferAllocatorPool pool) throws IOException {
    if ((codec == null || ((codec instanceof DirectDecompressionCodec)
            && ((DirectDecompressionCodec) codec).isAvailable()))) {
      /* codec is null or is available */
      return SHIMS.getZeroCopyReader(file, pool);
    }
    return null;
  }

  // this is an implementation copied from ElasticByteBufferPool in hadoop-2,
  // which lacks a clear()/clean() operation
  public final static class ByteBufferAllocatorPool implements HadoopShims.ByteBufferPoolShim {
    private static final class Key implements Comparable<Key> {
      private final int capacity;
      private final long insertionGeneration;

      Key(int capacity, long insertionGeneration) {
        this.capacity = capacity;
        this.insertionGeneration = insertionGeneration;
      }

      @Override
      public int compareTo(Key other) {
        if (capacity != other.capacity) {
          return capacity - other.capacity;
        } else {
          return Long.compare(insertionGeneration, other.insertionGeneration);
        }
      }

      @Override
      public boolean equals(Object rhs) {
        if (rhs == null) {
          return false;
        }
        try {
          Key o = (Key) rhs;
          return (compareTo(o) == 0);
        } catch (ClassCastException e) {
          return false;
        }
      }

      @Override
      public int hashCode() {
        return new HashCodeBuilder().append(capacity).append(insertionGeneration)
            .toHashCode();
      }
    }

    private final TreeMap<Key, ByteBuffer> buffers = new TreeMap<Key, ByteBuffer>();

    private final TreeMap<Key, ByteBuffer> directBuffers = new TreeMap<Key, ByteBuffer>();

    private long currentGeneration = 0;

    private final TreeMap<Key, ByteBuffer> getBufferTree(boolean direct) {
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
      while (true) {
        Key key = new Key(buffer.capacity(), currentGeneration++);
        if (!tree.containsKey(key)) {
          tree.put(key, buffer);
          return;
        }
        // Buffers are indexed by (capacity, generation).
        // If our key is not unique on the first try, we try again
      }
    }
  }
}
