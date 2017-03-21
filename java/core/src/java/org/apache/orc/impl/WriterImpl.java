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
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.BinaryColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;
import org.apache.orc.util.BloomFilterUtf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.Text;

import com.google.protobuf.ByteString;

/**
 * An ORC file writer. The file is divided into stripes, which is the natural
 * unit of work when reading. Each stripe is buffered in memory until the
 * memory reaches the stripe size and then it is written out broken down by
 * columns. Each column is written by a TreeWriter that is specific to that
 * type of column. TreeWriters may have children TreeWriters that handle the
 * sub-types. Each of the TreeWriters writes the column's data as a set of
 * streams.
 *
 * This class is unsynchronized like most Stream objects, so from the creation
 * of an OrcFile and all access to a single instance has to be from a single
 * thread.
 *
 * There are no known cases where these happen between different threads today.
 *
 * Caveat: the MemoryManager is created during WriterOptions create, that has
 * to be confined to a single thread as well.
 *
 */
public class WriterImpl implements Writer, MemoryManager.Callback {

  private static final Logger LOG = LoggerFactory.getLogger(WriterImpl.class);

  private static final int MIN_ROW_INDEX_STRIDE = 1000;

  private final Path path;
  private final long defaultStripeSize;
  private long adjustedStripeSize;
  private final int rowIndexStride;
  private final CompressionKind compress;
  private int bufferSize;
  private final long blockSize;
  private final TypeDescription schema;
  private final PhysicalWriter physicalWriter;
  private final OrcFile.WriterVersion writerVersion;

  private int columnCount;
  private long rowCount = 0;
  private long rowsInStripe = 0;
  private long rawDataSize = 0;
  private int rowsInIndex = 0;
  private long lastFlushOffset = 0;
  private int stripesAtLastFlush = -1;
  private final List<OrcProto.StripeInformation> stripes =
    new ArrayList<OrcProto.StripeInformation>();
  private final Map<String, ByteString> userMetadata =
    new TreeMap<String, ByteString>();
  private final StreamFactory streamFactory = new StreamFactory();
  private final TreeWriter treeWriter;
  private final boolean buildIndex;
  private final MemoryManager memoryManager;
  private final OrcFile.Version version;
  private final Configuration conf;
  private final OrcFile.WriterCallback callback;
  private final OrcFile.WriterContext callbackContext;
  private final OrcFile.EncodingStrategy encodingStrategy;
  private final OrcFile.CompressionStrategy compressionStrategy;
  private final boolean[] bloomFilterColumns;
  private final double bloomFilterFpp;
  private final OrcFile.BloomFilterVersion bloomFilterVersion;
  private boolean writeTimeZone;

  public WriterImpl(FileSystem fs,
                    Path path,
                    OrcFile.WriterOptions opts) throws IOException {
    this.path = path;
    this.conf = opts.getConfiguration();
    this.callback = opts.getCallback();
    this.schema = opts.getSchema();
    this.writerVersion = opts.getWriterVersion();
    bloomFilterVersion = opts.getBloomFilterVersion();
    if (callback != null) {
      callbackContext = new OrcFile.WriterContext(){

        @Override
        public Writer getWriter() {
          return WriterImpl.this;
        }
      };
    } else {
      callbackContext = null;
    }
    this.adjustedStripeSize = opts.getStripeSize();
    this.defaultStripeSize = opts.getStripeSize();
    this.version = opts.getVersion();
    this.encodingStrategy = opts.getEncodingStrategy();
    this.compressionStrategy = opts.getCompressionStrategy();
    this.blockSize = opts.getBlockSize();
    this.compress = opts.getCompress();
    this.rowIndexStride = opts.getRowIndexStride();
    this.memoryManager = opts.getMemoryManager();
    buildIndex = rowIndexStride > 0;
    int numColumns = schema.getMaximumId() + 1;
    if (opts.isEnforceBufferSize()) {
      this.bufferSize = opts.getBufferSize();
    } else {
      this.bufferSize = getEstimatedBufferSize(defaultStripeSize,
          numColumns, opts.getBufferSize());
    }
    if (version == OrcFile.Version.V_0_11) {
      /* do not write bloom filters for ORC v11 */
      this.bloomFilterColumns = new boolean[schema.getMaximumId() + 1];
    } else {
      this.bloomFilterColumns =
          OrcUtils.includeColumns(opts.getBloomFilterColumns(), schema);
    }
    this.bloomFilterFpp = opts.getBloomFilterFpp();
    this.physicalWriter = opts.getPhysicalWriter() == null ?
        new PhysicalFsWriter(fs, path, opts) : opts.getPhysicalWriter();
    physicalWriter.writeHeader();
    treeWriter = createTreeWriter(schema, streamFactory, false);
    if (buildIndex && rowIndexStride < MIN_ROW_INDEX_STRIDE) {
      throw new IllegalArgumentException("Row stride must be at least " +
          MIN_ROW_INDEX_STRIDE);
    }

    // ensure that we are able to handle callbacks before we register ourselves
    memoryManager.addWriter(path, opts.getStripeSize(), this);
    LOG.info("ORC writer created for path: {} with stripeSize: {} blockSize: {}" +
        " compression: {} bufferSize: {}", path, defaultStripeSize, blockSize,
        compress, bufferSize);
  }

  //@VisibleForTesting
  public static int getEstimatedBufferSize(long stripeSize, int numColumns,
                                           int bs) {
    // The worst case is that there are 2 big streams per a column and
    // we want to guarantee that each stream gets ~10 buffers.
    // This keeps buffers small enough that we don't get really small stripe
    // sizes.
    int estBufferSize = (int) (stripeSize / (20L * numColumns));
    estBufferSize = getClosestBufferSize(estBufferSize);
    return estBufferSize > bs ? bs : estBufferSize;
  }

  /**
   * Increase the buffer size for this writer.
   * This function is internal only and should only be called by the
   * ORC file merger.
   * @param newSize the new buffer size.
   */
  public void increaseCompressionSize(int newSize) {
    if (newSize > bufferSize) {
      bufferSize = newSize;
    }
  }

  private static int getClosestBufferSize(int estBufferSize) {
    final int kb4 = 4 * 1024;
    final int kb8 = 8 * 1024;
    final int kb16 = 16 * 1024;
    final int kb32 = 32 * 1024;
    final int kb64 = 64 * 1024;
    final int kb128 = 128 * 1024;
    final int kb256 = 256 * 1024;
    if (estBufferSize <= kb4) {
      return kb4;
    } else if (estBufferSize > kb4 && estBufferSize <= kb8) {
      return kb8;
    } else if (estBufferSize > kb8 && estBufferSize <= kb16) {
      return kb16;
    } else if (estBufferSize > kb16 && estBufferSize <= kb32) {
      return kb32;
    } else if (estBufferSize > kb32 && estBufferSize <= kb64) {
      return kb64;
    } else if (estBufferSize > kb64 && estBufferSize <= kb128) {
      return kb128;
    } else {
      return kb256;
    }
  }

  public static CompressionCodec createCodec(CompressionKind kind) {
    switch (kind) {
      case NONE:
        return null;
      case ZLIB:
        return new ZlibCodec();
      case SNAPPY:
        return new SnappyCodec();
      case LZO:
        return new AircompressorCodec(new LzoCompressor(),
            new LzoDecompressor());
      case LZ4:
        return new AircompressorCodec(new Lz4Compressor(),
            new Lz4Decompressor());
      default:
        throw new IllegalArgumentException("Unknown compression codec: " +
            kind);
    }
  }

  @Override
  public boolean checkMemory(double newScale) throws IOException {
    long limit = (long) Math.round(adjustedStripeSize * newScale);
    long size = treeWriter.estimateMemory();
    if (LOG.isDebugEnabled()) {
      LOG.debug("ORC writer " + physicalWriter + " size = " + size +
          " limit = " + limit);
    }
    if (size > limit) {
      flushStripe();
      return true;
    }
    return false;
  }

  private static class RowIndexPositionRecorder implements PositionRecorder {
    private final OrcProto.RowIndexEntry.Builder builder;

    RowIndexPositionRecorder(OrcProto.RowIndexEntry.Builder builder) {
      this.builder = builder;
    }

    @Override
    public void addPosition(long position) {
      builder.addPositions(position);
    }
  }

  CompressionCodec getCustomizedCodec(OrcProto.Stream.Kind kind) {
    // TODO: modify may create a new codec here. We want to end() it when the stream is closed,
    //       but at this point there's no close() for the stream.
    CompressionCodec result = physicalWriter.getCompressionCodec();
    if (result != null) {
      switch (kind) {
        case BLOOM_FILTER:
        case DATA:
        case DICTIONARY_DATA:
        case BLOOM_FILTER_UTF8:
          if (compressionStrategy == OrcFile.CompressionStrategy.SPEED) {
            result = result.modify(EnumSet.of(CompressionCodec.Modifier.FAST,
                CompressionCodec.Modifier.TEXT));
          } else {
            result = result.modify(EnumSet.of(CompressionCodec.Modifier.DEFAULT,
                CompressionCodec.Modifier.TEXT));
          }
          break;
        case LENGTH:
        case DICTIONARY_COUNT:
        case PRESENT:
        case ROW_INDEX:
        case SECONDARY:
          // easily compressed using the fastest modes
          result = result.modify(EnumSet.of(CompressionCodec.Modifier.FASTEST,
              CompressionCodec.Modifier.BINARY));
          break;
        default:
          LOG.info("Missing ORC compression modifiers for " + kind);
          break;
      }
    }
    return result;
  }

  /**
   * Interface from the Writer to the TreeWriters. This limits the visibility
   * that the TreeWriters have into the Writer.
   */
  private class StreamFactory {
    /**
     * Create a stream to store part of a column.
     * @param column the column id for the stream
     * @param kind the kind of stream
     * @return The output outStream that the section needs to be written to.
     * @throws IOException
     */
    public OutStream createStream(int column,
                                  OrcProto.Stream.Kind kind
                                  ) throws IOException {
      final StreamName name = new StreamName(column, kind);
      CompressionCodec codec = getCustomizedCodec(kind);

      return new OutStream(physicalWriter.toString(), bufferSize, codec,
          physicalWriter.createDataStream(name));
    }

    /**
     * Get the next column id.
     * @return a number from 0 to the number of columns - 1
     */
    public int getNextColumnId() {
      return columnCount++;
    }

    /**
     * Get the stride rate of the row index.
     */
    public int getRowIndexStride() {
      return rowIndexStride;
    }

    /**
     * Should be building the row index.
     * @return true if we are building the index
     */
    public boolean buildIndex() {
      return buildIndex;
    }

    /**
     * Is the ORC file compressed?
     * @return are the streams compressed
     */
    public boolean isCompressed() {
      return physicalWriter.getCompressionCodec() != null;
    }

    /**
     * Get the encoding strategy to use.
     * @return encoding strategy
     */
    public OrcFile.EncodingStrategy getEncodingStrategy() {
      return encodingStrategy;
    }

    /**
     * Get the compression strategy to use.
     * @return compression strategy
     */
    public OrcFile.CompressionStrategy getCompressionStrategy() {
      return compressionStrategy;
    }

    /**
     * Get the bloom filter columns
     * @return bloom filter columns
     */
    public boolean[] getBloomFilterColumns() {
      return bloomFilterColumns;
    }

    /**
     * Get bloom filter false positive percentage.
     * @return fpp
     */
    public double getBloomFilterFPP() {
      return bloomFilterFpp;
    }

    /**
     * Get the writer's configuration.
     * @return configuration
     */
    public Configuration getConfiguration() {
      return conf;
    }

    /**
     * Get the version of the file to write.
     */
    public OrcFile.Version getVersion() {
      return version;
    }

    public void useWriterTimeZone(boolean val) {
      writeTimeZone = val;
    }

    public boolean hasWriterTimeZone() {
      return writeTimeZone;
    }

    public OrcFile.BloomFilterVersion getBloomFilterVersion() {
      return bloomFilterVersion;
    }

    public void writeIndex(StreamName name,
                           OrcProto.RowIndex.Builder index) throws IOException {
      physicalWriter.writeIndex(name, index, getCustomizedCodec(name.getKind()));
    }

    public void writeBloomFilter(StreamName name,
                                 OrcProto.BloomFilterIndex.Builder bloom
                                 ) throws IOException {
      physicalWriter.writeBloomFilter(name, bloom,
          getCustomizedCodec(name.getKind()));
    }
  }

  /**
   * The parent class of all of the writers for each column. Each column
   * is written by an instance of this class. The compound types (struct,
   * list, map, and union) have children tree writers that write the children
   * types.
   */
  private abstract static class TreeWriter {
    protected final int id;
    protected final BitFieldWriter isPresent;
    private final boolean isCompressed;
    protected final ColumnStatisticsImpl indexStatistics;
    protected final ColumnStatisticsImpl stripeColStatistics;
    private final ColumnStatisticsImpl fileStatistics;
    protected TreeWriter[] childrenWriters;
    protected final RowIndexPositionRecorder rowIndexPosition;
    private final OrcProto.RowIndex.Builder rowIndex;
    private final OrcProto.RowIndexEntry.Builder rowIndexEntry;
    protected final BloomFilter bloomFilter;
    protected final BloomFilterUtf8 bloomFilterUtf8;
    protected final boolean createBloomFilter;
    private final OrcProto.BloomFilterIndex.Builder bloomFilterIndex;
    private final OrcProto.BloomFilterIndex.Builder bloomFilterIndexUtf8;
    protected final OrcProto.BloomFilter.Builder bloomFilterEntry;
    private boolean foundNulls;
    private OutStream isPresentOutStream;
    private final List<OrcProto.StripeStatistics.Builder> stripeStatsBuilders;
    private final StreamFactory streamFactory;

    /**
     * Create a tree writer.
     * @param columnId the column id of the column to write
     * @param schema the row schema
     * @param streamFactory limited access to the Writer's data.
     * @param nullable can the value be null?
     * @throws IOException
     */
    TreeWriter(int columnId,
               TypeDescription schema,
               StreamFactory streamFactory,
               boolean nullable) throws IOException {
      this.streamFactory = streamFactory;
      this.isCompressed = streamFactory.isCompressed();
      this.id = columnId;
      if (nullable) {
        isPresentOutStream = streamFactory.createStream(id,
            OrcProto.Stream.Kind.PRESENT);
        isPresent = new BitFieldWriter(isPresentOutStream, 1);
      } else {
        isPresent = null;
      }
      this.foundNulls = false;
      createBloomFilter = streamFactory.getBloomFilterColumns()[columnId];
      indexStatistics = ColumnStatisticsImpl.create(schema);
      stripeColStatistics = ColumnStatisticsImpl.create(schema);
      fileStatistics = ColumnStatisticsImpl.create(schema);
      childrenWriters = new TreeWriter[0];
      if (streamFactory.buildIndex()) {
        rowIndex = OrcProto.RowIndex.newBuilder();
        rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
        rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
      } else {
        rowIndex = null;
        rowIndexEntry = null;
        rowIndexPosition = null;
      }
      stripeStatsBuilders = new ArrayList<>();
      if (createBloomFilter) {
        bloomFilterEntry = OrcProto.BloomFilter.newBuilder();
        if (streamFactory.getBloomFilterVersion() == OrcFile.BloomFilterVersion.ORIGINAL) {
          bloomFilter = new BloomFilter(streamFactory.getRowIndexStride(),
              streamFactory.getBloomFilterFPP());
          bloomFilterIndex = OrcProto.BloomFilterIndex.newBuilder();
        } else {
          bloomFilter = null;
          bloomFilterIndex = null;
        }
        bloomFilterUtf8 = new BloomFilterUtf8(streamFactory.getRowIndexStride(),
            streamFactory.getBloomFilterFPP());
        bloomFilterIndexUtf8 = OrcProto.BloomFilterIndex.newBuilder();
      } else {
        bloomFilterEntry = null;
        bloomFilterIndex = null;
        bloomFilterIndexUtf8 = null;
        bloomFilter = null;
        bloomFilterUtf8 = null;
      }
    }

    protected OrcProto.RowIndex.Builder getRowIndex() {
      return rowIndex;
    }

    protected ColumnStatisticsImpl getStripeStatistics() {
      return stripeColStatistics;
    }

    protected OrcProto.RowIndexEntry.Builder getRowIndexEntry() {
      return rowIndexEntry;
    }

    IntegerWriter createIntegerWriter(PositionedOutputStream output,
                                      boolean signed, boolean isDirectV2,
                                      StreamFactory writer) {
      if (isDirectV2) {
        boolean alignedBitpacking = false;
        if (writer.getEncodingStrategy().equals(OrcFile.EncodingStrategy.SPEED)) {
          alignedBitpacking = true;
        }
        return new RunLengthIntegerWriterV2(output, signed, alignedBitpacking);
      } else {
        return new RunLengthIntegerWriter(output, signed);
      }
    }

    boolean isNewWriteFormat(StreamFactory writer) {
      return writer.getVersion() != OrcFile.Version.V_0_11;
    }

    /**
     * Handle the top level object write.
     *
     * This default method is used for all types except structs, which are the
     * typical case. VectorizedRowBatch assumes the top level object is a
     * struct, so we use the first column for all other types.
     * @param batch the batch to write from
     * @param offset the row to start on
     * @param length the number of rows to write
     * @throws IOException
     */
    void writeRootBatch(VectorizedRowBatch batch, int offset,
                        int length) throws IOException {
      writeBatch(batch.cols[0], offset, length);
    }

    /**
     * Write the values from the given vector from offset for length elements.
     * @param vector the vector to write from
     * @param offset the first value from the vector to write
     * @param length the number of values from the vector to write
     * @throws IOException
     */
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      if (vector.noNulls) {
        indexStatistics.increment(length);
        if (isPresent != null) {
          for (int i = 0; i < length; ++i) {
            isPresent.write(1);
          }
        }
      } else {
        if (vector.isRepeating) {
          boolean isNull = vector.isNull[0];
          if (isPresent != null) {
            for (int i = 0; i < length; ++i) {
              isPresent.write(isNull ? 0 : 1);
            }
          }
          if (isNull) {
            foundNulls = true;
            indexStatistics.setNull();
          } else {
            indexStatistics.increment(length);
          }
        } else {
          // count the number of non-null values
          int nonNullCount = 0;
          for(int i = 0; i < length; ++i) {
            boolean isNull = vector.isNull[i + offset];
            if (!isNull) {
              nonNullCount += 1;
            }
            if (isPresent != null) {
              isPresent.write(isNull ? 0 : 1);
            }
          }
          indexStatistics.increment(nonNullCount);
          if (nonNullCount != length) {
            foundNulls = true;
            indexStatistics.setNull();
          }
        }
      }
    }

    private void removeIsPresentPositions() {
      for(int i=0; i < rowIndex.getEntryCount(); ++i) {
        OrcProto.RowIndexEntry.Builder entry = rowIndex.getEntryBuilder(i);
        List<Long> positions = entry.getPositionsList();
        // bit streams use 3 positions if uncompressed, 4 if compressed
        positions = positions.subList(isCompressed ? 4 : 3, positions.size());
        entry.clearPositions();
        entry.addAllPositions(positions);
      }
    }

    /**
     * Write the stripe out to the file.
     * @param builder the stripe footer that contains the information about the
     *                layout of the stripe. The TreeWriter is required to update
     *                the footer with its information.
     * @param requiredIndexEntries the number of index entries that are
     *                             required. this is to check to make sure the
     *                             row index is well formed.
     * @throws IOException
     */
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      if (isPresent != null) {
        isPresent.flush();

        // if no nulls are found in a stream, then suppress the stream
        if(!foundNulls) {
          isPresentOutStream.suppress();
          // since isPresent bitstream is suppressed, update the index to
          // remove the positions of the isPresent stream
          if (rowIndex != null) {
            removeIsPresentPositions();
          }
        }
      }

      // merge stripe-level column statistics to file statistics and write it to
      // stripe statistics
      OrcProto.StripeStatistics.Builder stripeStatsBuilder = OrcProto.StripeStatistics.newBuilder();
      writeStripeStatistics(stripeStatsBuilder, this);
      stripeStatsBuilders.add(stripeStatsBuilder);

      // reset the flag for next stripe
      foundNulls = false;

      builder.addColumns(getEncoding());
      if (streamFactory.hasWriterTimeZone()) {
        builder.setWriterTimezone(TimeZone.getDefault().getID());
      }
      if (rowIndex != null) {
        if (rowIndex.getEntryCount() != requiredIndexEntries) {
          throw new IllegalArgumentException("Column has wrong number of " +
               "index entries found: " + rowIndex.getEntryCount() + " expected: " +
               requiredIndexEntries);
        }
        streamFactory.writeIndex(new StreamName(id, OrcProto.Stream.Kind.ROW_INDEX), rowIndex);
        rowIndex.clear();
        rowIndexEntry.clear();
      }

      // write the bloom filter to out stream
      if (bloomFilterIndex != null) {
        streamFactory.writeBloomFilter(new StreamName(id,
            OrcProto.Stream.Kind.BLOOM_FILTER), bloomFilterIndex);
        bloomFilterIndex.clear();
      }
      // write the bloom filter to out stream
      if (bloomFilterIndexUtf8 != null) {
        streamFactory.writeBloomFilter(new StreamName(id,
            OrcProto.Stream.Kind.BLOOM_FILTER_UTF8), bloomFilterIndexUtf8);
        bloomFilterIndexUtf8.clear();
      }
    }

    private void writeStripeStatistics(OrcProto.StripeStatistics.Builder builder,
        TreeWriter treeWriter) {
      treeWriter.fileStatistics.merge(treeWriter.stripeColStatistics);
      builder.addColStats(treeWriter.stripeColStatistics.serialize().build());
      treeWriter.stripeColStatistics.reset();
      for (TreeWriter child : treeWriter.getChildrenWriters()) {
        writeStripeStatistics(builder, child);
      }
    }

    TreeWriter[] getChildrenWriters() {
      return childrenWriters;
    }

    /**
     * Get the encoding for this column.
     * @return the information about the encoding of this column
     */
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder builder =
          OrcProto.ColumnEncoding.newBuilder()
              .setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      if (createBloomFilter) {
        builder.setBloomEncoding(BloomFilterIO.Encoding.CURRENT.getId());
      }
      return builder;
    }

    /**
     * Create a row index entry with the previous location and the current
     * index statistics. Also merges the index statistics into the file
     * statistics before they are cleared. Finally, it records the start of the
     * next index and ensures all of the children columns also create an entry.
     * @throws IOException
     */
    void createRowIndexEntry() throws IOException {
      stripeColStatistics.merge(indexStatistics);
      rowIndexEntry.setStatistics(indexStatistics.serialize());
      indexStatistics.reset();
      rowIndex.addEntry(rowIndexEntry);
      rowIndexEntry.clear();
      addBloomFilterEntry();
      recordPosition(rowIndexPosition);
      for(TreeWriter child: childrenWriters) {
        child.createRowIndexEntry();
      }
    }

    void addBloomFilterEntry() {
      if (createBloomFilter) {
        if (bloomFilter != null) {
          BloomFilterIO.serialize(bloomFilterEntry, bloomFilter);
          bloomFilterIndex.addBloomFilter(bloomFilterEntry.build());
          bloomFilter.reset();
        }
        if (bloomFilterUtf8 != null) {
          BloomFilterIO.serialize(bloomFilterEntry, bloomFilterUtf8);
          bloomFilterIndexUtf8.addBloomFilter(bloomFilterEntry.build());
          bloomFilterUtf8.reset();
        }
      }
    }

    /**
     * Record the current position in each of this column's streams.
     * @param recorder where should the locations be recorded
     * @throws IOException
     */
    void recordPosition(PositionRecorder recorder) throws IOException {
      if (isPresent != null) {
        isPresent.getPosition(recorder);
      }
    }

    /**
     * Estimate how much memory the writer is consuming excluding the streams.
     * @return the number of bytes.
     */
    long estimateMemory() {
      long result = 0;
      if (isPresent != null) {
        result = isPresentOutStream.getBufferSize();
      }
      for (TreeWriter child: childrenWriters) {
        result += child.estimateMemory();
      }
      return result;
    }
  }

  private static class BooleanTreeWriter extends TreeWriter {
    private final BitFieldWriter writer;

    BooleanTreeWriter(int columnId,
                      TypeDescription schema,
                      StreamFactory writer,
                      boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      PositionedOutputStream out = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      this.writer = new BitFieldWriter(out, 1);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      LongColumnVector vec = (LongColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          int value = vec.vector[0] == 0 ? 0 : 1;
          indexStatistics.updateBoolean(value != 0, length);
          for(int i=0; i < length; ++i) {
            writer.write(value);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            int value = vec.vector[i + offset] == 0 ? 0 : 1;
            writer.write(value);
            indexStatistics.updateBoolean(value != 0, 1);
          }
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      writer.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      writer.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + writer.estimateMemory();
    }
  }

  private static class ByteTreeWriter extends TreeWriter {
    private final RunLengthByteWriter writer;

    ByteTreeWriter(int columnId,
                      TypeDescription schema,
                      StreamFactory writer,
                      boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.writer = new RunLengthByteWriter(writer.createStream(id,
          OrcProto.Stream.Kind.DATA));
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      LongColumnVector vec = (LongColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          byte value = (byte) vec.vector[0];
          indexStatistics.updateInteger(value, length);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(value);
            }
            bloomFilterUtf8.addLong(value);
          }
          for(int i=0; i < length; ++i) {
            writer.write(value);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            byte value = (byte) vec.vector[i + offset];
            writer.write(value);
            indexStatistics.updateInteger(value, 1);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addLong(value);
              }
              bloomFilterUtf8.addLong(value);
            }
          }
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      writer.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      writer.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + writer.estimateMemory();
    }
  }

  private static class IntegerTreeWriter extends TreeWriter {
    private final IntegerWriter writer;
    private boolean isDirectV2 = true;

    IntegerTreeWriter(int columnId,
                      TypeDescription schema,
                      StreamFactory writer,
                      boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      OutStream out = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      this.isDirectV2 = isNewWriteFormat(writer);
      this.writer = createIntegerWriter(out, true, isDirectV2, writer);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder result = super.getEncoding();
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
      return result;
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      LongColumnVector vec = (LongColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          long value = vec.vector[0];
          indexStatistics.updateInteger(value, length);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(value);
            }
            bloomFilterUtf8.addLong(value);
          }
          for(int i=0; i < length; ++i) {
            writer.write(value);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            long value = vec.vector[i + offset];
            writer.write(value);
            indexStatistics.updateInteger(value, 1);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addLong(value);
              }
              bloomFilterUtf8.addLong(value);
            }
          }
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      writer.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      writer.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + writer.estimateMemory();
    }
  }

  private static class FloatTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;
    private final SerializationUtils utils;

    FloatTreeWriter(int columnId,
                      TypeDescription schema,
                      StreamFactory writer,
                      boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.stream = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      this.utils = new SerializationUtils();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      DoubleColumnVector vec = (DoubleColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          float value = (float) vec.vector[0];
          indexStatistics.updateDouble(value);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addDouble(value);
            }
            bloomFilterUtf8.addDouble(value);
          }
          for(int i=0; i < length; ++i) {
            utils.writeFloat(stream, value);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            float value = (float) vec.vector[i + offset];
            utils.writeFloat(stream, value);
            indexStatistics.updateDouble(value);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addDouble(value);
              }
              bloomFilterUtf8.addDouble(value);
            }
          }
        }
      }
    }


    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      stream.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      stream.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + stream.getBufferSize();
    }
  }

  private static class DoubleTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;
    private final SerializationUtils utils;

    DoubleTreeWriter(int columnId,
                    TypeDescription schema,
                    StreamFactory writer,
                    boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.stream = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      this.utils = new SerializationUtils();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      DoubleColumnVector vec = (DoubleColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          double value = vec.vector[0];
          indexStatistics.updateDouble(value);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addDouble(value);
            }
            bloomFilterUtf8.addDouble(value);
          }
          for(int i=0; i < length; ++i) {
            utils.writeDouble(stream, value);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            double value = vec.vector[i + offset];
            utils.writeDouble(stream, value);
            indexStatistics.updateDouble(value);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addDouble(value);
              }
              bloomFilterUtf8.addDouble(value);
            }
          }
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      stream.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      stream.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + stream.getBufferSize();
    }
  }

  private static abstract class StringBaseTreeWriter extends TreeWriter {
    private static final int INITIAL_DICTIONARY_SIZE = 4096;
    private final OutStream stringOutput;
    protected final IntegerWriter lengthOutput;
    private final IntegerWriter rowOutput;
    protected final StringRedBlackTree dictionary =
        new StringRedBlackTree(INITIAL_DICTIONARY_SIZE);
    protected final DynamicIntArray rows = new DynamicIntArray();
    protected final PositionedOutputStream directStreamOutput;
    private final List<OrcProto.RowIndexEntry> savedRowIndex =
        new ArrayList<OrcProto.RowIndexEntry>();
    private final boolean buildIndex;
    private final List<Long> rowIndexValueCount = new ArrayList<Long>();
    // If the number of keys in a dictionary is greater than this fraction of
    //the total number of non-null rows, turn off dictionary encoding
    private final double dictionaryKeySizeThreshold;
    protected boolean useDictionaryEncoding = true;
    private boolean isDirectV2 = true;
    private boolean doneDictionaryCheck;
    private final boolean strideDictionaryCheck;

    StringBaseTreeWriter(int columnId,
                     TypeDescription schema,
                     StreamFactory writer,
                     boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.isDirectV2 = isNewWriteFormat(writer);
      directStreamOutput = writer.createStream(id, OrcProto.Stream.Kind.DATA);
      stringOutput = writer.createStream(id,
          OrcProto.Stream.Kind.DICTIONARY_DATA);
      lengthOutput = createIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
      rowOutput = createIntegerWriter(directStreamOutput, false, isDirectV2,
          writer);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
      rowIndexValueCount.add(0L);
      buildIndex = writer.buildIndex();
      Configuration conf = writer.getConfiguration();
      dictionaryKeySizeThreshold =
          OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getDouble(conf);
      strideDictionaryCheck =
          OrcConf.ROW_INDEX_STRIDE_DICTIONARY_CHECK.getBoolean(conf);
      doneDictionaryCheck = false;
    }

    private boolean checkDictionaryEncoding() {
      if (!doneDictionaryCheck) {
        // Set the flag indicating whether or not to use dictionary encoding
        // based on whether or not the fraction of distinct keys over number of
        // non-null rows is less than the configured threshold
        float ratio = rows.size() > 0 ? (float) (dictionary.size()) / rows.size() : 0.0f;
        useDictionaryEncoding = !isDirectV2 || ratio <= dictionaryKeySizeThreshold;
        doneDictionaryCheck = true;
      }
      return useDictionaryEncoding;
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      // if rows in stripe is less than dictionaryCheckAfterRows, dictionary
      // checking would not have happened. So do it again here.
      checkDictionaryEncoding();

      if (useDictionaryEncoding) {
        flushDictionary();
      } else {
        // flushout any left over entries from dictionary
        if (rows.size() > 0) {
          flushDictionary();
        }

        // suppress the stream for every stripe if dictionary is disabled
        stringOutput.suppress();
      }

      // we need to build the rowindex before calling super, since it
      // writes it out.
      super.writeStripe(builder, requiredIndexEntries);
      if (useDictionaryEncoding) {
        stringOutput.flush();
        lengthOutput.flush();
        rowOutput.flush();
      } else {
        directStreamOutput.flush();
        lengthOutput.flush();
      }
      // reset all of the fields to be ready for the next stripe.
      dictionary.clear();
      savedRowIndex.clear();
      rowIndexValueCount.clear();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
      rowIndexValueCount.add(0L);

      if (!useDictionaryEncoding) {
        // record the start positions of first index stride of next stripe i.e
        // beginning of the direct streams when dictionary is disabled
        recordDirectStreamPosition();
      }
    }

    private void flushDictionary() throws IOException {
      final int[] dumpOrder = new int[dictionary.size()];

      if (useDictionaryEncoding) {
        // Write the dictionary by traversing the red-black tree writing out
        // the bytes and lengths; and creating the map from the original order
        // to the final sorted order.

        dictionary.visit(new StringRedBlackTree.Visitor() {
          private int currentId = 0;
          @Override
          public void visit(StringRedBlackTree.VisitorContext context
                           ) throws IOException {
            context.writeBytes(stringOutput);
            lengthOutput.write(context.getLength());
            dumpOrder[context.getOriginalPosition()] = currentId++;
          }
        });
      } else {
        // for direct encoding, we don't want the dictionary data stream
        stringOutput.suppress();
      }
      int length = rows.size();
      int rowIndexEntry = 0;
      OrcProto.RowIndex.Builder rowIndex = getRowIndex();
      Text text = new Text();
      // write the values translated into the dump order.
      for(int i = 0; i <= length; ++i) {
        // now that we are writing out the row values, we can finalize the
        // row index
        if (buildIndex) {
          while (i == rowIndexValueCount.get(rowIndexEntry) &&
              rowIndexEntry < savedRowIndex.size()) {
            OrcProto.RowIndexEntry.Builder base =
                savedRowIndex.get(rowIndexEntry++).toBuilder();
            if (useDictionaryEncoding) {
              rowOutput.getPosition(new RowIndexPositionRecorder(base));
            } else {
              PositionRecorder posn = new RowIndexPositionRecorder(base);
              directStreamOutput.getPosition(posn);
              lengthOutput.getPosition(posn);
            }
            rowIndex.addEntry(base.build());
          }
        }
        if (i != length) {
          if (useDictionaryEncoding) {
            rowOutput.write(dumpOrder[rows.get(i)]);
          } else {
            dictionary.getText(text, rows.get(i));
            directStreamOutput.write(text.getBytes(), 0, text.getLength());
            lengthOutput.write(text.getLength());
          }
        }
      }
      rows.clear();
    }

    @Override
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder result = super.getEncoding();
      if (useDictionaryEncoding) {
        result.setDictionarySize(dictionary.size());
        if(isDirectV2) {
          result.setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY_V2);
        } else {
          result.setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY);
        }
      } else {
        if(isDirectV2) {
          result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
        } else {
          result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
        }
      }
      return result;
    }

    /**
     * This method doesn't call the super method, because unlike most of the
     * other TreeWriters, this one can't record the position in the streams
     * until the stripe is being flushed. Therefore it saves all of the entries
     * and augments them with the final information as the stripe is written.
     * @throws IOException
     */
    @Override
    void createRowIndexEntry() throws IOException {
      getStripeStatistics().merge(indexStatistics);
      OrcProto.RowIndexEntry.Builder rowIndexEntry = getRowIndexEntry();
      rowIndexEntry.setStatistics(indexStatistics.serialize());
      indexStatistics.reset();
      OrcProto.RowIndexEntry base = rowIndexEntry.build();
      savedRowIndex.add(base);
      rowIndexEntry.clear();
      addBloomFilterEntry();
      recordPosition(rowIndexPosition);
      rowIndexValueCount.add(Long.valueOf(rows.size()));
      if (strideDictionaryCheck) {
        checkDictionaryEncoding();
      }
      if (!useDictionaryEncoding) {
        if (rows.size() > 0) {
          flushDictionary();
          // just record the start positions of next index stride
          recordDirectStreamPosition();
        } else {
          // record the start positions of next index stride
          recordDirectStreamPosition();
          getRowIndex().addEntry(base);
        }
      }
    }

    private void recordDirectStreamPosition() throws IOException {
      if (rowIndexPosition != null) {
        directStreamOutput.getPosition(rowIndexPosition);
        lengthOutput.getPosition(rowIndexPosition);
      }
    }

    @Override
    long estimateMemory() {
      long parent = super.estimateMemory();
      if (useDictionaryEncoding) {
        return parent + dictionary.getSizeInBytes() + rows.getSizeInBytes();
      } else {
        return parent + lengthOutput.estimateMemory() +
            directStreamOutput.getBufferSize();
      }
    }
  }

  private static class StringTreeWriter extends StringBaseTreeWriter {
    StringTreeWriter(int columnId,
                   TypeDescription schema,
                   StreamFactory writer,
                   boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      BytesColumnVector vec = (BytesColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          if (useDictionaryEncoding) {
            int id = dictionary.add(vec.vector[0], vec.start[0], vec.length[0]);
            for(int i=0; i < length; ++i) {
              rows.add(id);
            }
          } else {
            for(int i=0; i < length; ++i) {
              directStreamOutput.write(vec.vector[0], vec.start[0],
                  vec.length[0]);
              lengthOutput.write(vec.length[0]);
            }
          }
          indexStatistics.updateString(vec.vector[0], vec.start[0],
              vec.length[0], length);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              // translate from UTF-8 to the default charset
              bloomFilter.addString(new String(vec.vector[0], vec.start[0],
                  vec.length[0], StandardCharsets.UTF_8));
            }
            bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            if (useDictionaryEncoding) {
              rows.add(dictionary.add(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]));
            } else {
              directStreamOutput.write(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]);
              lengthOutput.write(vec.length[offset + i]);
            }
            indexStatistics.updateString(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i], 1);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                // translate from UTF-8 to the default charset
                bloomFilter.addString(new String(vec.vector[offset + i],
                    vec.start[offset + i], vec.length[offset + i],
                    StandardCharsets.UTF_8));
              }
              bloomFilterUtf8.addBytes(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]);
            }
          }
        }
      }
    }
  }

  /**
   * Under the covers, char is written to ORC the same way as string.
   */
  private static class CharTreeWriter extends StringBaseTreeWriter {
    private final int itemLength;
    private final byte[] padding;

    CharTreeWriter(int columnId,
        TypeDescription schema,
        StreamFactory writer,
        boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      itemLength = schema.getMaxLength();
      padding = new byte[itemLength];
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      BytesColumnVector vec = (BytesColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          byte[] ptr;
          int ptrOffset;
          if (vec.length[0] >= itemLength) {
            ptr = vec.vector[0];
            ptrOffset = vec.start[0];
          } else {
            ptr = padding;
            ptrOffset = 0;
            System.arraycopy(vec.vector[0], vec.start[0], ptr, 0,
                vec.length[0]);
            Arrays.fill(ptr, vec.length[0], itemLength, (byte) ' ');
          }
          if (useDictionaryEncoding) {
            int id = dictionary.add(ptr, ptrOffset, itemLength);
            for(int i=0; i < length; ++i) {
              rows.add(id);
            }
          } else {
            for(int i=0; i < length; ++i) {
              directStreamOutput.write(ptr, ptrOffset, itemLength);
              lengthOutput.write(itemLength);
            }
          }
          indexStatistics.updateString(ptr, ptrOffset, itemLength, length);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              // translate from UTF-8 to the default charset
              bloomFilter.addString(new String(vec.vector[0], vec.start[0],
                  vec.length[0], StandardCharsets.UTF_8));
            }
            bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            byte[] ptr;
            int ptrOffset;
            if (vec.length[offset + i] >= itemLength) {
              ptr = vec.vector[offset + i];
              ptrOffset = vec.start[offset + i];
            } else {
              // it is the wrong length, so copy it
              ptr = padding;
              ptrOffset = 0;
              System.arraycopy(vec.vector[offset + i], vec.start[offset + i],
                  ptr, 0, vec.length[offset + i]);
              Arrays.fill(ptr, vec.length[offset + i], itemLength, (byte) ' ');
            }
            if (useDictionaryEncoding) {
              rows.add(dictionary.add(ptr, ptrOffset, itemLength));
            } else {
              directStreamOutput.write(ptr, ptrOffset, itemLength);
              lengthOutput.write(itemLength);
            }
            indexStatistics.updateString(ptr, ptrOffset, itemLength, 1);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                // translate from UTF-8 to the default charset
                bloomFilter.addString(new String(vec.vector[offset + i],
                    vec.start[offset + i], vec.length[offset + i],
                    StandardCharsets.UTF_8));
              }
              bloomFilterUtf8.addBytes(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]);
            }
          }
        }
      }
    }
  }

  /**
   * Under the covers, varchar is written to ORC the same way as string.
   */
  private static class VarcharTreeWriter extends StringBaseTreeWriter {
    private final int maxLength;

    VarcharTreeWriter(int columnId,
        TypeDescription schema,
        StreamFactory writer,
        boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      maxLength = schema.getMaxLength();
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      BytesColumnVector vec = (BytesColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          int itemLength = Math.min(vec.length[0], maxLength);
          if (useDictionaryEncoding) {
            int id = dictionary.add(vec.vector[0], vec.start[0], itemLength);
            for(int i=0; i < length; ++i) {
              rows.add(id);
            }
          } else {
            for(int i=0; i < length; ++i) {
              directStreamOutput.write(vec.vector[0], vec.start[0],
                  itemLength);
              lengthOutput.write(itemLength);
            }
          }
          indexStatistics.updateString(vec.vector[0], vec.start[0],
              itemLength, length);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              // translate from UTF-8 to the default charset
              bloomFilter.addString(new String(vec.vector[0],
                  vec.start[0], itemLength,
                  StandardCharsets.UTF_8));
            }
            bloomFilterUtf8.addBytes(vec.vector[0],
                vec.start[0], itemLength);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            int itemLength = Math.min(vec.length[offset + i], maxLength);
            if (useDictionaryEncoding) {
              rows.add(dictionary.add(vec.vector[offset + i],
                  vec.start[offset + i], itemLength));
            } else {
              directStreamOutput.write(vec.vector[offset + i],
                  vec.start[offset + i], itemLength);
              lengthOutput.write(itemLength);
            }
            indexStatistics.updateString(vec.vector[offset + i],
                vec.start[offset + i], itemLength, 1);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                // translate from UTF-8 to the default charset
                bloomFilter.addString(new String(vec.vector[offset + i],
                    vec.start[offset + i], itemLength,
                    StandardCharsets.UTF_8));
              }
              bloomFilterUtf8.addBytes(vec.vector[offset + i],
                  vec.start[offset + i], itemLength);
            }
          }
        }
      }
    }
  }

  private static class BinaryTreeWriter extends TreeWriter {
    private final PositionedOutputStream stream;
    private final IntegerWriter length;
    private boolean isDirectV2 = true;

    BinaryTreeWriter(int columnId,
                     TypeDescription schema,
                     StreamFactory writer,
                     boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.stream = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      this.isDirectV2 = isNewWriteFormat(writer);
      this.length = createIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder result = super.getEncoding();
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
      return result;
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      BytesColumnVector vec = (BytesColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          for(int i=0; i < length; ++i) {
            stream.write(vec.vector[0], vec.start[0],
                  vec.length[0]);
            this.length.write(vec.length[0]);
          }
          indexStatistics.updateBinary(vec.vector[0], vec.start[0],
              vec.length[0], length);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
            }
            bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            stream.write(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]);
            this.length.write(vec.length[offset + i]);
            indexStatistics.updateBinary(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i], 1);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addBytes(vec.vector[offset + i],
                    vec.start[offset + i], vec.length[offset + i]);
              }
              bloomFilterUtf8.addBytes(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]);
            }
          }
        }
      }
    }


    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      stream.flush();
      length.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      stream.getPosition(recorder);
      length.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + stream.getBufferSize() +
          length.estimateMemory();
    }
  }

  public static final int MILLIS_PER_SECOND = 1000;
  public static final String BASE_TIMESTAMP_STRING = "2015-01-01 00:00:00";

  private static class TimestampTreeWriter extends TreeWriter {
    private final IntegerWriter seconds;
    private final IntegerWriter nanos;
    private final boolean isDirectV2;
    private final TimeZone localTimezone;
    private final long baseEpochSecsLocalTz;

    TimestampTreeWriter(int columnId,
                     TypeDescription schema,
                     StreamFactory writer,
                     boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.isDirectV2 = isNewWriteFormat(writer);
      this.seconds = createIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.DATA), true, isDirectV2, writer);
      this.nanos = createIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.SECONDARY), false, isDirectV2, writer);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
      this.localTimezone = TimeZone.getDefault();
      // for unit tests to set different time zones
      this.baseEpochSecsLocalTz = Timestamp.valueOf(BASE_TIMESTAMP_STRING).getTime() / MILLIS_PER_SECOND;
      writer.useWriterTimeZone(true);
    }

    @Override
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder result = super.getEncoding();
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
      return result;
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      TimestampColumnVector vec = (TimestampColumnVector) vector;
      Timestamp val;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          val = vec.asScratchTimestamp(0);
          long millis = val.getTime();
          long utc = SerializationUtils.convertToUtc(localTimezone, millis);
          indexStatistics.updateTimestamp(utc);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(millis);
            }
            bloomFilterUtf8.addLong(utc);
          }
          final long secs = millis / MILLIS_PER_SECOND - baseEpochSecsLocalTz;
          final long nano = formatNanos(val.getNanos());
          for(int i=0; i < length; ++i) {
            seconds.write(secs);
            nanos.write(nano);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            val = vec.asScratchTimestamp(i + offset);
            long millis = val.getTime();
            long secs = millis / MILLIS_PER_SECOND - baseEpochSecsLocalTz;
            long utc = SerializationUtils.convertToUtc(localTimezone, millis);
            seconds.write(secs);
            nanos.write(formatNanos(val.getNanos()));
            indexStatistics.updateTimestamp(utc);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addLong(millis);
              }
              bloomFilterUtf8.addLong(utc);
            }
          }
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      seconds.flush();
      nanos.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    private static long formatNanos(int nanos) {
      if (nanos == 0) {
        return 0;
      } else if (nanos % 100 != 0) {
        return ((long) nanos) << 3;
      } else {
        nanos /= 100;
        int trailingZeros = 1;
        while (nanos % 10 == 0 && trailingZeros < 7) {
          nanos /= 10;
          trailingZeros += 1;
        }
        return ((long) nanos) << 3 | trailingZeros;
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      seconds.getPosition(recorder);
      nanos.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + seconds.estimateMemory() +
          nanos.estimateMemory();
    }
  }

  private static class DateTreeWriter extends TreeWriter {
    private final IntegerWriter writer;
    private final boolean isDirectV2;

    DateTreeWriter(int columnId,
                   TypeDescription schema,
                   StreamFactory writer,
                   boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      OutStream out = writer.createStream(id,
          OrcProto.Stream.Kind.DATA);
      this.isDirectV2 = isNewWriteFormat(writer);
      this.writer = createIntegerWriter(out, true, isDirectV2, writer);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      LongColumnVector vec = (LongColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          int value = (int) vec.vector[0];
          indexStatistics.updateDate(value);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(value);
            }
            bloomFilterUtf8.addLong(value);
          }
          for(int i=0; i < length; ++i) {
            writer.write(value);
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            int value = (int) vec.vector[i + offset];
            writer.write(value);
            indexStatistics.updateDate(value);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addLong(value);
              }
              bloomFilterUtf8.addLong(value);
            }
          }
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      writer.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      writer.getPosition(recorder);
    }

    @Override
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder result = super.getEncoding();
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
      return result;
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + writer.estimateMemory();
    }
  }

  private static class DecimalTreeWriter extends TreeWriter {
    private final PositionedOutputStream valueStream;

    // These scratch buffers allow us to serialize decimals much faster.
    private final long[] scratchLongs;
    private final byte[] scratchBuffer;

    private final IntegerWriter scaleStream;
    private final boolean isDirectV2;

    DecimalTreeWriter(int columnId,
                        TypeDescription schema,
                        StreamFactory writer,
                        boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.isDirectV2 = isNewWriteFormat(writer);
      valueStream = writer.createStream(id, OrcProto.Stream.Kind.DATA);
      scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
      scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
      this.scaleStream = createIntegerWriter(writer.createStream(id,
          OrcProto.Stream.Kind.SECONDARY), true, isDirectV2, writer);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder result = super.getEncoding();
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
      return result;
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      DecimalColumnVector vec = (DecimalColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          HiveDecimalWritable value = vec.vector[0];
          indexStatistics.updateDecimal(value);
          if (createBloomFilter) {
            String str = value.toString(scratchBuffer);
            if (bloomFilter != null) {
              bloomFilter.addString(str);
            }
            bloomFilterUtf8.addString(str);
          }
          for(int i=0; i < length; ++i) {
            value.serializationUtilsWrite(valueStream,
                                          scratchLongs);
            scaleStream.write(value.scale());
          }
        }
      } else {
        for(int i=0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            HiveDecimalWritable value = vec.vector[i + offset];
            value.serializationUtilsWrite(valueStream, scratchLongs);
            scaleStream.write(value.scale());
            indexStatistics.updateDecimal(value);
            if (createBloomFilter) {
              String str = value.toString(scratchBuffer);
              if (bloomFilter != null) {
                bloomFilter.addString(str);
              }
              bloomFilterUtf8.addString(str);
            }
          }
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      valueStream.flush();
      scaleStream.flush();
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      valueStream.getPosition(recorder);
      scaleStream.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + valueStream.getBufferSize() +
          scaleStream.estimateMemory();
    }
  }

  private static class StructTreeWriter extends TreeWriter {
    StructTreeWriter(int columnId,
                     TypeDescription schema,
                     StreamFactory writer,
                     boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      List<TypeDescription> children = schema.getChildren();
      childrenWriters = new TreeWriter[children.size()];
      for(int i=0; i < childrenWriters.length; ++i) {
        childrenWriters[i] = createTreeWriter(
          children.get(i), writer,
          true);
      }
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void writeRootBatch(VectorizedRowBatch batch, int offset,
                        int length) throws IOException {
      // update the statistics for the root column
      indexStatistics.increment(length);
      // I'm assuming that the root column isn't nullable so that I don't need
      // to update isPresent.
      for(int i=0; i < childrenWriters.length; ++i) {
        childrenWriters[i].writeBatch(batch.cols[i], offset, length);
      }
    }

    private static void writeFields(StructColumnVector vector,
                                    TreeWriter[] childrenWriters,
                                    int offset, int length) throws IOException {
      for(int field=0; field < childrenWriters.length; ++field) {
        childrenWriters[field].writeBatch(vector.fields[field], offset, length);
      }
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      StructColumnVector vec = (StructColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          writeFields(vec, childrenWriters, offset, length);
        }
      } else if (vector.noNulls) {
        writeFields(vec, childrenWriters, offset, length);
      } else {
        // write the records in runs
        int currentRun = 0;
        boolean started = false;
        for(int i=0; i < length; ++i) {
          if (!vec.isNull[i + offset]) {
            if (!started) {
              started = true;
              currentRun = i;
            }
          } else if (started) {
            started = false;
            writeFields(vec, childrenWriters, offset + currentRun,
                i - currentRun);
          }
        }
        if (started) {
          writeFields(vec, childrenWriters, offset + currentRun,
              length - currentRun);
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder, requiredIndexEntries);
      }
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }
  }

  private static class ListTreeWriter extends TreeWriter {
    private final IntegerWriter lengths;
    private final boolean isDirectV2;

    ListTreeWriter(int columnId,
                   TypeDescription schema,
                   StreamFactory writer,
                   boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.isDirectV2 = isNewWriteFormat(writer);
      childrenWriters = new TreeWriter[1];
      childrenWriters[0] =
        createTreeWriter(schema.getChildren().get(0), writer, true);
      lengths = createIntegerWriter(writer.createStream(columnId,
          OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder result = super.getEncoding();
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
      return result;
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      ListColumnVector vec = (ListColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          int childOffset = (int) vec.offsets[0];
          int childLength = (int) vec.lengths[0];
          for(int i=0; i < length; ++i) {
            lengths.write(childLength);
            childrenWriters[0].writeBatch(vec.child, childOffset, childLength);
          }
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(childLength);
            }
            bloomFilterUtf8.addLong(childLength);
          }
        }
      } else {
        // write the elements in runs
        int currentOffset = 0;
        int currentLength = 0;
        for(int i=0; i < length; ++i) {
          if (!vec.isNull[i + offset]) {
            int nextLength = (int) vec.lengths[offset + i];
            int nextOffset = (int) vec.offsets[offset + i];
            lengths.write(nextLength);
            if (currentLength == 0) {
              currentOffset = nextOffset;
              currentLength = nextLength;
            } else if (currentOffset + currentLength != nextOffset) {
              childrenWriters[0].writeBatch(vec.child, currentOffset,
                  currentLength);
              currentOffset = nextOffset;
              currentLength = nextLength;
            } else {
              currentLength += nextLength;
            }
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addLong(nextLength);
              }
              bloomFilterUtf8.addLong(nextLength);
            }
          }
        }
        if (currentLength != 0) {
          childrenWriters[0].writeBatch(vec.child, currentOffset,
              currentLength);
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      lengths.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder, requiredIndexEntries);
      }
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      lengths.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + lengths.estimateMemory();
    }
  }

  private static class MapTreeWriter extends TreeWriter {
    private final IntegerWriter lengths;
    private final boolean isDirectV2;

    MapTreeWriter(int columnId,
                  TypeDescription schema,
                  StreamFactory writer,
                  boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      this.isDirectV2 = isNewWriteFormat(writer);
      childrenWriters = new TreeWriter[2];
      List<TypeDescription> children = schema.getChildren();
      childrenWriters[0] =
        createTreeWriter(children.get(0), writer, true);
      childrenWriters[1] =
        createTreeWriter(children.get(1), writer, true);
      lengths = createIntegerWriter(writer.createStream(columnId,
          OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    OrcProto.ColumnEncoding.Builder getEncoding() {
      OrcProto.ColumnEncoding.Builder result = super.getEncoding();
      if (isDirectV2) {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
      } else {
        result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
      }
      return result;
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      MapColumnVector vec = (MapColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          int childOffset = (int) vec.offsets[0];
          int childLength = (int) vec.lengths[0];
          for(int i=0; i < length; ++i) {
            lengths.write(childLength);
            childrenWriters[0].writeBatch(vec.keys, childOffset, childLength);
            childrenWriters[1].writeBatch(vec.values, childOffset, childLength);
          }
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(childLength);
            }
            bloomFilterUtf8.addLong(childLength);
          }
        }
      } else {
        // write the elements in runs
        int currentOffset = 0;
        int currentLength = 0;
        for(int i=0; i < length; ++i) {
          if (!vec.isNull[i + offset]) {
            int nextLength = (int) vec.lengths[offset + i];
            int nextOffset = (int) vec.offsets[offset + i];
            lengths.write(nextLength);
            if (currentLength == 0) {
              currentOffset = nextOffset;
              currentLength = nextLength;
            } else if (currentOffset + currentLength != nextOffset) {
              childrenWriters[0].writeBatch(vec.keys, currentOffset,
                  currentLength);
              childrenWriters[1].writeBatch(vec.values, currentOffset,
                  currentLength);
              currentOffset = nextOffset;
              currentLength = nextLength;
            } else {
              currentLength += nextLength;
            }
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addLong(nextLength);
              }
              bloomFilterUtf8.addLong(nextLength);
            }
          }
        }
        if (currentLength != 0) {
          childrenWriters[0].writeBatch(vec.keys, currentOffset,
              currentLength);
          childrenWriters[1].writeBatch(vec.values, currentOffset,
              currentLength);
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      lengths.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder, requiredIndexEntries);
      }
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      lengths.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + lengths.estimateMemory();
    }
  }

  private static class UnionTreeWriter extends TreeWriter {
    private final RunLengthByteWriter tags;

    UnionTreeWriter(int columnId,
                  TypeDescription schema,
                  StreamFactory writer,
                  boolean nullable) throws IOException {
      super(columnId, schema, writer, nullable);
      List<TypeDescription> children = schema.getChildren();
      childrenWriters = new TreeWriter[children.size()];
      for(int i=0; i < childrenWriters.length; ++i) {
        childrenWriters[i] =
            createTreeWriter(children.get(i), writer, true);
      }
      tags =
        new RunLengthByteWriter(writer.createStream(columnId,
            OrcProto.Stream.Kind.DATA));
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void writeBatch(ColumnVector vector, int offset,
                    int length) throws IOException {
      super.writeBatch(vector, offset, length);
      UnionColumnVector vec = (UnionColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          byte tag = (byte) vec.tags[0];
          for(int i=0; i < length; ++i) {
            tags.write(tag);
          }
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(tag);
            }
            bloomFilterUtf8.addLong(tag);
          }
          childrenWriters[tag].writeBatch(vec.fields[tag], offset, length);
        }
      } else {
        // write the records in runs of the same tag
        int[] currentStart = new int[vec.fields.length];
        int[] currentLength = new int[vec.fields.length];
        for(int i=0; i < length; ++i) {
          // only need to deal with the non-nulls, since the nulls were dealt
          // with in the super method.
          if (vec.noNulls || !vec.isNull[i + offset]) {
            byte tag = (byte) vec.tags[offset + i];
            tags.write(tag);
            if (currentLength[tag] == 0) {
              // start a new sequence
              currentStart[tag] = i + offset;
              currentLength[tag] = 1;
            } else if (currentStart[tag] + currentLength[tag] == i + offset) {
              // ok, we are extending the current run for that tag.
              currentLength[tag] += 1;
            } else {
              // otherwise, we need to close off the old run and start a new one
              childrenWriters[tag].writeBatch(vec.fields[tag],
                  currentStart[tag], currentLength[tag]);
              currentStart[tag] = i + offset;
              currentLength[tag] = 1;
            }
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addLong(tag);
              }
              bloomFilterUtf8.addLong(tag);
            }
          }
        }
        // write out any left over sequences
        for(int tag=0; tag < currentStart.length; ++tag) {
          if (currentLength[tag] != 0) {
            childrenWriters[tag].writeBatch(vec.fields[tag], currentStart[tag],
                currentLength[tag]);
          }
        }
      }
    }

    @Override
    void writeStripe(OrcProto.StripeFooter.Builder builder,
                     int requiredIndexEntries) throws IOException {
      super.writeStripe(builder, requiredIndexEntries);
      tags.flush();
      for(TreeWriter child: childrenWriters) {
        child.writeStripe(builder, requiredIndexEntries);
      }
      if (rowIndexPosition != null) {
        recordPosition(rowIndexPosition);
      }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
      super.recordPosition(recorder);
      tags.getPosition(recorder);
    }

    @Override
    long estimateMemory() {
      return super.estimateMemory() + tags.estimateMemory();
    }
  }

  private static TreeWriter createTreeWriter(TypeDescription schema,
                                             StreamFactory streamFactory,
                                             boolean nullable) throws IOException {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case BYTE:
        return new ByteTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case SHORT:
      case INT:
      case LONG:
        return new IntegerTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case FLOAT:
        return new FloatTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case DOUBLE:
        return new DoubleTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case STRING:
        return new StringTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case CHAR:
        return new CharTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case VARCHAR:
        return new VarcharTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case BINARY:
        return new BinaryTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case TIMESTAMP:
        return new TimestampTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case DATE:
        return new DateTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case DECIMAL:
        return new DecimalTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory,  nullable);
      case STRUCT:
        return new StructTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case MAP:
        return new MapTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case LIST:
        return new ListTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      case UNION:
        return new UnionTreeWriter(streamFactory.getNextColumnId(),
            schema, streamFactory, nullable);
      default:
        throw new IllegalArgumentException("Bad category: " +
            schema.getCategory());
    }
  }

  private static void writeTypes(OrcProto.Footer.Builder builder,
                                 TypeDescription schema) {
    OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
    List<TypeDescription> children = schema.getChildren();
    switch (schema.getCategory()) {
      case BOOLEAN:
        type.setKind(OrcProto.Type.Kind.BOOLEAN);
        break;
      case BYTE:
        type.setKind(OrcProto.Type.Kind.BYTE);
        break;
      case SHORT:
        type.setKind(OrcProto.Type.Kind.SHORT);
        break;
      case INT:
        type.setKind(OrcProto.Type.Kind.INT);
        break;
      case LONG:
        type.setKind(OrcProto.Type.Kind.LONG);
        break;
      case FLOAT:
        type.setKind(OrcProto.Type.Kind.FLOAT);
        break;
      case DOUBLE:
        type.setKind(OrcProto.Type.Kind.DOUBLE);
        break;
      case STRING:
        type.setKind(OrcProto.Type.Kind.STRING);
        break;
      case CHAR:
        type.setKind(OrcProto.Type.Kind.CHAR);
        type.setMaximumLength(schema.getMaxLength());
        break;
      case VARCHAR:
        type.setKind(OrcProto.Type.Kind.VARCHAR);
        type.setMaximumLength(schema.getMaxLength());
        break;
      case BINARY:
        type.setKind(OrcProto.Type.Kind.BINARY);
        break;
      case TIMESTAMP:
        type.setKind(OrcProto.Type.Kind.TIMESTAMP);
        break;
      case DATE:
        type.setKind(OrcProto.Type.Kind.DATE);
        break;
      case DECIMAL:
        type.setKind(OrcProto.Type.Kind.DECIMAL);
        type.setPrecision(schema.getPrecision());
        type.setScale(schema.getScale());
        break;
      case LIST:
        type.setKind(OrcProto.Type.Kind.LIST);
        type.addSubtypes(children.get(0).getId());
        break;
      case MAP:
        type.setKind(OrcProto.Type.Kind.MAP);
        for(TypeDescription t: children) {
          type.addSubtypes(t.getId());
        }
        break;
      case STRUCT:
        type.setKind(OrcProto.Type.Kind.STRUCT);
        for(TypeDescription t: children) {
          type.addSubtypes(t.getId());
        }
        for(String field: schema.getFieldNames()) {
          type.addFieldNames(field);
        }
        break;
      case UNION:
        type.setKind(OrcProto.Type.Kind.UNION);
        for(TypeDescription t: children) {
          type.addSubtypes(t.getId());
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown category: " +
          schema.getCategory());
    }
    builder.addTypes(type);
    if (children != null) {
      for(TypeDescription child: children) {
        writeTypes(builder, child);
      }
    }
  }

  private void createRowIndexEntry() throws IOException {
    treeWriter.createRowIndexEntry();
    rowsInIndex = 0;
  }

  private void flushStripe() throws IOException {
    if (buildIndex && rowsInIndex != 0) {
      createRowIndexEntry();
    }
    if (rowsInStripe != 0) {
      if (callback != null) {
        callback.preStripeWrite(callbackContext);
      }
      // finalize the data for the stripe
      int requiredIndexEntries = rowIndexStride == 0 ? 0 :
          (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
      OrcProto.StripeFooter.Builder builder =
          OrcProto.StripeFooter.newBuilder();
      treeWriter.writeStripe(builder, requiredIndexEntries);
      OrcProto.StripeInformation.Builder dirEntry =
          OrcProto.StripeInformation.newBuilder()
              .setNumberOfRows(rowsInStripe);
      physicalWriter.finalizeStripe(builder, dirEntry);
      stripes.add(dirEntry.build());
      rowCount += rowsInStripe;
      rowsInStripe = 0;
    }
  }

  private long computeRawDataSize() {
    return getRawDataSize(treeWriter, schema);
  }

  private long getRawDataSize(TreeWriter child,
                              TypeDescription schema) {
    long total = 0;
    long numVals = child.fileStatistics.getNumberOfValues();
    switch (schema.getCategory()) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case FLOAT:
        return numVals * JavaDataModel.get().primitive1();
      case LONG:
      case DOUBLE:
        return numVals * JavaDataModel.get().primitive2();
      case STRING:
      case VARCHAR:
      case CHAR:
        // ORC strings are converted to java Strings. so use JavaDataModel to
        // compute the overall size of strings
        StringColumnStatistics scs = (StringColumnStatistics) child.fileStatistics;
        numVals = numVals == 0 ? 1 : numVals;
        int avgStringLen = (int) (scs.getSum() / numVals);
        return numVals * JavaDataModel.get().lengthForStringOfLength(avgStringLen);
      case DECIMAL:
        return numVals * JavaDataModel.get().lengthOfDecimal();
      case DATE:
        return numVals * JavaDataModel.get().lengthOfDate();
      case BINARY:
        // get total length of binary blob
        BinaryColumnStatistics bcs = (BinaryColumnStatistics) child.fileStatistics;
        return bcs.getSum();
      case TIMESTAMP:
        return numVals * JavaDataModel.get().lengthOfTimestamp();
      case LIST:
      case MAP:
      case UNION:
      case STRUCT: {
        TreeWriter[] childWriters = child.getChildrenWriters();
        List<TypeDescription> childTypes = schema.getChildren();
        for (int i=0; i < childWriters.length; ++i) {
          total += getRawDataSize(childWriters[i], childTypes.get(i));
        }
        break;
      }
      default:
        LOG.debug("Unknown object inspector category.");
        break;
    }
    return total;
  }

  private OrcProto.CompressionKind writeCompressionKind(CompressionKind kind) {
    switch (kind) {
      case NONE: return OrcProto.CompressionKind.NONE;
      case ZLIB: return OrcProto.CompressionKind.ZLIB;
      case SNAPPY: return OrcProto.CompressionKind.SNAPPY;
      case LZO: return OrcProto.CompressionKind.LZO;
      case LZ4: return OrcProto.CompressionKind.LZ4;
      default:
        throw new IllegalArgumentException("Unknown compression " + kind);
    }
  }

  private void writeFileStatistics(OrcProto.Footer.Builder builder,
                                   TreeWriter writer) throws IOException {
    builder.addStatistics(writer.fileStatistics.serialize());
    for(TreeWriter child: writer.getChildrenWriters()) {
      writeFileStatistics(builder, child);
    }
  }

  private void writeMetadata() throws IOException {
    OrcProto.Metadata.Builder builder = OrcProto.Metadata.newBuilder();
    for(OrcProto.StripeStatistics.Builder ssb : treeWriter.stripeStatsBuilders) {
      builder.addStripeStats(ssb.build());
    }
    physicalWriter.writeFileMetadata(builder);
  }

  private long writePostScript() throws IOException {
    OrcProto.PostScript.Builder builder =
        OrcProto.PostScript.newBuilder()
            .setCompression(writeCompressionKind(compress))
            .setMagic(OrcFile.MAGIC)
            .addVersion(version.getMajor())
            .addVersion(version.getMinor())
            .setWriterVersion(writerVersion.getId());
    if (compress != CompressionKind.NONE) {
      builder.setCompressionBlockSize(bufferSize);
    }
    return physicalWriter.writePostScript(builder);
  }

  private long writeFooter() throws IOException {
    writeMetadata();
    OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();
    builder.setNumberOfRows(rowCount);
    builder.setRowIndexStride(rowIndexStride);
    rawDataSize = computeRawDataSize();
    // serialize the types
    writeTypes(builder, schema);
    // add the stripe information
    for(OrcProto.StripeInformation stripe: stripes) {
      builder.addStripes(stripe);
    }
    // add the column statistics
    writeFileStatistics(builder, treeWriter);
    // add all of the user metadata
    for(Map.Entry<String, ByteString> entry: userMetadata.entrySet()) {
      builder.addMetadata(OrcProto.UserMetadataItem.newBuilder()
        .setName(entry.getKey()).setValue(entry.getValue()));
    }
    physicalWriter.writeFileFooter(builder);
    return writePostScript();
  }

  @Override
  public TypeDescription getSchema() {
    return schema;
  }

  @Override
  public void addUserMetadata(String name, ByteBuffer value) {
    userMetadata.put(name, ByteString.copyFrom(value));
  }

  @Override
  public void addRowBatch(VectorizedRowBatch batch) throws IOException {
    if (buildIndex) {
      // Batch the writes up to the rowIndexStride so that we can get the
      // right size indexes.
      int posn = 0;
      while (posn < batch.size) {
        int chunkSize = Math.min(batch.size - posn,
            rowIndexStride - rowsInIndex);
        treeWriter.writeRootBatch(batch, posn, chunkSize);
        posn += chunkSize;
        rowsInIndex += chunkSize;
        rowsInStripe += chunkSize;
        if (rowsInIndex >= rowIndexStride) {
          createRowIndexEntry();
        }
      }
    } else {
      rowsInStripe += batch.size;
      treeWriter.writeRootBatch(batch, 0, batch.size);
    }
    memoryManager.addedRow(batch.size);
  }

  @Override
  public void close() throws IOException {
    if (callback != null) {
      callback.preFooterWrite(callbackContext);
    }
    // remove us from the memory manager so that we don't get any callbacks
    memoryManager.removeWriter(path);
    // actually close the file
    flushStripe();
    lastFlushOffset = writeFooter();
    physicalWriter.close();
  }

  /**
   * Raw data size will be compute when writing the file footer. Hence raw data
   * size value will be available only after closing the writer.
   */
  @Override
  public long getRawDataSize() {
    return rawDataSize;
  }

  /**
   * Row count gets updated when flushing the stripes. To get accurate row
   * count call this method after writer is closed.
   */
  @Override
  public long getNumberOfRows() {
    return rowCount;
  }

  @Override
  public long writeIntermediateFooter() throws IOException {
    // flush any buffered rows
    flushStripe();
    // write a footer
    if (stripesAtLastFlush != stripes.size()) {
      if (callback != null) {
        callback.preFooterWrite(callbackContext);
      }
      lastFlushOffset = writeFooter();
      stripesAtLastFlush = stripes.size();
      physicalWriter.flush();
    }
    return lastFlushOffset;
  }

  static void checkArgument(boolean expression, String message) {
    if (!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  @Override
  public void appendStripe(byte[] stripe, int offset, int length,
      StripeInformation stripeInfo,
      OrcProto.StripeStatistics stripeStatistics) throws IOException {
    checkArgument(stripe != null, "Stripe must not be null");
    checkArgument(length <= stripe.length,
        "Specified length must not be greater specified array length");
    checkArgument(stripeInfo != null, "Stripe information must not be null");
    checkArgument(stripeStatistics != null,
        "Stripe statistics must not be null");

    rowsInStripe = stripeInfo.getNumberOfRows();
    // update stripe information
    OrcProto.StripeInformation.Builder dirEntry = OrcProto.StripeInformation
        .newBuilder()
        .setNumberOfRows(rowsInStripe)
        .setIndexLength(stripeInfo.getIndexLength())
        .setDataLength(stripeInfo.getDataLength())
        .setFooterLength(stripeInfo.getFooterLength());
    physicalWriter.appendRawStripe(ByteBuffer.wrap(stripe, offset, length),
        dirEntry);

    // since we have already written the stripe, just update stripe statistics
    treeWriter.stripeStatsBuilders.add(stripeStatistics.toBuilder());

    // update file level statistics
    updateFileStatistics(stripeStatistics);

    stripes.add(dirEntry.build());

    // reset it after writing the stripe
    rowCount += rowsInStripe;
    rowsInStripe = 0;
  }

  private void updateFileStatistics(OrcProto.StripeStatistics stripeStatistics) {
    List<OrcProto.ColumnStatistics> cs = stripeStatistics.getColStatsList();
    List<TreeWriter> allWriters = getAllColumnTreeWriters(treeWriter);
    for (int i = 0; i < allWriters.size(); i++) {
      allWriters.get(i).fileStatistics.merge(ColumnStatisticsImpl.deserialize(cs.get(i)));
    }
  }

  private List<TreeWriter> getAllColumnTreeWriters(TreeWriter rootTreeWriter) {
    List<TreeWriter> result = new ArrayList<>();
    getAllColumnTreeWritersImpl(rootTreeWriter, result);
    return result;
  }

  private void getAllColumnTreeWritersImpl(TreeWriter tw,
      List<TreeWriter> result) {
    result.add(tw);
    for (TreeWriter child : tw.childrenWriters) {
      getAllColumnTreeWritersImpl(child, result);
    }
  }

  @Override
  public void appendUserMetadata(List<OrcProto.UserMetadataItem> userMetadata) {
    if (userMetadata != null) {
      for (OrcProto.UserMetadataItem item : userMetadata) {
        this.userMetadata.put(item.getName(), item.getValue());
      }
    }
  }

  @Override
  public ColumnStatistics[] getStatistics()
      throws IOException {
    // Generate the stats
    OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();

    // add the column statistics
    writeFileStatistics(builder, treeWriter);
    return ReaderImpl.deserializeStats(builder.getStatisticsList());
  }

  public CompressionCodec getCompressionCodec() {
    return physicalWriter.getCompressionCodec();
  }
}
