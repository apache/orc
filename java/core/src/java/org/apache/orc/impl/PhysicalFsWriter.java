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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.protobuf.CodedOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.writer.StreamOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PhysicalFsWriter implements PhysicalWriter {
  private static final Logger LOG = LoggerFactory.getLogger(PhysicalFsWriter.class);

  private static final int HDFS_BUFFER_SIZE = 256 * 1024;

  private FSDataOutputStream rawWriter;
  // the compressed metadata information outStream
  private OutStream writer;
  // a protobuf outStream around streamFactory
  private CodedOutputStream protobufWriter;

  private final Path path;
  private final HadoopShims shims;
  private final long blockSize;
  private final int bufferSize;
  private final int maxPadding;
  private final CompressionKind compress;
  private final OrcFile.CompressionStrategy compressionStrategy;
  private CompressionCodec codec;
  private final boolean addBlockPadding;
  private final boolean writeVariableLengthBlocks;

  // the streams that make up the current stripe
  private final Map<StreamName, BufferedStream> streams =
    new TreeMap<>();

  private long headerLength;
  private long stripeStart;
  // The position of the last time we wrote a short block, which becomes the
  // natural blocks
  private long blockOffset;
  private int metadataLength;
  private int footerLength;

  public PhysicalFsWriter(FileSystem fs,
                          Path path,
                          OrcFile.WriterOptions opts) throws IOException {
    this.path = path;
    long defaultStripeSize = opts.getStripeSize();
    this.addBlockPadding = opts.getBlockPadding();
    if (opts.isEnforceBufferSize()) {
      this.bufferSize = opts.getBufferSize();
    } else {
      this.bufferSize = WriterImpl.getEstimatedBufferSize(defaultStripeSize,
          opts.getSchema().getMaximumId() + 1,
          opts.getBufferSize());
    }
    this.compress = opts.getCompress();
    this.compressionStrategy = opts.getCompressionStrategy();
    this.maxPadding = (int) (opts.getPaddingTolerance() * defaultStripeSize);
    this.blockSize = opts.getBlockSize();
    LOG.info("ORC writer created for path: {} with stripeSize: {} blockSize: {}" +
        " compression: {} bufferSize: {}", path, defaultStripeSize, blockSize,
        compress, bufferSize);
    rawWriter = fs.create(path, opts.getOverwrite(), HDFS_BUFFER_SIZE,
        fs.getDefaultReplication(path), blockSize);
    blockOffset = 0;
    codec = OrcCodecPool.getCodec(compress);
    StreamOptions options = new StreamOptions(bufferSize);
    if (codec != null) {
      options.withCodec(codec, codec.createOptions());
    }
    writer = new OutStream("metadata", options,
        new DirectStream(rawWriter));
    protobufWriter = CodedOutputStream.newInstance(writer);
    writeVariableLengthBlocks = opts.getWriteVariableLengthBlocks();
    shims = opts.getHadoopShims();
  }

  @Override
  public CompressionCodec getCompressionCodec() {
    return codec;
  }

  /**
   * Get the number of bytes for a file in a given column
   * by finding all the streams (not suppressed)
   * for a given column and returning the sum of their sizes.
   * excludes index
   *
   * @param column column from which to get file size
   * @return number of bytes for the given column
   */
  @Override
  public long getFileBytes(final int column) {
    long size = 0;
    for (final Map.Entry<StreamName, BufferedStream> pair: streams.entrySet()) {
      final BufferedStream receiver = pair.getValue();
      if(!receiver.isSuppressed) {

        final StreamName name = pair.getKey();
        if(name.getColumn() == column && name.getArea() != StreamName.Area.INDEX ) {
          size += receiver.getOutputSize();
        }
      }

    }
    return size;
  }

  private static final byte[] ZEROS = new byte[64*1024];

  private static void writeZeros(OutputStream output,
                                 long remaining) throws IOException {
    while (remaining > 0) {
      long size = Math.min(ZEROS.length, remaining);
      output.write(ZEROS, 0, (int) size);
      remaining -= size;
    }
  }

  /**
   * Do any required shortening of the HDFS block or padding to avoid stradling
   * HDFS blocks. This is called before writing the current stripe.
   * @param stripeSize the number of bytes in the current stripe
   */
  private void padStripe(long stripeSize) throws IOException {
    this.stripeStart = rawWriter.getPos();
    long previousBytesInBlock = (stripeStart - blockOffset) % blockSize;
    // We only have options if this isn't the first stripe in the block
    if (previousBytesInBlock > 0) {
      if (previousBytesInBlock + stripeSize >= blockSize) {
        // Try making a short block
        if (writeVariableLengthBlocks &&
            shims.endVariableLengthBlock(rawWriter)) {
          blockOffset = stripeStart;
        } else if (addBlockPadding) {
          // if we cross the block boundary, figure out what we should do
          long padding = blockSize - previousBytesInBlock;
          if (padding <= maxPadding) {
            writeZeros(rawWriter, padding);
            stripeStart += padding;
          }
        }
      }
    }
  }

  /**
   * An output receiver that writes the ByteBuffers to the output stream
   * as they are received.
   */
  private static class DirectStream implements OutputReceiver {
    private final FSDataOutputStream output;

    DirectStream(FSDataOutputStream output) {
      this.output = output;
    }

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          buffer.remaining());
    }

    @Override
    public void suppress() {
      throw new UnsupportedOperationException("Can't suppress direct stream");
    }
  }

  private void writeStripeFooter(OrcProto.StripeFooter footer,
                                 long dataSize,
                                 long indexSize,
                                 OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    footer.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    dirEntry.setOffset(stripeStart);
    dirEntry.setFooterLength(rawWriter.getPos() - stripeStart - dataSize - indexSize);
  }

  @Override
  public void writeFileMetadata(OrcProto.Metadata.Builder builder) throws IOException {
    long startPosn = rawWriter.getPos();
    OrcProto.Metadata metadata = builder.build();
    metadata.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    this.metadataLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public void writeFileFooter(OrcProto.Footer.Builder builder) throws IOException {
    long bodyLength = rawWriter.getPos() - metadataLength;
    builder.setContentLength(bodyLength);
    builder.setHeaderLength(headerLength);
    long startPosn = rawWriter.getPos();
    OrcProto.Footer footer = builder.build();
    footer.writeTo(protobufWriter);
    protobufWriter.flush();
    writer.flush();
    this.footerLength = (int) (rawWriter.getPos() - startPosn);
  }

  @Override
  public long writePostScript(OrcProto.PostScript.Builder builder) throws IOException {
    builder.setFooterLength(footerLength);
    builder.setMetadataLength(metadataLength);
    OrcProto.PostScript ps = builder.build();
    // need to write this uncompressed
    long startPosn = rawWriter.getPos();
    ps.writeTo(rawWriter);
    long length = rawWriter.getPos() - startPosn;
    if (length > 255) {
      throw new IllegalArgumentException("PostScript too large at " + length);
    }
    rawWriter.writeByte((int)length);
    return rawWriter.getPos();
  }

  @Override
  public void close() throws IOException {
    // We don't use the codec directly but do give it out codec in getCompressionCodec;
    // that is used in tests, for boolean checks, and in StreamFactory. Some of the changes that
    // would get rid of this pattern require cross-project interface changes, so just return the
    // codec for now.
    OrcCodecPool.returnCodec(compress, codec);
    codec = null;
    rawWriter.close();
    rawWriter = null;
  }

  @Override
  public void flush() throws IOException {
    rawWriter.hflush();
  }

  @Override
  public void appendRawStripe(ByteBuffer buffer,
      OrcProto.StripeInformation.Builder dirEntry) throws IOException {
    long start = rawWriter.getPos();
    int length = buffer.remaining();
    long availBlockSpace = blockSize - (start % blockSize);

    // see if stripe can fit in the current hdfs block, else pad the remaining
    // space in the block
    if (length < blockSize && length > availBlockSpace &&
        addBlockPadding) {
      byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
      LOG.info(String.format("Padding ORC by %d bytes while merging..",
          availBlockSpace));
      start += availBlockSpace;
      while (availBlockSpace > 0) {
        int writeLen = (int) Math.min(availBlockSpace, pad.length);
        rawWriter.write(pad, 0, writeLen);
        availBlockSpace -= writeLen;
      }
    }
    rawWriter.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
        length);
    dirEntry.setOffset(start);
  }


  /**
   * This class is used to hold the contents of streams as they are buffered.
   * The TreeWriters write to the outStream and the codec compresses the
   * data as buffers fill up and stores them in the output list. When the
   * stripe is being written, the whole stream is written to the file.
   */
  private static final class BufferedStream implements OutputReceiver {
    private boolean isSuppressed = false;
    private final List<ByteBuffer> output = new ArrayList<>();

    @Override
    public void output(ByteBuffer buffer) {
      if (!isSuppressed) {
        output.add(buffer);
      }
    }

    public void suppress() {
      isSuppressed = true;
      output.clear();
    }

    /**
     * Write any saved buffers to the OutputStream if needed, and clears all the
     * buffers.
     */
    void spillToDiskAndClear(FSDataOutputStream raw
                                       ) throws IOException {
      if (!isSuppressed) {
        for (ByteBuffer buffer: output) {
          raw.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
            buffer.remaining());
        }
        output.clear();
      }
      isSuppressed = false;
    }

    /**
     * Get the number of bytes that will be written to the output.
     *
     * Assumes the stream writing into this receiver has already been flushed.
     * @return number of bytes
     */
    public long getOutputSize() {
      long result = 0;
      for (ByteBuffer buffer: output) {
        result += buffer.remaining();
      }
      return result;
    }
  }

  @Override
  public void finalizeStripe(OrcProto.StripeFooter.Builder footerBuilder,
                             OrcProto.StripeInformation.Builder dirEntry
                             ) throws IOException {
    long indexSize = 0;
    long dataSize = 0;
    for (Map.Entry<StreamName, BufferedStream> pair: streams.entrySet()) {
      BufferedStream receiver = pair.getValue();
      if (!receiver.isSuppressed) {
        long streamSize = receiver.getOutputSize();
        StreamName name = pair.getKey();
        footerBuilder.addStreams(OrcProto.Stream.newBuilder().setColumn(name.getColumn())
            .setKind(name.getKind()).setLength(streamSize));
        if (StreamName.Area.INDEX == name.getArea()) {
          indexSize += streamSize;
        } else {
          dataSize += streamSize;
        }
      }
    }
    dirEntry.setIndexLength(indexSize).setDataLength(dataSize);

    OrcProto.StripeFooter footer = footerBuilder.build();
    // Do we need to pad the file so the stripe doesn't straddle a block boundary?
    padStripe(indexSize + dataSize + footer.getSerializedSize());

    // write out the data streams
    for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
      pair.getValue().spillToDiskAndClear(rawWriter);
    }
    // Write out the footer.
    writeStripeFooter(footer, dataSize, indexSize, dirEntry);
  }

  @Override
  public void writeHeader() throws IOException {
    rawWriter.writeBytes(OrcFile.MAGIC);
    headerLength = rawWriter.getPos();
  }

  @Override
  public BufferedStream createDataStream(StreamName name) {
    BufferedStream result = streams.get(name);
    if (result == null) {
      result = new BufferedStream();
      streams.put(name, result);
    }
    return result;
  }

  StreamOptions getOptions(OrcProto.Stream.Kind kind) {
    StreamOptions options = new StreamOptions(bufferSize);
    if (codec != null) {
      options.withCodec(codec, WriterImpl.getCustomizedCodec(codec,
          compressionStrategy, kind));
    }
    return options;
  }

  @Override
  public void writeIndex(StreamName name,
                         OrcProto.RowIndex.Builder index) throws IOException {
    OutputStream stream = new OutStream(path.toString(),
        getOptions(name.getKind()), createDataStream(name));
    index.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public void writeBloomFilter(StreamName name,
                               OrcProto.BloomFilterIndex.Builder bloom
                               ) throws IOException {
    OutputStream stream = new OutStream(path.toString(),
        getOptions(name.getKind()), createDataStream(name));
    bloom.build().writeTo(stream);
    stream.flush();
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
