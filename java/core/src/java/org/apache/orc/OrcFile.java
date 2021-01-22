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

package org.apache.orc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.HadoopShimsFactory;
import org.apache.orc.impl.KeyProvider;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.impl.writer.WriterImplV2;
import org.apache.orc.shims.FileIO;


/**
 * Contains factory methods to read or write ORC files.
 */
public class OrcFile extends org.apache.orc.core.OrcFile {
  // unused
  protected OrcFile() {
  }

  public static class ReaderOptions extends org.apache.orc.core.OrcFile.ReaderOptions {
    private FileSystem filesystem;
    private final Configuration conf;

    // TODO: We can generalize FileMetada interface. Make OrcTail implement FileMetadata interface
    // and remove this class altogether. Both footer caching and llap caching just needs OrcTail.
    // For now keeping this around to avoid complex surgery
    private FileMetadata fileMetadata;

    public ReaderOptions(Configuration conf) {
      super(HadoopShimsFactory.get().createConfiguration(null, conf));
      this.conf = conf;
    }

    public ReaderOptions filesystem(FileSystem fs) {
      this.filesystem = fs;
      super.fileIO(HadoopShimsFactory.get().createFileIO(() -> fs));
      return this;
    }

    @Override
    public ReaderOptions fileIO(FileIO fio) {
      super.fileIO(fio);
      return this;
    }

    @Override
    public ReaderOptions maxLength(long val) {
      super.maxLength(val);
      return this;
    }

    @Override
    public ReaderOptions orcTail(OrcTail tail) {
      super.orcTail(tail);
      return this;
    }

    @Override
    public ReaderOptions setKeyProvider(KeyProvider provider) {
      super.setKeyProvider(provider);
      return this;
    }

    @Override
    public ReaderOptions convertToProlepticGregorian(boolean newValue) {
      super.convertToProlepticGregorian(newValue);
      return this;
    }

    @Override
    public ReaderOptions useUTCTimestamp(boolean value) {
      super.useUTCTimestamp(value);
      return this;
    }

    public FileSystem getFilesystem() {
      return filesystem;
    }

    /**
     * @deprecated Use {@link #orcTail(OrcTail)} instead.
     */
    public ReaderOptions fileMetadata(final FileMetadata metadata) {
      fileMetadata = metadata;
      return this;
    }

    public FileMetadata getFileMetadata() {
      return fileMetadata;
    }

    public Configuration getConfiguration() {
      return conf;
    }
  }

  public static ReaderOptions readerOptions(Configuration conf) {
    return new ReaderOptions(conf);
  }

  public static Reader createReader(Path path, ReaderOptions options) throws IOException {
    return new ReaderImpl(path, options);
  }

  /**
   * Options for creating ORC file writers.
   */
  public static class WriterOptions extends org.apache.orc.core.OrcFile.WriterOptions {
    private FileSystem fileSystemValue = null;
    private Configuration conf;

    protected WriterOptions(Properties tableProperties, Configuration conf) {
      super(HadoopShimsFactory.get().createConfiguration(tableProperties, conf));
      this.conf = conf;
    }

    /**
     * @return a SHALLOW clone
     */
    @Override
    public WriterOptions clone() {
      return (WriterOptions) super.clone();
    }

    /**
     * Provide the filesystem for the path, if the client has it available.
     * If it is not provided, it will be found from the path.
     */
    public WriterOptions fileSystem(FileSystem value) {
      fileSystemValue = value;
      super.fileIO(HadoopShimsFactory.get().createFileIO(() -> value));
      return this;
    }

    @Override
    public WriterOptions fileIO(FileIO value) {
      super.fileIO(value);
      return this;
    }

    @Override
    public WriterOptions overwrite(boolean value) {
      super.overwrite(value);
      return this;
    }

    @Override
    public WriterOptions stripeSize(long value) {
      super.stripeSize(value);
      return this;
    }

    @Override
    public WriterOptions blockSize(long value) {
      super.blockSize(value);
      return this;
    }

    @Override
    public WriterOptions rowIndexStride(int value) {
      super.rowIndexStride(value);
      return this;
    }

    @Override
    public WriterOptions bufferSize(int value) {
      super.bufferSize(value);
      return this;
    }

    @Override
    public WriterOptions enforceBufferSize() {
      super.enforceBufferSize();
      return this;
    }

    @Override
    public WriterOptions blockPadding(boolean value) {
      super.blockPadding(value);
      return this;
    }

    @Override
    public WriterOptions encodingStrategy(EncodingStrategy strategy) {
      super.encodingStrategy(strategy);
      return this;
    }

    @Override
    public WriterOptions paddingTolerance(double value) {
      super.paddingTolerance(value);
      return this;
    }

    @Override
    public WriterOptions bloomFilterColumns(String columns) {
      super.bloomFilterColumns(columns);
      return this;
    }

    @Override
    public WriterOptions bloomFilterFpp(double fpp) {
      super.bloomFilterFpp(fpp);
      return this;
    }

    @Override
    public WriterOptions compress(CompressionKind value) {
      super.compress(value);
      return this;
    }

    @Override
    public WriterOptions setSchema(TypeDescription schema) {
      super.setSchema(schema);
      return this;
    }

    @Override
    public WriterOptions version(Version value) {
      super.version(value);
      return this;
    }

    @Override
    public WriterOptions callback(WriterCallback callback) {
      super.callback(callback);
      return this;
    }

    @Override
    public WriterOptions bloomFilterVersion(BloomFilterVersion version) {
      super.bloomFilterVersion(version);
      return this;
    }

    @Override
    public WriterOptions physicalWriter(PhysicalWriter writer) {
      super.physicalWriter(writer);
      return this;
    }

    @Override
    public WriterOptions memory(MemoryManager value) {
      super.memory(value);
      return this;
    }

    @Override
    public WriterOptions writeVariableLengthBlocks(boolean value) {
      super.writeVariableLengthBlocks(value);
      return this;
    }

    @Override
    public WriterOptions setShims(HadoopShims value) {
      super.setShims(value);
      return this;
    }

    @Override
    protected WriterOptions writerVersion(WriterVersion version) {
      super.writerVersion(version);
      return this;
    }

    @Override
    public WriterOptions useUTCTimestamp(boolean value) {
      super.useUTCTimestamp(value);
      return this;
    }

    @Override
    public WriterOptions directEncodingColumns(String value) {
      super.directEncodingColumns(value);
      return this;
    }

    @Override
    public boolean getOverwrite() {
      return super.getOverwrite();
    }

    public WriterOptions encrypt(String value) {
      super.encrypt(value);
      return this;
    }

    @Override
    public boolean getBlockPadding() {
      return super.getBlockPadding();
    }

    public WriterOptions masks(String value) {
      super.masks(value);
      return this;
    }

    @Override
    public BloomFilterVersion getBloomFilterVersion() {
      return super.getBloomFilterVersion();
    }

    public WriterOptions setKeyVersion(String keyName, int version, EncryptionAlgorithm algorithm) {
      super.setKeyVersion(keyName, version, algorithm);
      return this;
    }

    @Override
    public WriterOptions setKeyProvider(KeyProvider provider) {
      super.setKeyProvider(provider);
      return this;
    }

    @Override
    public WriterOptions setProlepticGregorian(boolean newValue) {
      super.setProlepticGregorian(newValue);
      return this;
    }

    public FileSystem getFileSystem() {
      return fileSystemValue;
    }

    public Configuration getConfiguration() {
      return conf;
    }
  }

  /**
   * Create a set of writer options based on a configuration.
   * @param conf the configuration to use for values
   * @return A WriterOptions object that can be modified
   */
  public static WriterOptions writerOptions(Configuration conf) {
    return new WriterOptions(null, conf);
  }

  /**
   * Create a set of write options based on a set of table properties and
   * configuration.
   * @param tableProperties the properties of the table
   * @param conf the configuration of the query
   * @return a WriterOptions object that can be modified
   */
  public static WriterOptions writerOptions(Properties tableProperties, Configuration conf) {
    return new WriterOptions(tableProperties, conf);
  }

  /**
   * Create an ORC file writer. This is the public interface for creating
   * writers going forward and new options will only be added to this method.
   * @param path filename to write to
   * @param opts the options
   * @return a new ORC file writer
   */
  public static Writer createWriter(Path path, WriterOptions opts) throws IOException {
    switch (opts.getVersion()) {
      case V_0_11:
      case V_0_12:
        return new WriterImpl(null, path, opts);
      case UNSTABLE_PRE_2_0:
        return new WriterImplV2(null, path, opts);
      default:
        throw new IllegalArgumentException("Unknown version " +
            opts.getVersion());
    }
  }

  /**
   * Merges multiple ORC files that all have the same schema to produce
   * a single ORC file.
   * The merge will reject files that aren't compatible with the merged file
   * so the output list may be shorter than the input list.
   * The stripes are copied as serialized byte buffers.
   * The user metadata are merged and files that disagree on the value
   * associated with a key will be rejected.
   *
   * @param outputPath the output file
   * @param options the options for writing with although the options related
   *                to the input files' encodings are overridden
   * @param inputFiles the list of files to merge
   * @return the list of files that were successfully merged
   */
  public static List<Path> mergeFiles(Path outputPath, WriterOptions options, List<Path> inputFiles) throws IOException {
    List<String> inputList = new ArrayList<>(inputFiles.size());
    for (Path in : inputFiles) {
      inputList.add(in.toString());
    }
    List<String> stringList = org.apache.orc.core.OrcFile.mergeFiles(outputPath.toString(), options, inputList);
    List<Path> result = new ArrayList<>(stringList.size());
    for (String out : stringList) {
      result.add(new Path(out));
    }
    return result;
  }

  static boolean understandFormat(Path path, Reader reader) {
    return org.apache.orc.core.OrcFile.understandFormat(path.toString(), reader);
  }
}