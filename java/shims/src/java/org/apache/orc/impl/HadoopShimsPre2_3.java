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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.shims.Configuration;
import org.apache.orc.shims.FileIO;
import org.apache.orc.shims.FileStatus;
import org.apache.orc.shims.PositionOutputStream;
import org.apache.orc.shims.SeekableInputStream;


/**
 * Shims for versions of Hadoop up to and including 2.2.x
 */
public class HadoopShimsPre2_3 implements HadoopShims {

  HadoopShimsPre2_3() {
  }

  @Override
  public DirectDecompressor getDirectDecompressor(
      DirectCompressionType codec) {
    return null;
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(SeekableInputStream in,
                                              ByteBufferPoolShim pool
                                              ) {
    /* not supported */
    return null;
  }

  @Override
  public boolean endVariableLengthBlock(OutputStream output) throws IOException {
    return false;
  }

  @Override
  public FileIO createFileIO(Supplier<Object> fs) {
    return new HadoopFileIO(fs);
  }

  @Override
  public FileIO createFileIO(String path, Configuration conf) throws IOException {
    Path p = new Path(path);
    FileSystem fs = p.getFileSystem(((HadoopConfiguration) conf).getHadoopConfig());
    return new HadoopFileIO(() -> fs);
  }

  @Override
  public Configuration createConfiguration(Properties tableProperties, Object hadoopConfig) {
    return new HadoopConfiguration(tableProperties, hadoopConfig);
  }

  private static final int HDFS_BUFFER_SIZE = 256 * 1024;

  static class HadoopFileStatus implements FileStatus {
    private final org.apache.hadoop.fs.FileStatus status;

    HadoopFileStatus(org.apache.hadoop.fs.FileStatus status) {
      this.status = status;
    }

    @Override
    public long getLength() {
      return status.getLen();
    }

    @Override
    public long getModificationTime() {
      return status.getModificationTime();
    }
  }

  static class HadoopFileIO implements FileIO {
    private final Supplier<Object> fs;

    HadoopFileIO(Supplier<Object> fs) {
      this.fs = fs;
    }

    FileSystem getFileSystem() {
      return (FileSystem) fs.get();
    }

    @Override
    public boolean exists(String name) throws IOException {
      return getFileSystem().exists(new Path(name));
    }

    @Override
    public FileStatus getFileStatus(String name) throws IOException {
      return new HadoopFileStatus(getFileSystem().getFileStatus(new Path(name)));
    }

    @Override
    public SeekableInputStream createInputFile(String name) throws IOException {
      return new HadoopSeekableInputStream(getFileSystem().open(new Path(name)));
    }

    @Override
    public PositionOutputStream createOutputFile(String name,
                                                 boolean overwrite,
                                                 long blockSize) throws IOException {
      Path path = new Path(name);
      FileSystem fileSystem = getFileSystem();
      return new HadoopPositionOutputStream(fileSystem.create(path, overwrite, HDFS_BUFFER_SIZE,
          fileSystem.getDefaultReplication(path), blockSize));
    }

    @Override
    public void delete(String name) throws IOException {
      getFileSystem().delete(new Path(name), false);
    }
  }

  static class HadoopPositionOutputStream implements PositionOutputStream {
    private final FSDataOutputStream inner;

    HadoopPositionOutputStream(FSDataOutputStream stream) {
      inner = stream;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      inner.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      inner.write(b);
    }

    @Override
    public long getPos() throws IOException {
      return inner.getPos();
    }

    @Override
    public void flush() throws IOException {
      inner.hflush();
    }

    @Override
    public void close() throws IOException {
      inner.close();
    }

    @Override
    public OutputStream getOutputStream() {
      return inner;
    }

    @Override
    public String toString() {
      return inner.toString();
    }
  }

  static class HadoopSeekableInputStream implements SeekableInputStream {
    private final FSDataInputStream inner;

    HadoopSeekableInputStream(FSDataInputStream stream) {
      inner = stream;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      inner.readFully(position, buffer, offset, length);
    }

    @Override
    public void seek(long offset) throws IOException {
      inner.seek(offset);
    }

    @Override
    public long getPosition() throws IOException {
      return inner.getPos();
    }

    @Override
    public InputStream getInputStream() {
      return inner;
    }

    @Override
    public void close() throws IOException {
      inner.close();
    }
  }

  static class HadoopConfiguration implements Configuration {
    private final org.apache.hadoop.conf.Configuration hadoopConfig;
    private final Properties tableProperties;

    HadoopConfiguration(Properties tableProperties,
                        Object hadoopConfig) {
      this.hadoopConfig =
          hadoopConfig != null && hadoopConfig instanceof org.apache.hadoop.conf.Configuration
              ? (org.apache.hadoop.conf.Configuration) hadoopConfig
              : new org.apache.hadoop.conf.Configuration();
      this.tableProperties = tableProperties;
    }

    @Override
    public String get(String key) {
      String result = null;
      if (tableProperties != null) {
        result = tableProperties.getProperty(key);
      }
      if (result == null) {
        result = hadoopConfig.get(key);
      }
      return result;
    }

    @Override
    public void set(String key, String value) {
      hadoopConfig.set(key, value);
    }

    /**
     * Should only be called when we are using Hadoop.
     * @return the Hadoop configuration
     */
    org.apache.hadoop.conf.Configuration getHadoopConfig() {
      return hadoopConfig;
    }
  }
}
