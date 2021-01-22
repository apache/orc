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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Properties;
import java.util.function.Supplier;
import org.apache.orc.shims.Configuration;
import org.apache.orc.shims.FileIO;
import org.apache.orc.shims.FileStatus;
import org.apache.orc.shims.PositionOutputStream;
import org.apache.orc.shims.SeekableInputStream;

/**
 * Shims for non-Hadoop deployments
 */
public class NonHadoopShims implements HadoopShims {

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
  public boolean endVariableLengthBlock(OutputStream output) {
    return false;
  }

  @Override
  public FileIO createFileIO(Supplier<Object> fs) {
    return new NonHadoopFileIO();
  }

  @Override
  public FileIO createFileIO(String path, Configuration conf) throws IOException {
    return new NonHadoopFileIO();
  }

  @Override
  public Configuration createConfiguration(Properties tableProperties, Object config) {
    return new NonHadoopConfiguration(tableProperties, (Properties) config);
  }

  static class NonHadoopFileStatus implements FileStatus {
    private final File status;

    NonHadoopFileStatus(File status) {
      this.status = status;
    }

    @Override
    public long getLength() {
      return status.length();
    }

    @Override
    public long getModificationTime() {
      return status.lastModified();
    }
  }

  static class NonHadoopFileIO implements FileIO {

    @Override
    public boolean exists(String name) throws IOException {
      return new File(name).exists();
    }

    @Override
    public FileStatus getFileStatus(String name) {
      return new NonHadoopFileStatus(new File(name));
    }

    @Override
    public SeekableInputStream createInputFile(String name) throws IOException {
      return new NonHadoopSeekableInputStream(new RandomAccessFile(name , "r"));
    }

    @Override
    public PositionOutputStream createOutputFile(String name,
                                                 boolean overwrite,
                                                 long blockSize) throws IOException {
      if (overwrite) {
        new File(name).delete();
      }
      return new NonHadoopPositionOutputStream(name, new RandomAccessFile(name, "rw"));
    }

    @Override
    public void delete(String name) throws IOException {
      if (!new File(name).delete()) {
        throw new IOException("Failed to delete " + name);
      }
    }
  }

  static class NonHadoopPositionOutputStream implements PositionOutputStream {
    private final String name;
    private final RandomAccessFile inner;

    NonHadoopPositionOutputStream(String name, RandomAccessFile stream) {
      this.name = name;
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
      return inner.getFilePointer();
    }

    @Override
    public void flush() throws IOException {
      // it doesn't keep a buffer
    }

    @Override
    public void close() throws IOException {
      inner.close();
    }

    @Override
    public OutputStream getOutputStream() {
      return new OutputStream() {
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          inner.write(b, off, len);
        }

        @Override
        public void write(int b) throws IOException {
          inner.write(b);
        }

        @Override
        public void close() throws IOException {
          inner.close();
        }
      };
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static class NonHadoopSeekableInputStream implements SeekableInputStream {
    private final RandomAccessFile inner;

    NonHadoopSeekableInputStream(RandomAccessFile stream) {
      inner = stream;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      inner.seek(position);
      inner.readFully(buffer, offset, length);
    }

    @Override
    public void seek(long offset) throws IOException {
      inner.seek(offset);
    }

    @Override
    public long getPosition() throws IOException {
      return inner.getFilePointer();
    }

    @Override
    public InputStream getInputStream() {
      return new InputStream() {
        @Override
        public int read() throws IOException {
          return inner.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          return inner.read(b, off, len);
        }

        @Override
        public long skip(long bytes) throws IOException {
          return inner.skipBytes((int) Math.min(Integer.MAX_VALUE, bytes));
        }

        @Override
        public int available() throws IOException {
          return (int) Math.min(Integer.MAX_VALUE, inner.length() - inner.getFilePointer());
        }

        @Override
        public void close() throws IOException {
          inner.close();
        }
      };
    }

    @Override
    public void close() throws IOException {
      inner.close();
    }
  }

  static class NonHadoopConfiguration implements Configuration {
    private final Properties config;
    private final Properties tableProperties;

    NonHadoopConfiguration(Properties tableProperties,
                           Properties config) {
      this.config = config != null ? config : new Properties();
      this.tableProperties = tableProperties;
    }

    @Override
    public String get(String key) {
      String result = null;
      if (tableProperties != null) {
        result = tableProperties.getProperty(key);
      }
      if (result == null) {
        result = config.getProperty(key);
      }
      return result;
    }

    @Override
    public void set(String key, String value) {
      config.put(key, value);
    }
  }
}
