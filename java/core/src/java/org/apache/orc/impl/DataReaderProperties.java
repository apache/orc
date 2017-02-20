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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class DataReaderProperties {

  private final FileSystem fileSystem;
  private final Path path;
  private final FSDataInputStream file;
  private final InStream.StreamOptions compression;
  private final boolean zeroCopy;
  private final int maxDiskRangeChunkLimit;

  private DataReaderProperties(Builder builder) {
    this.fileSystem = builder.fileSystem;
    this.path = builder.path;
    this.file = builder.file;
    this.compression = builder.compression;
    this.zeroCopy = builder.zeroCopy;
    this.maxDiskRangeChunkLimit = builder.maxDiskRangeChunkLimit;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public Path getPath() {
    return path;
  }

  public FSDataInputStream getFile() {
    return file;
  }

  public InStream.StreamOptions getCompression() {
    return compression;
  }

  public boolean getZeroCopy() {
    return zeroCopy;
  }

  public int getMaxDiskRangeChunkLimit() {
    return maxDiskRangeChunkLimit;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private FileSystem fileSystem;
    private Path path;
    private FSDataInputStream file;
    private InStream.StreamOptions compression;
    private boolean zeroCopy;
    private int maxDiskRangeChunkLimit;

    private Builder() {

    }

    public Builder withFileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public Builder withPath(Path path) {
      this.path = path;
      return this;
    }

    public Builder withFile(FSDataInputStream file) {
      this.file = file;
      return this;
    }

    public Builder withCompression(InStream.StreamOptions value) {
      this.compression = value;
      return this;
    }

    public Builder withZeroCopy(boolean zeroCopy) {
      this.zeroCopy = zeroCopy;
      return this;
    }

    public Builder withMaxDiskRangeChunkLimit(int value) {
      maxDiskRangeChunkLimit = value;
      return this;
    }

    public DataReaderProperties build() {
      if (fileSystem == null || path == null) {
        throw new NullPointerException("Filesystem = " + fileSystem +
                                           ", path = " + path);
      }

      return new DataReaderProperties(this);
    }

  }
}
