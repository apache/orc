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

import java.util.function.Supplier;

/**
 * A DataReaderProperties that includes the old Hadoop-based parameters.
 */
public final class DataReaderProperties extends DataReaderPropertiesCore {

  private final Supplier<FileSystem> fileSystemSupplier;
  private final Path path;

  private DataReaderProperties(Builder builder) {
    super(builder);
    this.fileSystemSupplier = builder.fileSystemSupplier;
    this.path = builder.path;
  }

  public Supplier<FileSystem> getFileSystemSupplier() {
    return fileSystemSupplier;
  }

  public Path getPath() {
    return path;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends DataReaderPropertiesCore.Builder {

    private Supplier<FileSystem> fileSystemSupplier;
    private Path path;

    private Builder() {

    }

    public Builder withFileSystemSupplier(Supplier<FileSystem> supplier) {
      this.fileSystemSupplier = supplier;
      super.withFileIO(HadoopShimsFactory.get().createFileIO((Supplier) supplier));
      return this;
    }

    public Builder withFileSystem(FileSystem filesystem) {
      withFileSystemSupplier(() -> filesystem);
      return this;
    }

    public Builder withPath(Path path) {
      this.path = path;
      withFileName(path.toString());
      return this;
    }

    public Builder withFile(FSDataInputStream file) {
      super.withFile(new HadoopShimsPre2_3.HadoopSeekableInputStream(file));
      return this;
    }

    public Builder withCompression(InStream.StreamOptions value) {
      super.withCompression(value);
      return this;
    }

    public Builder withZeroCopy(boolean zeroCopy) {
      super.withZeroCopy(zeroCopy);
      return this;
    }

    public Builder withMaxDiskRangeChunkLimit(int value) {
      super.withMaxDiskRangeChunkLimit(value);
      return this;
    }

    public DataReaderProperties build() {
      super.build();
      return new DataReaderProperties(this);
    }

  }
}
