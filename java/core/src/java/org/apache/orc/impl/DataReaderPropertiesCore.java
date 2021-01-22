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

import org.apache.orc.shims.FileIO;
import org.apache.orc.OrcConf;
import org.apache.orc.shims.SeekableInputStream;


public class DataReaderPropertiesCore {

  private final FileIO fileIO;
  private final String fileName;
  private final SeekableInputStream file;
  private final InStream.StreamOptions compression;
  private final boolean zeroCopy;
  private final int maxDiskRangeChunkLimit;

  protected DataReaderPropertiesCore(Builder builder) {
    this.fileIO = builder.fileIO;
    this.fileName = builder.fileName;
    this.file = builder.file;
    this.compression = builder.compression;
    this.zeroCopy = builder.zeroCopy;
    this.maxDiskRangeChunkLimit = builder.maxDiskRangeChunkLimit;
  }

  public FileIO getFileIO() {
    return fileIO;
  }

  public String getFileName() {
    return fileName;
  }

  public SeekableInputStream getFile() {
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

    private FileIO fileIO;
    private String fileName;
    private SeekableInputStream file;
    private InStream.StreamOptions compression;
    private boolean zeroCopy;
    private int maxDiskRangeChunkLimit = (int) OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getDefaultValue();;

    protected Builder() {

    }

    public Builder withFileIO(FileIO fileIO) {
      this.fileIO = fileIO;
      return this;
    }

    public Builder withFileName(String fileName) {
      this.fileName = fileName;
      return this;
    }

    public Builder withFile(SeekableInputStream file) {
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

    public DataReaderPropertiesCore build() {
      if (fileIO == null || fileName == null) {
        throw new NullPointerException("Filesystem = " + fileIO +
                                           ", fileName = " + fileName);
      }

      return new DataReaderPropertiesCore(this);
    }

  }
}
