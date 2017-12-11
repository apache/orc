/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl.acid;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

class NullReader implements Reader {

  private static NullReader self = null;

  static NullReader getNullReader() {
    if (self == null) self = new NullReader();
    return self;
  }

  private NullReader() {

  }

  @Override
  public long getNumberOfRows() {
    return 0;
  }

  @Override
  public long getRawDataSize() {
    return 0;
  }

  @Override
  public long getRawDataSizeOfColumns(List<String> colNames) {
    return 0;
  }

  @Override
  public long getRawDataSizeFromColIndices(List<Integer> colIds) {
    return 0;
  }

  @Override
  public List<String> getMetadataKeys() {
    return Collections.emptyList();
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    return ByteBuffer.allocate(0);
  }

  @Override
  public boolean hasMetadataValue(String key) {
    return false;
  }

  @Override
  public CompressionKind getCompressionKind() {
    return CompressionKind.NONE;
  }

  @Override
  public int getCompressionSize() {
    return 0;
  }

  @Override
  public int getRowIndexStride() {
    return 0;
  }

  @Override
  public List<StripeInformation> getStripes() {
    return Collections.emptyList();
  }

  @Override
  public long getContentLength() {
    return 0;
  }

  @Override
  public ColumnStatistics[] getStatistics() {
    return new ColumnStatistics[0];
  }

  @Override
  public TypeDescription getSchema() {
    // TODO not sure if this is ok or not
    return null;
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return Collections.emptyList();
  }

  @Override
  public OrcFile.Version getFileVersion() {
    return OrcFile.Version.CURRENT;
  }

  @Override
  public OrcFile.WriterVersion getWriterVersion() {
    return OrcFile.CURRENT_WRITER;
  }

  @Override
  public OrcProto.FileTail getFileTail() {
    // TODO not sure if this is ok or not
    return null;
  }

  @Override
  public Options options() {
    return new Options();
  }

  @Override
  public RecordReader rows() throws IOException {
    return nullRecordReader;
  }

  @Override
  public RecordReader rows(Options options) throws IOException {
    return nullRecordReader;
  }

  @Override
  public List<Integer> getVersionList() {
    return Collections.emptyList();
  }

  @Override
  public int getMetadataSize() {
    return 0;
  }

  @Override
  public List<OrcProto.StripeStatistics> getOrcProtoStripeStatistics() {
    return Collections.emptyList();
  }

  @Override
  public List<StripeStatistics> getStripeStatistics() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public List<OrcProto.ColumnStatistics> getOrcProtoFileStatistics() {
    return Collections.emptyList();
  }

  @Override
  public ByteBuffer getSerializedFileFooter() {
    return ByteBuffer.allocate(0);
  }

  static RecordReader nullRecordReader = new RecordReader() {
    @Override
    public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
      return false;
    }

    @Override
    public long getRowNumber() throws IOException {
      return 0;
    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void seekToRow(long rowCount) throws IOException {

    }
  };
}
