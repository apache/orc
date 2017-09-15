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

import org.apache.hadoop.fs.Path;
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
import java.util.List;

public class AcidReaderImpl implements Reader {
  public AcidReaderImpl(Path dir, OrcFile.ReaderOptions options) {
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
    return null;
  }

  @Override
  public ByteBuffer getMetadataValue(String key) {
    return null;
  }

  @Override
  public boolean hasMetadataValue(String key) {
    return false;
  }

  @Override
  public CompressionKind getCompressionKind() {
    return null;
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
    return null;
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
    return null;
  }

  @Override
  public List<OrcProto.Type> getTypes() {
    return null;
  }

  @Override
  public OrcFile.Version getFileVersion() {
    return null;
  }

  @Override
  public OrcFile.WriterVersion getWriterVersion() {
    return null;
  }

  @Override
  public OrcProto.FileTail getFileTail() {
    return null;
  }

  @Override
  public Options options() {
    return null;
  }

  @Override
  public RecordReader rows() throws IOException {
    return null;
  }

  @Override
  public RecordReader rows(Options options) throws IOException {
    return null;
  }

  @Override
  public List<Integer> getVersionList() {
    return null;
  }

  @Override
  public int getMetadataSize() {
    return 0;
  }

  @Override
  public List<OrcProto.StripeStatistics> getOrcProtoStripeStatistics() {
    return null;
  }

  @Override
  public List<StripeStatistics> getStripeStatistics() throws IOException {
    return null;
  }

  @Override
  public List<OrcProto.ColumnStatistics> getOrcProtoFileStatistics() {
    return null;
  }

  @Override
  public ByteBuffer getSerializedFileFooter() {
    return null;
  }
}
