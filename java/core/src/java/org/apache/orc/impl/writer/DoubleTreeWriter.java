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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.DoubleWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;

import java.io.IOException;

public class DoubleTreeWriter extends TreeWriterBase {
  private final DoubleWriter writer;
  private final boolean isDoubleV2;

  public DoubleTreeWriter(int columnId,
                          TypeDescription schema,
                          WriterContext writer,
                          boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    OutStream out = writer.createStream(id, OrcProto.Stream.Kind.DATA);
    this.isDoubleV2 = writer.getVersion() == OrcFile.Version.UNSTABLE_PRE_2_0;
    this.writer = createDoubleWriter(out, isDoubleV2);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
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
        for (int i = 0; i < length; ++i) {
          writer.write(value);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          double value = vec.vector[i + offset];
          writer.write(value);
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
  public void writeStripe(OrcProto.StripeFooter.Builder builder,
                          OrcProto.StripeStatistics.Builder stats,
                          int requiredIndexEntries) throws IOException {
    super.writeStripe(builder, stats, requiredIndexEntries);
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
  public long estimateMemory() {
    return super.estimateMemory() + writer.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    long num = fileStatistics.getNumberOfValues();
    return num * JavaDataModel.get().primitive2();
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder builder = super.getEncoding();
    if (isDoubleV2) {
      builder.setKind(OrcProto.ColumnEncoding.Kind.DOUBLE_FPC);
    }
    return builder;
  }
}
