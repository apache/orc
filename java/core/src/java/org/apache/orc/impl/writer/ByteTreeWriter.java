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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.RunLengthByteWriter;

import java.io.IOException;

public class ByteTreeWriter extends TreeWriterBase {
  private final RunLengthByteWriter writer;

  public ByteTreeWriter(int columnId,
                        TypeDescription schema,
                        WriterContext writer,
                        boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.writer = new RunLengthByteWriter(writer.createStream(id,
        OrcProto.Stream.Kind.DATA));
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    LongColumnVector vec = (LongColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        byte value = (byte) vec.vector[0];
        indexStatistics.updateInteger(value, length);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(value);
          }
          bloomFilterUtf8.addLong(value);
        }
        for (int i = 0; i < length; ++i) {
          writer.write(value);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          byte value = (byte) vec.vector[i + offset];
          writer.write(value);
          indexStatistics.updateInteger(value, 1);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(value);
            }
            bloomFilterUtf8.addLong(value);
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
    return num * JavaDataModel.get().primitive1();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    writer.flush();
  }
}
