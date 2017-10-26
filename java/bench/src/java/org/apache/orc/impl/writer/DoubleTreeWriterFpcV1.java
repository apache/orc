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
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.FpcV1;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;

import java.io.IOException;

public final class DoubleTreeWriterFpcV1 extends TreeWriterBase {
  private final PositionedOutputStream stream;
  private final FpcV1.FpcCompressor compressor;
  private final int BUFFER_SIZE = 32 * 1024;
  private final double[] values = new double[BUFFER_SIZE];
  int storedValues = 0;

  public DoubleTreeWriterFpcV1(int columnId,
                               TypeDescription schema,
                               WriterContext writer,
                               boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.stream = writer.createStream(id,
        OrcProto.Stream.Kind.DATA);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
    compressor = new FpcV1.FpcCompressor(stream);
  }

  private void compress() throws IOException {
    compressor.compress(values, storedValues);
    storedValues = 0;
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    DoubleColumnVector vec = (DoubleColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        final double value = vec.vector[0];
        indexStatistics.updateDouble(value);
        if (createBloomFilter) {
          bloomFilterUtf8.addDouble(value);
        }
        while (length > 0) {
          int fill = Math.min(values.length - storedValues, length);
          for(int i=0; i < fill; ++i) {
            values[storedValues++] = value;
          }
          length -= fill;
          if (values.length == storedValues) {
            compress();
          }
        }
      }
    } else if (vec.noNulls) {
      while (length > 0) {
        int fill = Math.min(values.length - storedValues, length);
        System.arraycopy(vec.vector, offset, values, storedValues, fill);
        storedValues += fill;
        offset += fill;
        length -= fill;
        if (values.length == storedValues) {
          compress();
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (!vec.isNull[i + offset]) {
          if (values.length == storedValues) {
            compress();
          }
          final double value = vec.vector[i + offset];
          values[storedValues++] = value;
          indexStatistics.updateDouble(value);
          if (createBloomFilter) {
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
    if (storedValues > 0) {
      compress();
    }
    stream.flush();
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    stream.getPosition(recorder);
    recorder.addPosition(storedValues);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + stream.getBufferSize();
  }

  @Override
  public long getRawDataSize() {
    long num = fileStatistics.getNumberOfValues();
    return num * JavaDataModel.get().primitive2();
  }
}
