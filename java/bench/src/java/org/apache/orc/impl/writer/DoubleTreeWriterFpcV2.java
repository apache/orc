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
import org.apache.orc.impl.FpcV2;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;

import java.io.IOException;

public final class DoubleTreeWriterFpcV2 extends TreeWriterBase {
  private final PositionedOutputStream stream;
  private final FpcV2.FpcCompressor compressor;

  public DoubleTreeWriterFpcV2(int columnId,
                               TypeDescription schema,
                               WriterContext writer,
                               boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.stream = writer.createStream(id,
        OrcProto.Stream.Kind.DATA);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
    compressor = new FpcV2.FpcCompressor(stream);
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
          compressor.compress(vec.vector, 0, 1);
          length -= 1;
        }
      }
    } else if (vec.noNulls) {
      compressor.compress(vec.vector, offset, length);
    } else {
      int start = offset;
      final int stop = offset + length;
      while (start < stop) {
        // skip through any nulls at the start
        while (start < stop && vec.isNull[start]) {
          start += 1;
        }
        // now find the end of the range
        int end = start;
        while (end < stop && !vec.isNull[end]) {
          end += 1;
        }
        if (start != end) {
          compressor.compress(vec.vector, start, end - start);
        }
        start = end;
      }
    }
  }

  @Override
  public void writeStripe(OrcProto.StripeFooter.Builder builder,
                          OrcProto.StripeStatistics.Builder stats,
                          int requiredIndexEntries) throws IOException {
    super.writeStripe(builder, stats, requiredIndexEntries);
    compressor.flush();
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    stream.getPosition(recorder);
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
