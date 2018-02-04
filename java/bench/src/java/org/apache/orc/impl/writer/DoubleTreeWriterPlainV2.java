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
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.SerializationUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;

public final class DoubleTreeWriterPlainV2 extends TreeWriterBase {
  private static final int BUFFER_SIZE = 1024;
  private static final int DOUBLE_SIZE = Double.SIZE / 8;
  private final PositionedOutputStream stream;
  private final ByteBuffer bytes =
      ByteBuffer.allocate(BUFFER_SIZE * DOUBLE_SIZE)
          .order(ByteOrder.LITTLE_ENDIAN);
  private final DoubleBuffer doubles = bytes.asDoubleBuffer();

  public DoubleTreeWriterPlainV2(int columnId,
                                 TypeDescription schema,
                                 WriterContext writer,
                                 boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.stream = writer.createStream(id,
        OrcProto.Stream.Kind.DATA);
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
        final double value = vec.vector[0];
        indexStatistics.updateDouble(value);
        if (createBloomFilter) {
          bloomFilterUtf8.addDouble(value);
        }
        doubles.clear();
        int fill = Math.min(length, BUFFER_SIZE);
        for(int i=0; i < fill; ++i) {
          doubles.put(value);
        }
        while (length > fill) {
          stream.write(bytes.array(), bytes.arrayOffset(), fill * DOUBLE_SIZE);
          length -= fill;
        }
        if (length > 0) {
          stream.write(bytes.array(), bytes.arrayOffset(), length * DOUBLE_SIZE);
        }
      }
    } else if (vec.noNulls) {
      while (length > 0) {
        doubles.clear();
        int fill = Math.min(length, BUFFER_SIZE);
        doubles.put(vec.vector, offset, fill);
        stream.write(bytes.array(), bytes.arrayOffset(), fill * DOUBLE_SIZE);
        length -= fill;
        offset += fill;
      }
    } else {
      doubles.clear();
      for (int i = 0; i < length; ++i) {
        if (!vec.isNull[i + offset]) {
          if (!doubles.hasRemaining()) {
            stream.write(bytes.array(), bytes.arrayOffset(),
                doubles.position() * DOUBLE_SIZE);
            doubles.clear();
          }
          final double value = vec.vector[i + offset];
          doubles.put(value);
          indexStatistics.updateDouble(value);
          if (createBloomFilter) {
            bloomFilterUtf8.addDouble(value);
          }
        }
      }
      if (doubles.position() > 0) {
        stream.write(bytes.array(), bytes.arrayOffset(),
            doubles.position() * DOUBLE_SIZE);
      }
    }
  }

  @Override
  public void writeStripe(OrcProto.StripeFooter.Builder builder,
                          OrcProto.StripeStatistics.Builder stats,
                          int requiredIndexEntries) throws IOException {
    super.writeStripe(builder, stats, requiredIndexEntries);
    stream.flush();
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
