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
import org.apache.orc.impl.BitFieldWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;
import org.apache.orc.impl.RunLengthIntegerWriterV2;

import java.io.IOException;

public final class DoubleTreeWriterSplit extends TreeWriterBase {
  private final BitFieldWriter signWriter;
  private final RunLengthIntegerWriterV2 exponentStream;
  private final RunLengthIntegerWriterV2 mantissaStream;

  public DoubleTreeWriterSplit(int columnId,
                               TypeDescription schema,
                               WriterContext writer,
                               boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    signWriter = new BitFieldWriter(writer.createStream(columnId,
        OrcProto.Stream.Kind.SIGN), 1);
    exponentStream = new RunLengthIntegerWriterV2(writer.createStream(0,
        OrcProto.Stream.Kind.EXPONENT), true, true);
    mantissaStream = new RunLengthIntegerWriterV2(writer.createStream(0,
        OrcProto.Stream.Kind.DATA), false, true);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  private void writeNumber(double x) throws IOException {
    long xLong = Double.doubleToRawLongBits(x);
    signWriter.write(xLong >= 0 ? 0 : 1);
    exponentStream.write(((xLong >> 52) & 0x7ff) - 1023);
    mantissaStream.write(Long.reverse(xLong << 12));
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
        for(int i=0; i < length; ++i) {
          writeNumber(value);
        }
      }
    } else if (vec.noNulls) {
      for(int i=0; i < length; ++i) {
        double value = vec.vector[offset + i];
        writeNumber(value);
        indexStatistics.updateDouble(value);
        if (createBloomFilter) {
          bloomFilterUtf8.addDouble(value);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (!vec.isNull[i + offset]) {
          double value = vec.vector[i + offset];
          writeNumber(value);
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
    signWriter.flush();
    exponentStream.flush();
    mantissaStream.flush();
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    signWriter.getPosition(recorder);
    exponentStream.getPosition(recorder);
    mantissaStream.getPosition(recorder);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + signWriter.estimateMemory() +
        exponentStream.estimateMemory() + mantissaStream.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    long num = fileStatistics.getNumberOfValues();
    return num * JavaDataModel.get().primitive2();
  }
}
