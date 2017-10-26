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

import java.io.IOException;

/**
 * This encoding takes a set of 8 doubles, and flips their bytes with the
 * goal of making longer stretches of similar bytes. So you'll end up with
 * 8 bytes of sign + exp-high, 8 bytes of exp-low + mantissa-high, etc.
 *
 * Currently this writer pads the stream out to a multiple of 64 bytes, which
 * is fine for benchmarking, but should be fixed before it is used in
 * production.
 */
public final class DoubleTreeWriterFlip extends TreeWriterBase {
  private final PositionedOutputStream stream;
  private final int BUFFER_SIZE = 8;
  private final int DOUBLE_SIZE = Double.SIZE / 8;
  private final byte[] bytes = new byte[BUFFER_SIZE * DOUBLE_SIZE];
  private int buffered = 0;
  private ChunkStartPosition position = new ChunkStartPosition();

  public DoubleTreeWriterFlip(int columnId,
                              TypeDescription schema,
                              WriterContext writer,
                              boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.stream = writer.createStream(id,
        OrcProto.Stream.Kind.DATA);
    this.stream.getPosition(position);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  private void writeNumber(double x) throws IOException {
    long xLong = Double.doubleToRawLongBits(x);
    bytes[buffered] = (byte) (xLong >> 56);
    bytes[8 + buffered] = (byte) (xLong >> 48);
    bytes[16 + buffered] = (byte) (xLong >> 40);
    bytes[24 + buffered] = (byte) (xLong >> 32);
    bytes[32 + buffered] = (byte) (xLong >> 24);
    bytes[40 + buffered] = (byte) (xLong >> 16);
    bytes[48 + buffered] = (byte) (xLong >> 8);
    bytes[56 + buffered] = (byte) (xLong);
    if (++buffered == BUFFER_SIZE) {
      stream.write(bytes, 0, bytes.length);
      buffered = 0;
      position.reset();
      stream.getPosition(position);
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
    if (buffered != 0) {
      stream.write(bytes, 0, bytes.length);
    }
    stream.flush();
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    position.playbackPosition(recorder);
    recorder.addPosition(buffered);
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
