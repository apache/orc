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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BitFieldWriter;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;

import java.io.IOException;

public class DecimalTreeWriter64 extends TreeWriterBase {
  private final IntegerWriter writer;

  private final int scale;

  // These scratch buffers allow us to serialize decimals much faster.
  private final long[] scratchLongs;
  private final byte[] scratchBuffer;

  private final boolean isDirectV2;

  public DecimalTreeWriter64(int columnId,
                           TypeDescription schema,
                           WriterContext writerContext,
                           boolean nullable) throws IOException {
    super(columnId, schema, writerContext, nullable);
    OutStream out = writerContext.createStream(id,
        OrcProto.Stream.Kind.DATA);
    this.isDirectV2 = true;
    writer = createIntegerWriter(out, true, true, writerContext);

    scale = schema.getScale();

    scratchLongs = new long[HiveDecimal.SCRATCH_LONGS_LEN];
    scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];

    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    result.setKind(OrcProto.ColumnEncoding.Kind.DECIMAL_64);
    return result;
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    DecimalColumnVector decimalColVector = (DecimalColumnVector) vector;

    if (decimalColVector.isRepeating) {
      if (decimalColVector.noNulls || !decimalColVector.isNull[0]) {
        HiveDecimalWritable value = decimalColVector.vector[0];
        indexStatistics.updateDecimal(value);
        if (createBloomFilter) {
          String str = value.toString(scratchBuffer);
          if (bloomFilter != null) {
            bloomFilter.addString(str);
          }
          bloomFilterUtf8.addString(str);
        }
        for (int i = 0; i < length; ++i) {
          writer.write(value.serialize64(scale));
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (decimalColVector.noNulls || !decimalColVector.isNull[i + offset]) {
          HiveDecimalWritable value = decimalColVector.vector[i + offset];
          writer.write(value.serialize64(scale));
          indexStatistics.updateDecimal(value);
          if (createBloomFilter) {
            String str = value.toString(scratchBuffer);
            if (bloomFilter != null) {
              bloomFilter.addString(str);
            }
            bloomFilterUtf8.addString(str);
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
    return super.estimateMemory() + writer.estimateMemory() ;
  }

  @Override
  public long getRawDataSize() {
    return fileStatistics.getNumberOfValues() *
        JavaDataModel.get().lengthOfDecimal();
  }
}
