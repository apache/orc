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

/*
 * In decimal format V2, we write decimals whose precision is <= 18 as decimal64.  A decimal64 is a
 * signed decimal long padded with trailing zeroes so all the decimals have the same scale.  This
 * allows better RLE compression.
 *
 * NOTE: Decimal with precision > 18 will be written with the original writer DecimalTreeWriterV1.
 */
public class DecimalTreeWriterV2 extends TreeWriterBase {
  private final IntegerWriter writer;

  private final int precision;
  private final int scale;

  // These scratch buffer allows us to decimals to string much faster.
  private final byte[] scratchBuffer;

  public DecimalTreeWriterV2(int columnId,
                           TypeDescription fileSchema,
                           WriterContext writerContext,
                           boolean nullable) throws IOException {
    super(columnId, fileSchema, writerContext, nullable);
    OutStream out = writerContext.createStream(id,
        OrcProto.Stream.Kind.DATA);
    writer = createIntegerWriter(out, true, true, writerContext);

    precision = fileSchema.getPrecision();
    scale = fileSchema.getScale();

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

  /*
   * NOTE: We assert the precision and scale of the DecimalColumnVector is the same as the file
   * schema's decimal type.  And, we *assume* that all decimals in the DecimalColumnVector are
   * within the precision and scale (i.e.enforcePrecisionScale has been called already).  So we
   * do not check each decimal to get better performance.
   */
  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    DecimalColumnVector decimalColVector = (DecimalColumnVector) vector;
    assert precision == decimalColVector.precision;
    assert scale == decimalColVector.scale;
    if (decimalColVector.isRepeating) {
      if (decimalColVector.noNulls || !decimalColVector.isNull[0]) {
        final HiveDecimalWritable decValue = decimalColVector.vector[0];
        final long decimal64Long = decValue.serialize64(scale);
        indexStatistics.updateDecimal64(decValue, decimal64Long);
        if (createBloomFilter) {
          String str = decValue.toString(scratchBuffer);
          if (bloomFilter != null) {
            bloomFilter.addString(str);
          }
          bloomFilterUtf8.addString(str);
        }
        for (int i = 0; i < length; ++i) {
          writer.write(decimal64Long);
        }
      }
    } else {
      HiveDecimalWritable[] decimalVector = decimalColVector.vector;
      for (int i = 0; i < length; ++i) {
        if (decimalColVector.noNulls || !decimalColVector.isNull[i + offset]) {
          final HiveDecimalWritable decValue = decimalColVector.vector[i + offset];
          long decimal64Long = decValue.serialize64(scale);
          indexStatistics.updateDecimal64(decValue, decimal64Long);
          if (createBloomFilter) {
            String str = decValue.toString(scratchBuffer);
            if (bloomFilter != null) {
              bloomFilter.addString(str);
            }
            bloomFilterUtf8.addString(str);
          }
          writer.write(decimal64Long);
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
