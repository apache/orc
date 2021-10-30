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
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.CryptoUtils;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.InternalColumnVector;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.WholeColumnVector;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

public class MapTreeWriter extends TreeWriterBase {
  private final IntegerWriter lengths;
  private final boolean isDirectV2;
  private final TreeWriter keyWriter;
  private final TreeWriter valueWriter;

  MapTreeWriter(TypeDescription schema,
                WriterEncryptionVariant encryption,
                WriterContext context) throws IOException {
    super(schema, encryption, context);
    this.isDirectV2 = isNewWriteFormat(context);
    List<TypeDescription> children = schema.getChildren();
    keyWriter = Factory.create(children.get(0), encryption, context);
    valueWriter = Factory.create(children.get(1), encryption, context);
    lengths = createIntegerWriter(context.createStream(
        new StreamName(id, OrcProto.Stream.Kind.LENGTH, encryption)),
        false, isDirectV2, context);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    if (isDirectV2) {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
    } else {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    }
    return result;
  }

  @Override
  public void createRowIndexEntry() throws IOException {
    super.createRowIndexEntry();
    keyWriter.createRowIndexEntry();
    valueWriter.createRowIndexEntry();
  }

  @Override
  public void writeBatch(InternalColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    MapColumnVector vec = (MapColumnVector) vector.getColumnVector();
    InternalColumnVector internalKeys = new WholeColumnVector(vec.keys);
    InternalColumnVector internalValues = new WholeColumnVector(vec.values);

    /* update aggregate statistics */
    indexStatistics.updateCollectionLength(vec.lengths.length);

    if (vector.isRepeating()) {
      if (vector.notRepeatNull()) {
        int childOffset = (int) vec.offsets[0];
        int childLength = (int) vec.lengths[0];
        for (int i = 0; i < length; ++i) {
          lengths.write(childLength);
          keyWriter.writeBatch(internalKeys, childOffset, childLength);
          valueWriter.writeBatch(internalValues, childOffset, childLength);
        }
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(childLength);
          }
          bloomFilterUtf8.addLong(childLength);
        }
      }
    } else {
      // write the elements in runs
      int currentOffset = 0;
      int currentLength = 0;
      for (int i = 0; i < length; ++i) {
        if (!vector.isNull(i + offset)) {
          int valueOffset = vector.getValueOffset(i + offset);
          int nextLength = (int) vec.lengths[valueOffset];
          int nextOffset = (int) vec.offsets[valueOffset];
          lengths.write(nextLength);
          if (currentLength == 0) {
            currentOffset = nextOffset;
            currentLength = nextLength;
          } else if (currentOffset + currentLength != nextOffset) {
            keyWriter.writeBatch(internalKeys, currentOffset,
                currentLength);
            valueWriter.writeBatch(internalValues, currentOffset,
                currentLength);
            currentOffset = nextOffset;
            currentLength = nextLength;
          } else {
            currentLength += nextLength;
          }
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(nextLength);
            }
            bloomFilterUtf8.addLong(nextLength);
          }
        }
      }
      if (currentLength != 0) {
        keyWriter.writeBatch(internalKeys, currentOffset,
            currentLength);
        valueWriter.writeBatch(internalValues, currentOffset,
            currentLength);
      }
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    super.writeStripe(requiredIndexEntries);
    keyWriter.writeStripe(requiredIndexEntries);
    valueWriter.writeStripe(requiredIndexEntries);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    lengths.getPosition(recorder);
  }

  @Override
  public void addStripeStatistics(StripeStatistics[] stats
                                  ) throws IOException {
    super.addStripeStatistics(stats);
    keyWriter.addStripeStatistics(stats);
    valueWriter.addStripeStatistics(stats);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + lengths.estimateMemory() +
        keyWriter.estimateMemory() + valueWriter.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    return keyWriter.getRawDataSize() + valueWriter.getRawDataSize();
  }

  @Override
  public void writeFileStatistics() throws IOException {
    super.writeFileStatistics();
    keyWriter.writeFileStatistics();
    valueWriter.writeFileStatistics();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    lengths.flush();
    keyWriter.flushStreams();
    valueWriter.flushStreams();
  }

  @Override
  public void getCurrentStatistics(ColumnStatistics[] output) {
    super.getCurrentStatistics(output);
    keyWriter.getCurrentStatistics(output);
    valueWriter.getCurrentStatistics(output);
  }

  @Override
  public void prepareStripe(int stripeId) {
    super.prepareStripe(stripeId);
    lengths.changeIv(CryptoUtils.modifyIvForStripe(stripeId));
    keyWriter.prepareStripe(stripeId);
    valueWriter.prepareStripe(stripeId);
  }
}
