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

package org.apache.orc.impl;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.OrcProto;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.util.Map;

public final class DoubleTreeReaderV2 extends TreeReaderFactory.TreeReader {
  protected InStream stream;
  private final int BUFFER_SIZE = 1024;
  private final int DOUBLE_SIZE = Double.SIZE / 8;
  private final ByteBuffer bytes =
      ByteBuffer.allocate(BUFFER_SIZE * DOUBLE_SIZE)
          .order(ByteOrder.LITTLE_ENDIAN);
  private final DoubleBuffer doubles = bytes.asDoubleBuffer();

  public DoubleTreeReaderV2(int columnId, InStream isPresent,
                            TreeReaderFactory.Context context) throws IOException {
    super(columnId, isPresent, context);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams,
                          OrcProto.StripeFooter stripeFooter
                          ) throws IOException {
    super.startStripe(streams, stripeFooter);
    StreamName name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
    stream = streams.get(name);
  }

  @Override
  public void seek(PositionProvider[] index) throws IOException {
    seek(index[columnId]);
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    super.seek(index);
    stream.seek(index);
  }

  private int fillBuffer(int count) throws IOException {
    int len = Math.min(BUFFER_SIZE, count);
    SerializationUtils.readFully(stream, bytes.array(), bytes.arrayOffset(),
        len * DOUBLE_SIZE);
    doubles.position(0);
    doubles.limit(len);
    return len;
  }

  @Override
  public void nextVector(ColumnVector previousVector,
                         boolean[] isNull,
                         final int batchSize) throws IOException {
    final DoubleColumnVector result = (DoubleColumnVector) previousVector;

    // Read present/isNull stream
    super.nextVector(result, isNull, batchSize);

    final boolean hasNulls = !result.noNulls;
    if (hasNulls) {
      int nonNulls = 0;
      // conditions to ensure bounds checks skips
      for (int i = 0; i < batchSize && batchSize <= result.isNull.length; i++) {
        nonNulls += result.isNull[i] ? 0 : 1;
      }
      if (nonNulls == 0) {
        result.vector[0] = Double.NaN;
        result.isRepeating = true;
      } else {
        // some nulls
        result.isRepeating = false;
        nonNulls -= fillBuffer(nonNulls);
        // conditions to ensure bounds checks skips
        for (int i = 0; batchSize <= result.isNull.length
            && batchSize <= result.vector.length && i < batchSize; i++) {
          if (!result.isNull[i]) {
            if (!doubles.hasRemaining()) {
              nonNulls -= fillBuffer(nonNulls);
            }
            result.vector[i] = doubles.get();
          } else {
            // If the value is not present then set NaN
            result.vector[i] = Double.NaN;
          }
        }

        int offset = 0;
        while (offset < batchSize) {
          int len = fillBuffer(batchSize - offset);
          doubles.get(result.vector, offset, len);
          offset += len;
        }

      }
    } else {
      // no nulls
      int offset = 0;
      while (offset < batchSize) {
        int len = Math.min(BUFFER_SIZE, batchSize - offset);
        SerializationUtils.readFully(stream, bytes.array(), bytes.arrayOffset(),
            len * DOUBLE_SIZE);
        doubles.clear();
        doubles.get(result.vector, offset, len);
        offset += len;
      }
      if (batchSize > 1) {
        // conditions to ensure bounds checks skips
        boolean repeating = true;
        for (int i = 1; i < batchSize && batchSize <= result.vector.length; i++) {
          repeating = repeating && (result.vector[i-1] == result.vector[i]);
        }
        result.isRepeating = repeating;
      }
    }
  }

  @Override
  void skipRows(long items) throws IOException {
    items = countNonNulls(items);
    long len = items * 8;
    while (len > 0) {
      len -= stream.skip(len);
    }
  }
}
