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
import java.util.Map;

public final class DoubleTreeReaderFpcV1 extends TreeReaderFactory.TreeReader {
  protected FpcV1.FpcExtractor stream;
  protected double[] data;
  protected int position;

  public DoubleTreeReaderFpcV1(int columnId, InStream isPresent,
                               TreeReaderFactory.Context context) throws IOException {
    super(columnId, isPresent, context);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams,
                          OrcProto.StripeFooter stripeFooter
                          ) throws IOException {
    super.startStripe(streams, stripeFooter);
    StreamName name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
    stream = new FpcV1.FpcExtractor(streams.get(name));
  }

  @Override
  public void seek(PositionProvider[] index) throws IOException {
    seek(index[columnId]);
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    super.seek(index);
    data = stream.extract();
    position = (int) index.getNext();
  }

  double read() throws IOException {
    if (data == null || position >= data.length) {
      data = stream.extract();
      position = 0;
    }
    return data[position++];
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
        // conditions to ensure bounds checks skips
        for (int i = 0; batchSize <= result.isNull.length
            && batchSize <= result.vector.length && i < batchSize; i++) {
          if (!result.isNull[i]) {
            result.vector[i] = read();
          } else {
            // If the value is not present then set NaN
            result.vector[i] = Double.NaN;
          }
        }
      }
    } else {
      // no nulls
      boolean repeating = true;
      for(int i= 0; i < batchSize && batchSize <= result.vector.length; ++i) {
        result.vector[i] = read();
        repeating = repeating && (i == 0 || result.vector[0] == result.vector[i]);
      }
      if (batchSize > 1) {
        result.isRepeating = repeating;
      }
    }
  }

  @Override
  void skipRows(long items) throws IOException {
    long nonNulls = countNonNulls(items);
    for(long i=0; i < nonNulls; ++i) {
      read();
    }
  }
}
