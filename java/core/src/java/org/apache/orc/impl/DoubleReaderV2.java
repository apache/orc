/**
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

import java.io.IOException;

public class DoubleReaderV2 implements DoubleReader {
  private final InStream input;
  private final Fpc.FpcExtractor fpcExtractor;
  private int numLiterals;
  private double[] literals;
  private int used = 0;

  public DoubleReaderV2(InStream input) throws IOException {
    this.input = input;
    this.fpcExtractor = new Fpc.FpcExtractor(input);
    this.numLiterals = 0;
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed != 0) {
      // a loop is required for cases where we break the run into two parts
      while (consumed > 0) {
        readValues();
        used = consumed;
        consumed -= numLiterals;
      }
    } else {
      used = 0;
      numLiterals = 0;
    }
  }

  @Override
  public void skip(long numValues) throws IOException {
    while (numValues > 0) {
      if (used == numLiterals) {
        numLiterals = 0;
        used = 0;
        readValues();
      }
      long consume = Math.min(numValues, numLiterals - used);
      used += consume;
      numValues -= consume;
    }
  }

  private void readValues() throws IOException {
    literals = fpcExtractor.extract();
    numLiterals = fpcExtractor.numLiterals;
    used = 0;
  }

  @Override
  public double next() throws IOException {
    double result;
    if (used == numLiterals) {
      numLiterals = 0;
      used = 0;
      readValues();
    }
    result = literals[used++];
    return result;
  }

  @Override
  public void nextVector(ColumnVector column, double[] data, int batchSize)
      throws IOException {
    final DoubleColumnVector result = (DoubleColumnVector) column;

    final boolean hasNulls = !result.noNulls;
    boolean allNulls = hasNulls;

    if (hasNulls) {
      // conditions to ensure bounds checks skips
      for (int i = 0; i < batchSize && batchSize <= result.isNull.length;
           i++) {
        allNulls = allNulls & result.isNull[i];
      }
      if (allNulls) {
        result.vector[0] = Double.NaN;
        result.isRepeating = true;
      } else {
        // some nulls
        result.isRepeating = false;
        // conditions to ensure bounds checks skips
        for (int i = 0; batchSize <= result.isNull.length
            && batchSize <= result.vector.length && i < batchSize; i++) {
          if (!result.isNull[i]) {
            result.vector[i] = next();
          } else {
            // If the value is not present then set NaN
            result.vector[i] = Double.NaN;
          }
        }
      }
    } else {
      // no nulls
      boolean repeating = (batchSize > 1);
      final double d1 = next();
      result.vector[0] = d1;
      // conditions to ensure bounds checks skips
      for (int i = 1; i < batchSize && batchSize <= result.vector.length;
           i++) {
        final double d2 = next();
        repeating = repeating && (d1 == d2);
        result.vector[i] = d2;
      }
      result.isRepeating = repeating;
    }
  }
}
