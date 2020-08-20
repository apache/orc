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

package org.apache.orc.bench.core.filter;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.util.function.BiFunction;

public class ComputeSum {
  public static long sumWithFunction(VectorizedRowBatch b) {
    long sum = 0;
    BiFunction<ColumnVector, Integer, Long> gv = RowFilterFactory::getLong;
    ColumnVector v = b.cols[0];
    Object o;

    for (int i = 0; i < b.size; i++) {
      o = RowFilterFactory.getValue(v, i, gv);
      if (o != null) {
        sum += (long) o;
      }
    }
    return sum;
  }

  public static long sumDirect(VectorizedRowBatch b) {
    long sum = 0;
    LongColumnVector colVector = (LongColumnVector) b.cols[0];
    long[] vector = colVector.vector;
    if (colVector.isRepeating && (colVector.noNulls || !colVector.isNull[0])) {
      sum += vector[0] * b.getSelectedSize();
    } else if (colVector.noNulls) {
      for (int i = 0; i < b.size; i++) {
        sum += vector[i];
      }
    } else {
      for (int i = b.getSelectedSize() - 1; i > -1; --i) {
        if (!colVector.isNull[i]) {
          sum += vector[i];
        }
      }
    }
    return sum;
  }

  public static long sumWithMethod(VectorizedRowBatch b) {
    long sum = 0;
    LongColumnVector colVector = (LongColumnVector) b.cols[0];
    if (colVector.isRepeating && (colVector.noNulls || !colVector.isNull[0])) {
      sum += getLongValue(colVector, 0) * b.getSelectedSize();
    } else if (colVector.noNulls) {
      for (int i = 0; i < b.size; i++) {
        sum += getLongValue(colVector, i);
      }
    } else {
      for (int i = b.getSelectedSize() - 1; i > -1; --i) {
        if (!colVector.isNull[i]) {
          sum += getLongValue(colVector, i);
        }
      }
    }
    return sum;
  }

  private static long getLongValue(ColumnVector v, int idx) {
    LongColumnVector lv = (LongColumnVector) v;
    return lv.vector[idx];
  }
}
