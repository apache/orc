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
import org.apache.orc.filter.OrcFilterContext;

import java.util.Set;
import java.util.function.BiFunction;

public interface RowFilter {
  boolean accept(OrcFilterContext batch, int rowIdx);

  class INRowFilter<T> implements RowFilter {

    final String colName;
    public final Set<T> inValues;
    final BiFunction<ColumnVector, Integer, T> rowValue;

    public INRowFilter(String colName,
                       Set<T> inValues,
                       BiFunction<ColumnVector, Integer, T> rowValue) {
      this.colName = colName;
      this.inValues = inValues;
      this.rowValue = rowValue;
    }

    @Override
    public boolean accept(OrcFilterContext batch, int rowIdx) {
      return inValues.contains(getValue(batch.findColumnVector(colName), rowIdx, rowValue));
    }

    protected T getValue(ColumnVector v, int rowIdx, BiFunction<ColumnVector, Integer, T> f) {
      int idx = rowIdx;
      if (v.isRepeating) {
        idx = 0;
      }
      if (v.noNulls || !v.isNull[idx]) {
        return f.apply(v, idx);
      } else {
        return null;
      }
    }
  }


  class ORRowFilter implements RowFilter {

    public final RowFilter[] filters;

    public ORRowFilter(RowFilter[] filters) {
      this.filters = filters;
    }

    @Override
    public boolean accept(OrcFilterContext batch, int rowIdx) {
      boolean result = true;
      for (RowFilter filter : filters) {
        result = filter.accept(batch, rowIdx);
        if (result) {
          break;
        }
      }
      return result;
    }
  }

  class ANDRowFilter implements RowFilter {

    public final RowFilter[] filters;

    public ANDRowFilter(RowFilter[] filters) {
      this.filters = filters;
    }

    @Override
    public boolean accept(OrcFilterContext batch, int rowIdx) {
      boolean result = true;
      for (RowFilter filter : filters) {
        result = filter.accept(batch, rowIdx);
        if (!result) {
          break;
        }
      }
      return result;
    }
  }
}
