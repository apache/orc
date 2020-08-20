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

package org.apache.orc.filter.impl;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.filter.OrcFilterContext;
import org.apache.orc.filter.VectorFilter;

public abstract class LeafFilter implements VectorFilter {
  private final String colName;

  protected LeafFilter(String colName) {
    this.colName = colName;
  }

  @Override
  public void filter(OrcFilterContext fc,
                     Selected bound,
                     Selected selIn,
                     Selected selOut) {
    ColumnVector v = fc.findColumnVector(colName);
    int inIdx = 0;
    int currSize = 0;
    int rowIdx;

    if (v.isRepeating) {
      if (allow(v, 0)) {
        // If the repeating value is allowed then allow the current selSize
        for (int i = 0; i < bound.selSize; i++) {
          rowIdx = bound.sel[i];
          if (inIdx < selIn.selSize && rowIdx == selIn.sel[inIdx]) {
            // Row already protected, no need to evaluate
            inIdx++;
            continue;
          }
          selOut.sel[currSize++] = rowIdx;
        }
      }
    } else if (v.noNulls) {
      for (int i = 0; i < bound.selSize; i++) {
        rowIdx = bound.sel[i];

        if (inIdx < selIn.selSize && rowIdx == selIn.sel[inIdx]) {
          // Row already protected, no need to evaluate
          inIdx++;
          continue;
        }

        // Check the value
        if (allow(v, rowIdx)) {
          selOut.sel[currSize++] = rowIdx;
        }
      }
    } else {
      for (int i = 0; i < bound.selSize; i++) {
        rowIdx = bound.sel[i];

        if (inIdx < selIn.selSize && rowIdx == selIn.sel[inIdx]) {
          // Row already protected, no need to evaluate
          inIdx++;
          continue;
        }

        // Check the value only if not null
        if (!v.isNull[rowIdx] && allow(v, rowIdx)) {
          selOut.sel[currSize++] = rowIdx;
        }
      }
    }

    selOut.selSize = currSize;
  }

  abstract protected boolean allow(ColumnVector v, int rowIdx);

}
