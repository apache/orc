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

import org.apache.orc.OrcFilterContext;
import org.apache.orc.filter.VectorFilter;

public class OrFilter implements VectorFilter {

  public final VectorFilter[] filters;
  private final Selected orOut = new Selected();

  public OrFilter(VectorFilter[] filters) {
    this.filters = filters;
  }

  public static void merge(Selected src, Selected tgt) {
    int writeIdx = src.selSize + tgt.selSize - 1;
    int srcIdx = src.selSize - 1;
    int tgtIdx = tgt.selSize - 1;

    while (tgtIdx >= 0 || srcIdx >= 0) {
      if (srcIdx < 0 || (tgtIdx >= 0 && src.sel[srcIdx] < tgt.sel[tgtIdx])) {
        // src is exhausted or tgt is larger
        tgt.sel[writeIdx--] = tgt.sel[tgtIdx--];
      } else {
        tgt.sel[writeIdx--] = src.sel[srcIdx--];
      }
    }
    tgt.selSize += src.selSize;
  }

  @Override
  public void filter(OrcFilterContext fc,
                     Selected bound,
                     Selected selIn,
                     Selected selOut) {
    orOut.ensureSize(bound.selSize);
    // In case of an OR filter, the current selections are always protected and not evaluated again.
    selOut.set(selIn);
    for (VectorFilter f : filters) {
      // In case of OR since we have to append to existing output, pass the out as empty
      orOut.selSize = 0;
      f.filter(fc, bound, selOut, orOut);
      // During an OR operation the size cannot decrease, merge the current selections into selOut
      merge(orOut, selOut);
    }
  }
}
