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

import org.apache.orc.filter.OrcFilterContext;
import org.apache.orc.filter.VectorFilter;

import java.util.function.Consumer;

public class BatchFilter implements Consumer<OrcFilterContext> {

  final VectorFilter filter;
  private final String[] colNames;
  private final Selected bound = new Selected();
  private final Selected selIn = new Selected();
  private final Selected selOut = new Selected();

  public BatchFilter(VectorFilter filter, String[] colNames) {
    this.filter = filter;
    this.colNames = colNames;
  }

  @Override
  public void accept(OrcFilterContext fc) {
    // Define the bound to be the batch size
    bound.initSelected(fc.getSelectedSize());
    // None of the selected values are protected
    selIn.selSize = 0;
    // selOut is set to the selectedVector
    selOut.sel = fc.getSelected();
    selOut.selSize = 0;
    filter.filter(fc, bound, selIn, selOut);

    if (selOut.selSize < fc.getSelectedSize()) {
      fc.setSelectedSize(selOut.selSize);
      fc.setSelectedInUse(true);
    } else if (selOut.selSize > fc.getSelectedSize()) {
      throw new RuntimeException(
        String.format("Unexpected state: Filtered size %s > input size %s",
                      selOut.selSize, fc.getSelectedSize()));
    }
  }

  public String[] getColNames() {
    return colNames;
  }

}
