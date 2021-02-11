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

public class Selected {

  static final Selected EMPTY_SELECTED = new Selected();

  int[] sel;
  int selSize;

  Selected(int[] sel) {
    this.sel = sel;
    this.selSize = 0;
  }

  Selected() {
    this(new int[1024]);
  }

  void initSelected(int batchSize) {
    ensureSize(batchSize);
    selSize = batchSize;
    for (int i = 0; i < selSize; i++) {
      sel[i] = i;
    }
  }

  void ensureSize(int size) {
    if (size > sel.length) {
      sel = new int[size];
      selSize = 0;
    }
  }

  void set(Selected inBound) {
    ensureSize(inBound.selSize);
    System.arraycopy(inBound.sel, 0, sel, 0, inBound.selSize);
    selSize = inBound.selSize;
  }
}
