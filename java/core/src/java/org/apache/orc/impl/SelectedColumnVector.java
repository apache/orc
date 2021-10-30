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

import java.util.function.Function;

public class SelectedColumnVector extends InternalColumnVector {

  private final int[] selected;

  public SelectedColumnVector(ColumnVector columnVector, int[] selected) {
    super(columnVector);
    this.selected = selected;
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    throw new UnsupportedOperationException(
        "The ensureSize operation is not supported for SelectedColumnVector");
  }

  @Override
  public boolean isNull(int offset) {
    return columnVector.isNull[getValueOffset(offset)];
  }

  @Override
  public int getValueOffset(int offset) {
    return selected[offset];
  }

  @Override
  public Function<ColumnVector, InternalColumnVector> encapsulationFunction() {
    return vector -> new SelectedColumnVector(vector, selected);
  }

}
