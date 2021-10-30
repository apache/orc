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

public abstract class InternalColumnVector {

  protected final ColumnVector columnVector;

  public InternalColumnVector(ColumnVector columnVector) {
    this.columnVector = columnVector;
  }

  public ColumnVector getColumnVector() {
    return columnVector;
  }

  public boolean isRepeating() {
    return columnVector.isRepeating;
  }

  public boolean notRepeatNull() {
    return columnVector.noNulls || !columnVector.isNull[0];
  }

  public boolean noNulls() {
    return columnVector.noNulls;
  }

  public abstract void ensureSize(int size, boolean preserveData);

  public abstract boolean isNull(int offset);

  public abstract int getValueOffset(int offset);

  public abstract Function<ColumnVector, InternalColumnVector> encapsulationFunction();

}
