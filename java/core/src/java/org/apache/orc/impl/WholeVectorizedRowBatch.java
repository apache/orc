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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class WholeVectorizedRowBatch extends InternalVectorizedRowBatch {

  private boolean preserved;

  public WholeVectorizedRowBatch(VectorizedRowBatch vectorizedRowBatch) {
    this(vectorizedRowBatch, true);
  }

  public WholeVectorizedRowBatch(VectorizedRowBatch vectorizedRowBatch, boolean preserved) {
    super(vectorizedRowBatch);
    this.preserved = preserved;
  }

  @Override
  public void ensureSize(int rows) {
    if (!preserved) {
      vectorizedRowBatch.ensureSize(rows);
    } else {
      throw new UnsupportedOperationException(
          "The ensureSize operation is not supported after the data is preserved");
    }
  }

  public void setPreserved(boolean preserved) {
    this.preserved = preserved;
  }

  @Override
  public InternalColumnVector cols(int col) {
    return new WholeColumnVector(vectorizedRowBatch.cols[col], preserved);
  }

  @Override
  public int size() {
    return vectorizedRowBatch.size;
  }

}
