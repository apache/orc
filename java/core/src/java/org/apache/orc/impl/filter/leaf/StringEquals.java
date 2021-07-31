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

package org.apache.orc.impl.filter.leaf;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.orc.impl.filter.LeafFilter;

import java.nio.charset.StandardCharsets;

// This is generated from string_eq.txt
class StringEquals extends LeafFilter {

  private final byte[] aValue;

  StringEquals(String colName, Object aValue, boolean negated) {
    super(colName, negated);
    this.aValue = ((String) aValue).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  protected boolean allow(ColumnVector v, int rowIdx) {
    BytesColumnVector bv = (BytesColumnVector) v;
    return StringExpr.equal(aValue, 0, aValue.length,
                            bv.vector[rowIdx], bv.start[rowIdx], bv.length[rowIdx]);
  }
}
