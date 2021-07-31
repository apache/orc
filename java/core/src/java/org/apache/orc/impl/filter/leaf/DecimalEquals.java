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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.filter.LeafFilter;

// This is generated from decimal_cmp.txt
class DecimalEquals extends LeafFilter {
  private final HiveDecimalWritable aValue;

  DecimalEquals(String colName, Object aValue, boolean negated) {
    super(colName, negated);
    this.aValue = (HiveDecimalWritable) aValue;
  }

  @Override
  protected boolean allow(ColumnVector v, int rowIdx) {
    return ((DecimalColumnVector) v).vector[rowIdx].compareTo(aValue) == 0;
  }
}
