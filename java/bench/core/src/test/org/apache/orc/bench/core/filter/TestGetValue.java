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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class TestGetValue {
  private final VectorizedRowBatch b;
  private final long expSum;

  public TestGetValue() {
    b = FilterBenchUtil.createBatch(new Random(1024));
    LongColumnVector lv = (LongColumnVector) b.cols[0];
    long s = 0;
    for (int i = 0; i < b.size; i++) {
      s += lv.vector[i];
    }
    expSum = s;
    Assert.assertTrue(expSum != 0);
  }

  @Test
  public void testFunctionValue() {
    Assert.assertEquals(expSum, ComputeSum.sumWithFunction(b));
  }

  @Test
  public void testMethodValue() {
    Assert.assertEquals(expSum, ComputeSum.sumWithMethod(b));
  }

  @Test
  public void testDirect() {
    Assert.assertEquals(expSum, ComputeSum.sumDirect(b));
  }
}
