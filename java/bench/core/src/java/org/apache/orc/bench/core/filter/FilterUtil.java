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
import org.apache.orc.filter.OrcFilterContext;
import org.apache.orc.filter.VectorFilter;
import org.apache.orc.filter.impl.AndFilter;
import org.apache.orc.filter.impl.LongIn;
import org.apache.orc.filter.impl.OrFilter;
import org.apache.orc.filter.impl.BatchFilter;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

public class FilterUtil {
  public static Map.Entry<Consumer<OrcFilterContext>, int[]> createComplexFilter(Random rnd,
                                                                              VectorizedRowBatch b,
                                                                              int inSize,
                                                                              int orSize) {
    LongColumnVector f1Vector = (LongColumnVector) b.cols[0];
    LongColumnVector f2Vector = (LongColumnVector) b.cols[1];

    Object[] f1Values = new Object[inSize];
    Object[] f2Values = new Object[inSize];
    Set<Integer> sel = new HashSet<>();
    VectorFilter[] orFilters = new VectorFilter[orSize + 1];
    orFilters[0] = new LongIn("f2",
                              Arrays.asList(f2Vector.vector[0],
                                            f2Vector.vector[1]));
    sel.add(0);
    sel.add(1);
    int selIdx;

    for (int i = 0; i < orSize; i++) {
      for (int j = 0; j < inSize; j++) {
        selIdx = rnd.nextInt(b.getMaxSize());
        f1Values[j] = f1Vector.vector[selIdx];
        f2Values[j] = f2Vector.vector[selIdx];
        sel.add(selIdx);
      }
      orFilters[i + 1] = new AndFilter(new VectorFilter[] {
        new LongIn("f1",
                   Arrays.asList(f1Values)),
        new LongIn("f2",
                   Arrays.asList(f2Values))
      });
    }
    BatchFilter root = new BatchFilter(
      new OrFilter(orFilters),
      new String[] {"f1", "f2"}
    );

    int[] s = sel.stream()
      .mapToInt(Integer::intValue)
      .toArray();
    Arrays.sort(s);
    return new AbstractMap.SimpleImmutableEntry<>(root, s);
  }
}
