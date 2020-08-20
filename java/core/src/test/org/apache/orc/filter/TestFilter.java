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

package org.apache.orc.filter;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.filter.impl.ATestFilter;
import org.apache.orc.filter.impl.AndFilter;
import org.apache.orc.filter.impl.BatchFilter;
import org.apache.orc.filter.impl.LongIn;
import org.apache.orc.filter.impl.OrFilter;
import org.apache.orc.filter.impl.StringIn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

public class TestFilter extends ATestFilter {

  @Test
  public void testAndOfOr() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .startOr()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .end()
      .startOr()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});

    BatchFilter filter = (BatchFilter) FilterFactory.createVectorFilter(sArg, schema);
    filter.accept(fc);
    assertArrayEquals(new String[]{"f1", "f2"}, filter.getColNames());
    validateSelected(0, 2, 5);
  }

  @Test
  public void testOrOfAnd() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});

    FilterFactory.createVectorFilter(sArg, schema).accept(fc.setBatch(batch));
    validateSelected(0, 2);
  }

  @Test
  public void testOrOfAndNative() {
    VectorFilter f = new OrFilter(
      new VectorFilter[] {
        new AndFilter(new VectorFilter[] {
          new LongIn("f1",
                     Arrays.asList(1L, 6L)),
          new StringIn("f2",
                       Arrays.asList("a", "c"))
        }),
        new AndFilter(new VectorFilter[] {
          new LongIn("f1",
                     Arrays.asList(3L, 4L)),
          new StringIn("f2",
                       Arrays.asList("c", "e"))
        })
      }
    );

    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});

    filter(f);
    Assert.assertEquals(2, fc.getSelectedSize());
    assertArrayEquals(new int[] {0, 2},
                             Arrays.copyOf(fc.getSelected(), fc.getSelectedSize()));
  }

  @Test
  public void testAndNotNot() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .startNot()
      .in("f1", PredicateLeaf.Type.LONG, 7L)
      .end()
      .startNot()
      .isNull("f2", PredicateLeaf.Type.STRING)
      .end()
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});

    Consumer<OrcFilterContext> filter = FilterFactory.createVectorFilter(sArg, schema);
    filter.accept(fc.setBatch(batch));
    Assert.assertEquals(6, fc.getSelectedSize());
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5},
                             Arrays.copyOf(fc.getSelected(), fc.getSelectedSize()));
  }

  @Test
  public void testUnSupportedSArg() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .nullSafeEquals("f1", PredicateLeaf.Type.LONG, 0L)
      .build();

    Assert.assertNull(FilterFactory.createVectorFilter(sarg, schema));
  }

  @Test
  public void testRepeatedProtected() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .in("f2", PredicateLeaf.Type.STRING, "a", "d")
      .lessThan("f1", PredicateLeaf.Type.LONG, 6L)
      .end()
      .build();

    setBatch(new Long[] {1L, 1L, 1L, 1L, 1L, 1L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    batch.cols[0].isRepeating = true;
    FilterFactory.createVectorFilter(sArg, schema).accept(fc.setBatch(batch));
    validateAllSelected(6);
  }

  @Test
  public void testNullProtected() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .in("f2", PredicateLeaf.Type.STRING, "a", "d")
      .lessThan("f1", PredicateLeaf.Type.LONG, 4L)
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    FilterFactory.createVectorFilter(sArg, schema).accept(fc.setBatch(batch));
    validateSelected(0, 1, 3);
  }

  @Test
  public void testUnsupportedNotLeaf() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .nullSafeEquals("f1", PredicateLeaf.Type.LONG, 2L)
      .end()
      .build();

    assertNull(FilterFactory.createVectorFilter(sArg, schema));
  }

  @Test
  public void testAndOrAnd() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .startOr()
      .lessThan("f1", PredicateLeaf.Type.LONG, 3L)
      .startAnd()
      .equals("f2", PredicateLeaf.Type.STRING, "a")
      .equals("f1", PredicateLeaf.Type.LONG, 5L)
      .end()
      .end()
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    FilterFactory.createVectorFilter(sArg, schema).accept(fc.setBatch(batch));
    validateSelected(0);
  }
}
