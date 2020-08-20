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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.TypeDescription;
import org.apache.orc.filter.OrcFilterContext;
import org.apache.orc.filter.FilterFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestRowFilter extends ATestFilter {

  private final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createString());
  final OrcFilterContext fc = new OrcFilterContext(schema);

  private final VectorizedRowBatch batch = schema.createRowBatch();

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testINLongConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
      .build();

    Set<String> colIds = new HashSet<>();
    RowFilter filter = RowFilterFactory.createRowFilter(sarg.getExpression(),
                                                        colIds,
                                                        sarg.getLeaves());
    Assert.assertNotNull(filter);
    Assert.assertTrue(filter instanceof RowFilter.INRowFilter);
    Assert.assertEquals(new HashSet<>(Arrays.asList(1L, 2L, 3L)),
                        ((RowFilter.INRowFilter<?>) filter).inValues);
    Assert.assertEquals(1, colIds.size());
    Assert.assertTrue(colIds.contains("f1"));

    setBatch(new Long[] {1L, 0L, 2L, 4L, 3L},
             new String[] {});
    fc.setBatch(batch);

    for (int i = 0; i < batch.size; i++) {
      if (i % 2 == 0) {
        Assert.assertTrue(filter.accept(fc, i));
      } else {
        Assert.assertFalse(filter.accept(fc, i));
      }
    }
  }

  @Test
  public void testINStringConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("f2", PredicateLeaf.Type.STRING, "a", "b")
      .build();

    Set<String> colIds = new HashSet<>();
    RowFilter filter = RowFilterFactory.createRowFilter(sarg.getExpression(),
                                                        colIds,
                                                        sarg.getLeaves());
    Assert.assertNotNull(filter);
    Assert.assertTrue(filter instanceof RowFilter.INRowFilter);
    Assert.assertEquals(new HashSet<>(Arrays.asList("a", "b")),
                        ((RowFilter.INRowFilter<?>) filter).inValues);
    Assert.assertEquals(1, colIds.size());
    Assert.assertTrue(colIds.contains("f2"));

    setBatch(new Long[] {1L, 0L, 2L, 4L, 3L},
             new String[] {"a", "z", "b", "y", "a"});
    fc.setBatch(batch);

    for (int i = 0; i < batch.size; i++) {
      if (i % 2 == 0) {
        Assert.assertTrue(filter.accept(fc, i));
      } else {
        Assert.assertFalse(filter.accept(fc, i));
      }
    }
  }

  @Test
  public void testORConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .startOr()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "b", "c")
      .end()
      .build();

    Set<String> colIds = new HashSet<>();
    RowFilter filter = RowFilterFactory.createRowFilter(sarg.getExpression(),
                                                        colIds,
                                                        sarg.getLeaves());
    Assert.assertNotNull(filter);
    Assert.assertTrue(filter instanceof RowFilter.ORRowFilter);
    Assert.assertEquals(2, ((RowFilter.ORRowFilter) filter).filters.length);
    Assert.assertEquals(2, colIds.size());
    Assert.assertTrue(colIds.contains("f1"));
    Assert.assertTrue(colIds.contains("f2"));

    // Setup the data such that the OR condition should select every row
    setBatch(new Long[] {1L, 0L, 2L, 4L, 3L},
             new String[] {"z", "a", "y", "b", "x"});
    fc.setBatch(batch);


    for (int i = 0; i < batch.size; i++) {
      Assert.assertTrue(filter.accept(fc, i));
    }
  }

  @Test
  public void testANDConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "b", "c")
      .end()
      .build();

    Set<String> colIds = new HashSet<>();
    RowFilter filter = RowFilterFactory.createRowFilter(sarg.getExpression(),
                                                        colIds,
                                                        sarg.getLeaves());
    Assert.assertNotNull(filter);
    Assert.assertTrue(filter instanceof RowFilter.ANDRowFilter);
    Assert.assertEquals(2, ((RowFilter.ANDRowFilter) filter).filters.length);
    Assert.assertEquals(2, colIds.size());
    Assert.assertTrue(colIds.contains("f1"));
    Assert.assertTrue(colIds.contains("f2"));

    // Setup the data such that the AND condition should not select any row
    setBatch(new Long[] {1L, 0L, 2L, 4L, 3L},
             new String[] {"z", "a", "y", "b", "x"});
    fc.setBatch(batch);

    for (int i = 0; i < batch.size; i++) {
      Assert.assertFalse(filter.accept(fc, i));
    }
  }

  @Test
  public void testUnSupportedSArg() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .equals("f1", PredicateLeaf.Type.LONG, 0L)
      .build();

    thrown.expect(FilterFactory.UnSupportedSArgException.class);
    thrown.expectMessage("is not supported");
    RowFilterFactory.createRowFilter(sarg.getExpression(), new HashSet<>(), sarg.getLeaves());
  }
}