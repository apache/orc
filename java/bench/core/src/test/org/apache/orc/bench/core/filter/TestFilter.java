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
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcFile;
import org.apache.orc.filter.OrcFilterContext;
import org.apache.orc.filter.FilterFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

@RunWith(Parameterized.class)
public class TestFilter {
  private static final Logger LOG = LoggerFactory.getLogger(TestFilter.class);
  private static final long seed = 1024;
  protected final Random rnd = new Random(seed);
  protected final VectorizedRowBatch b = FilterBenchUtil.createBatch(rnd);
  protected final OrcFilterContext fc = new OrcFilterContext(FilterBenchUtil.schema).setBatch(b);
  protected final SearchArgument sArg;
  protected final int[] expSel;
  protected Consumer<OrcFilterContext> filter;

  public TestFilter(String complexity, String filterType, boolean normalize) {
    Map.Entry<SearchArgument, int[]> ft = null;
    switch (complexity) {
      case "simple":
        ft = FilterBenchUtil.createSArg(new Random(seed), b, 5);
        break;
      case "complex":
        ft = FilterBenchUtil.createComplexSArg(new Random(seed), b, 10, 8);
        break;
    }
    sArg = ft.getKey();
    LOG.info("SearchArgument has {} expressions", sArg.getExpression().getChildren().size());
    expSel = ft.getValue();

    switch (filterType) {
      case "row":
        filter = RowFilterFactory.createRowFilter(sArg, normalize);
        break;
      case "vector":
        filter = FilterFactory.createVectorFilter(sArg,
                                                  FilterBenchUtil.schema,
                                                  OrcFile.Version.CURRENT,
                                                  normalize);
        break;
    }
  }

  @Parameterized.Parameters(name = "#{index} - {0}+{1}")
  public static List<Object[]> filters() {
    return Arrays.asList(new Object[][] {
      {"simple", "row", false},
      {"simple", "vector", false},
      {"complex", "row", true},
      {"complex", "vector", true},
      {"complex", "row", false},
      {"complex", "vector", false},
    });
  }

  @Before
  public void setup() {
    FilterBenchUtil.unFilterBatch(fc);
  }

  @Test
  public void testFilter() {
    filter.accept(fc.setBatch(b));
    FilterBenchUtil.validate(fc, expSel);
  }
}