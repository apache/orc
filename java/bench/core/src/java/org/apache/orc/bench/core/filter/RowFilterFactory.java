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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.filter.OrcFilterContext;
import org.apache.orc.filter.FilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class RowFilterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RowFilterFactory.class);

  public static Consumer<OrcFilterContext> createRowFilter(SearchArgument sArg) {
    return createRowFilter(sArg, false);
  }

  static Consumer<OrcFilterContext> createRowFilter(SearchArgument sArg, boolean normalize) {
    try {
      Set<String> colIds = new HashSet<>();
      RowFilter filter = createRowFilter(normalize ? sArg.getExpression() :
                                           sArg.getUnexpandedExpression(),
                                         colIds,
                                         sArg.getLeaves());
      return new RowBatchFilter(filter, colIds.toArray(new String[0]));
    } catch (FilterFactory.UnSupportedSArgException e) {
      LOG.warn("SArg: {} is not supported\n{}", sArg, e.getMessage());
      return null;
    }
  }

  public static RowFilter createRowFilter(ExpressionTree expr,
                                          Set<String> colIds,
                                          List<PredicateLeaf> leaves)
    throws FilterFactory.UnSupportedSArgException {
    RowFilter result;
    switch (expr.getOperator()) {
      case OR:
        RowFilter[] orFilters = new RowFilter[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
          orFilters[i] = createRowFilter(expr.getChildren().get(i), colIds, leaves);
        }
        result = new RowFilter.ORRowFilter(orFilters);
        break;
      case AND:
        RowFilter[] andFilters = new RowFilter[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
          andFilters[i] = createRowFilter(expr.getChildren().get(i), colIds, leaves);
        }
        result = new RowFilter.ANDRowFilter(andFilters);
        break;
      case LEAF:
        result = createLeafFilter(leaves.get(expr.getLeaf()), colIds);
        break;
      default:
        throw new FilterFactory.UnSupportedSArgException(String.format(
          "SArg Expression: %s is not supported",
          expr));
    }
    return result;
  }

  private static RowFilter createLeafFilter(PredicateLeaf leaf,
                                            Set<String> colIds)
    throws FilterFactory.UnSupportedSArgException {
    RowFilter result;
    colIds.add(leaf.getColumnName());

    switch (leaf.getOperator()) {
      case IN:
        result = new RowFilter.INRowFilter<>(leaf.getColumnName(),
                                             new HashSet<>(leaf.getLiteralList()),
                                             getGet(leaf.getType()));
        break;
      default:
        throw new FilterFactory.UnSupportedSArgException(String.format(
          "Predicate: %s is not supported",
          leaf));
    }
    return result;
  }

  public static BiFunction<ColumnVector, Integer, Object> getGet(PredicateLeaf.Type type) {
    switch (type) {
      case LONG:
        return RowFilterFactory::getLong;
      case STRING:
        return RowFilterFactory::getString;
      default:
        return null;
    }
  }

  public static long getLong(ColumnVector v, int rowIdx) {
    return ((LongColumnVector) v).vector[rowIdx];
  }

  static String getString(ColumnVector v, int rowIdx) {
    BytesColumnVector bv = (BytesColumnVector) v;
    return new String(bv.vector[rowIdx], bv.start[rowIdx], bv.length[rowIdx]);
  }

  public static <T> T getValue(ColumnVector v,
                               int rowIdx,
                               BiFunction<ColumnVector, Integer, T> f) {
    int idx = rowIdx;
    if (v.isRepeating) {
      idx = 0;
    }
    if (v.noNulls || !v.isNull[idx]) {
      return f.apply(v, idx);
    } else {
      return null;
    }
  }

  public static class RowBatchFilter implements Consumer<OrcFilterContext> {

    private final RowFilter filter;
    private final String[] colNames;

    private RowBatchFilter(RowFilter filter, String[] colNames) {
      this.filter = filter;
      this.colNames = colNames;
    }

    @Override
    public void accept(OrcFilterContext batch) {
      int size = 0;
      int[] selected = batch.getSelected();

      for (int i = 0; i < batch.getSelectedSize(); i++) {
        if (filter.accept(batch, i)) {
          selected[size] = i;
          size += 1;
        }
      }
      batch.setSelectedInUse(true);
      batch.setSelected(selected);
      batch.setSelectedSize(size);
    }

    public String[] getColNames() {
      return colNames;
    }
  }
}
