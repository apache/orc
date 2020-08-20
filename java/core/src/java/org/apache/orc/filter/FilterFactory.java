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

import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.filter.impl.AndFilter;
import org.apache.orc.filter.impl.BatchFilter;
import org.apache.orc.filter.impl.IsNotNullFilter;
import org.apache.orc.filter.impl.IsNullFilter;
import org.apache.orc.filter.impl.LeafFilterFactory;
import org.apache.orc.filter.impl.OrFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class FilterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FilterFactory.class);

  // For Testing only
  public static Consumer<OrcFilterContext> createVectorFilter(SearchArgument sArg,
                                                           TypeDescription readSchema) {
    return createVectorFilter(sArg, readSchema, OrcFile.Version.UNSTABLE_PRE_2_0);
  }

  public static Consumer<OrcFilterContext> createVectorFilter(SearchArgument sArg,
                                                           TypeDescription readSchema,
                                                           OrcFile.Version version) {
    return createVectorFilter(sArg, readSchema, version, false);
  }

  public static Consumer<OrcFilterContext> createVectorFilter(SearchArgument sArg,
                                                           TypeDescription readSchema,
                                                           OrcFile.Version version,
                                                           boolean normalize) {
    try {
      Set<String> colIds = new HashSet<>();
      //TODO Once HIVE-24458 is merged, should use the getUnexpandedExpression for performance
      // reasons
      VectorFilter filter = createVectorFilter(sArg.getExpression(),
                                               colIds,
                                               sArg.getLeaves(),
                                               readSchema,
                                               version);
      return new BatchFilter(filter, colIds.toArray(new String[0]));
    } catch (UnSupportedSArgException e) {
      LOG.warn("SArg: {} is not supported\n{}", sArg, e.getMessage());
      return null;
    }
  }

  public static VectorFilter createVectorFilter(ExpressionTree expr,
                                                Set<String> colIds,
                                                List<PredicateLeaf> leaves,
                                                TypeDescription readSchema,
                                                OrcFile.Version version)
    throws UnSupportedSArgException {
    VectorFilter result;
    switch (expr.getOperator()) {
      case OR:
        VectorFilter[] orFilters = new VectorFilter[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
          orFilters[i] = createVectorFilter(expr.getChildren().get(i),
                                            colIds,
                                            leaves,
                                            readSchema,
                                            version);
        }
        result = new OrFilter(orFilters);
        break;
      case AND:
        VectorFilter[] andFilters = new VectorFilter[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
          andFilters[i] = createVectorFilter(expr.getChildren().get(i),
                                             colIds,
                                             leaves,
                                             readSchema,
                                             version);
        }
        result = new AndFilter(andFilters);
        break;
      case NOT:
        // Not is expected to be pushed down that it only happens on leaf filters
        ExpressionTree leaf = expr.getChildren().get(0);
        assert leaf.getOperator() == ExpressionTree.Operator.LEAF;
        result = createNotLeafVectorFilter(leaves.get(leaf.getLeaf()),
                                           colIds,
                                           readSchema,
                                           version);
        break;
      case LEAF:
        result = createLeafVectorFilter(leaves.get(expr.getLeaf()), colIds, readSchema, version);
        break;
      default:
        throw new UnSupportedSArgException(String.format("SArg expression: %s is not supported",
                                                         expr));
    }
    return result;
  }

  private static VectorFilter createLeafVectorFilter(PredicateLeaf leaf,
                                                     Set<String> colIds,
                                                     TypeDescription readSchema,
                                                     OrcFile.Version version)
    throws UnSupportedSArgException {
    VectorFilter result;
    colIds.add(leaf.getColumnName());
    TypeDescription colType = readSchema.findSubtype(leaf.getColumnName());

    switch (leaf.getOperator()) {
      case IN:
        result = LeafFilterFactory.createInFilter(leaf.getColumnName(),
                                                  leaf.getType(),
                                                  leaf.getLiteralList(),
                                                  colType,
                                                  version);
        break;
      case EQUALS:
        result = LeafFilterFactory.createEqualsFilter(leaf.getColumnName(),
                                                      leaf.getType(),
                                                      leaf.getLiteral(),
                                                      colType,
                                                      version);
        break;
      case LESS_THAN:
        result = LeafFilterFactory.createLessThanFilter(leaf.getColumnName(),
                                                        leaf.getType(),
                                                        leaf.getLiteral(),
                                                        colType,
                                                        version);
        break;
      case LESS_THAN_EQUALS:
        result = LeafFilterFactory.createLessThanEqualsFilter(leaf.getColumnName(),
                                                              leaf.getType(),
                                                              leaf.getLiteral(),
                                                              colType,
                                                              version);
        break;
      case BETWEEN:
        result = LeafFilterFactory.createBetweenFilter(leaf.getColumnName(),
                                                       leaf.getType(),
                                                       leaf.getLiteralList().get(0),
                                                       leaf.getLiteralList().get(1),
                                                       colType,
                                                       version);
        break;
      case IS_NULL:
        result = new IsNullFilter(leaf.getColumnName());
        break;
      default:
        throw new UnSupportedSArgException(String.format("Predicate: %s is not supported", leaf));
    }
    return result;
  }

  private static VectorFilter createNotLeafVectorFilter(PredicateLeaf leaf,
                                                        Set<String> colIds,
                                                        TypeDescription readSchema,
                                                        OrcFile.Version version)
    throws UnSupportedSArgException {
    VectorFilter result;
    colIds.add(leaf.getColumnName());
    TypeDescription colType = readSchema.findSubtype(leaf.getColumnName());

    switch (leaf.getOperator()) {
      case IN:
        result = LeafFilterFactory.createNotInFilter(leaf.getColumnName(),
                                                     leaf.getType(),
                                                     leaf.getLiteralList(),
                                                     colType,
                                                     version);
        break;
      case EQUALS:
        result = LeafFilterFactory.createNotEqualsFilter(leaf.getColumnName(),
                                                         leaf.getType(),
                                                         leaf.getLiteral(),
                                                         colType,
                                                         version);
        break;
      case LESS_THAN:
        result = LeafFilterFactory.createNotLessThanFilter(leaf.getColumnName(),
                                                           leaf.getType(),
                                                           leaf.getLiteral(),
                                                           colType,
                                                           version);
        break;
      case LESS_THAN_EQUALS:
        result = LeafFilterFactory.createNotLessThanEqualsFilter(leaf.getColumnName(),
                                                                 leaf.getType(),
                                                                 leaf.getLiteral(),
                                                                 colType,
                                                                 version);
        break;
      case BETWEEN:
        result = LeafFilterFactory.createNotBetweenFilter(leaf.getColumnName(),
                                                          leaf.getType(),
                                                          leaf.getLiteralList().get(0),
                                                          leaf.getLiteralList().get(1),
                                                          colType,
                                                          version);
        break;
      case IS_NULL:
        result = new IsNotNullFilter(leaf.getColumnName());
        break;
      default:
        throw new UnSupportedSArgException(String.format("Predicate: %s is not supported", leaf));
    }
    return result;
  }

  public static class UnSupportedSArgException extends Exception {

    public UnSupportedSArgException(String message) {
      super(message);
    }
  }
}
