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

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.filter.VectorFilter;
import org.apache.orc.impl.filter.FilterFactory;
import org.apache.orc.impl.filter.IsNotNullFilter;
import org.apache.orc.impl.filter.IsNullFilter;
import org.apache.orc.impl.filter.LeafFilter;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Set;

import static org.apache.orc.impl.TreeReaderFactory.isDecimalAsLong;

public class LeafFilterFactory {

  private LeafFilterFactory() {}

  private static LeafFilter createEqualsFilter(String colName,
                                               PredicateLeaf.Type type,
                                               Object literal,
                                               TypeDescription colType,
                                               OrcFile.Version version,
                                               boolean negated) {
    switch (type) {
      case BOOLEAN:
        return new LongEquals(colName, (boolean) literal ? 1L : 0L, negated);
      case DATE:
        return new LongEquals(colName,
            ((Date) literal).toLocalDate().toEpochDay(), negated);
      case DECIMAL:
        HiveDecimalWritable d = (HiveDecimalWritable) literal;
        assert d.scale() <= colType.getScale();
        if (isDecimalAsLong(version, colType.getPrecision())) {
          return new LongEquals(colName, d.serialize64(colType.getScale()), negated);
        } else {
          return new DecimalEquals(colName, d, negated);
        }
      case FLOAT:
        return new FloatEquals(colName, literal, negated);
      case LONG:
        return new LongEquals(colName, literal, negated);
      case STRING:
        return new StringEquals(colName, literal, negated);
      case TIMESTAMP:
        return new TimestampEquals(colName, literal, negated);
      default:
        throw new IllegalArgumentException(String.format("Equals does not support type: %s", type));
    }
  }

  private static LeafFilter createLessThanFilter(String colName,
                                                 PredicateLeaf.Type type,
                                                 Object literal,
                                                 TypeDescription colType,
                                                 OrcFile.Version version,
                                                 boolean negated) {
    switch (type) {
      case BOOLEAN:
        return new LongLessThan(colName, (boolean) literal ? 1L : 0L, negated);
      case DATE:
        return new LongLessThan(colName,
            ((Date) literal).toLocalDate().toEpochDay(), negated);
      case DECIMAL:
        HiveDecimalWritable d = (HiveDecimalWritable) literal;
        assert d.scale() <= colType.getScale();
        if (isDecimalAsLong(version, colType.getPrecision())) {
          return new LongLessThan(colName, d.serialize64(colType.getScale()),
              negated);
        } else {
          return new DecimalLessThan(colName, d, negated);
        }
      case FLOAT:
        return new FloatLessThan(colName, literal, negated);
      case LONG:
        return new LongLessThan(colName, literal, negated);
      case STRING:
        return new StringLessThan(colName, literal, negated);
      case TIMESTAMP:
        return new TimestampLessThan(colName, literal, negated);
      default:
        throw new IllegalArgumentException(String.format("LessThan does not support type: %s", type));
    }
  }

  private static LeafFilter createLessThanEqualsFilter(String colName,
                                                       PredicateLeaf.Type type,
                                                       Object literal,
                                                       TypeDescription colType,
                                                       OrcFile.Version version,
                                                       boolean negated) {
    switch (type) {
      case BOOLEAN:
        return new LongLessThanEquals(colName, (boolean) literal ? 1L : 0L,
            negated);
      case DATE:
        return new LongLessThanEquals(colName,
            ((Date) literal).toLocalDate().toEpochDay(), negated);
      case DECIMAL:
        HiveDecimalWritable d = (HiveDecimalWritable) literal;
        assert d.scale() <= colType.getScale();
        if (isDecimalAsLong(version, colType.getPrecision())) {
          return new LongLessThanEquals(colName,
              d.serialize64(colType.getScale()), negated);
        } else {
          return new DecimalLessThanEquals(colName, d, negated);
        }
      case FLOAT:
        return new FloatLessThanEquals(colName, literal, negated);
      case LONG:
        return new LongLessThanEquals(colName, literal, negated);
      case STRING:
        return new StringLessThanEquals(colName, literal, negated);
      case TIMESTAMP:
        return new TimestampLessThanEquals(colName, literal, negated);
      default:
        throw new IllegalArgumentException(String.format("LessThanEquals does not support type: %s", type));
    }
  }

  private static LeafFilter createBetweenFilter(String colName,
                                                PredicateLeaf.Type type,
                                                Object low,
                                                Object high,
                                                TypeDescription colType,
                                                OrcFile.Version version,
                                                boolean negated) {
    switch (type) {
      case BOOLEAN:
        return new LongBetween(colName, (boolean) low ? 1L : 0L,
            (boolean) high ? 1L : 0L, negated);
      case DATE:
        return new LongBetween(colName, ((Date) low).toLocalDate().toEpochDay(),
            ((Date) high).toLocalDate().toEpochDay(), negated);
      case DECIMAL:
        HiveDecimalWritable dLow = (HiveDecimalWritable) low;
        HiveDecimalWritable dHigh = (HiveDecimalWritable) high;
        assert dLow.scale() <= colType.getScale() && dLow.scale() <= colType.getScale();
        if (isDecimalAsLong(version, colType.getPrecision())) {
          return new LongBetween(colName, dLow.serialize64(colType.getScale()),
              dHigh.serialize64(colType.getScale()), negated);
        } else {
          return new DecimalBetween(colName, dLow, dHigh, negated);
        }
      case FLOAT:
        return new FloatBetween(colName, low, high, negated);
      case LONG:
        return new LongBetween(colName, low, high, negated);
      case STRING:
        return new StringBetween(colName, low, high, negated);
      case TIMESTAMP:
        return new TimestampBetween(colName, low, high, negated);
      default:
        throw new IllegalArgumentException(String.format("Between does not support type: %s", type));
    }
  }

  private static LeafFilter createInFilter(String colName,
                                           PredicateLeaf.Type type,
                                           List<Object> inList,
                                           TypeDescription colType,
                                           OrcFile.Version version,
                                           boolean negated) {
    switch (type) {
      case BOOLEAN:
        return new LongIn(colName,
            inList.stream().map((Object v) -> (boolean) v ? 1L : 0L)
                .collect(Collectors.toList()), negated);
      case DATE:
        return new LongIn(colName,
            inList.stream()
                .map((Object v) -> ((Date) v).toLocalDate().toEpochDay())
                .collect(Collectors.toList()), negated);
      case DECIMAL:
        if (isDecimalAsLong(version, colType.getPrecision())) {
          List<Object> values = new ArrayList<>(inList.size());
          for (Object o : inList) {
            HiveDecimalWritable v = (HiveDecimalWritable) o;
            assert v.scale() <= colType.getScale();
            values.add(v.serialize64(colType.getScale()));
          }
          return new LongIn(colName, values, negated);
        } else {
          return new DecimalIn(colName, inList, negated);
        }
      case FLOAT:
        return new FloatIn(colName, inList, negated);
      case LONG:
        return new LongIn(colName, inList, negated);
      case STRING:
        return new StringIn(colName, inList, negated);
      case TIMESTAMP:
        return new TimestampIn(colName, inList, negated);
      default:
        throw new IllegalArgumentException(String.format("In does not support type: %s", type));
    }
  }

  public static VectorFilter createLeafVectorFilter(PredicateLeaf leaf,
                                                    Set<String> colIds,
                                                    TypeDescription readSchema,
                                                    OrcFile.Version version,
                                                    boolean negated)
      throws FilterFactory.UnSupportedSArgException {
    colIds.add(leaf.getColumnName());
    TypeDescription colType = readSchema.findSubtype(leaf.getColumnName());

    switch (leaf.getOperator()) {
      case IN:
        return createInFilter(leaf.getColumnName(),
            leaf.getType(),
            leaf.getLiteralList(),
            colType,
            version,
            negated);
      case EQUALS:
        return createEqualsFilter(leaf.getColumnName(),
            leaf.getType(),
            leaf.getLiteral(),
            colType,
            version,
            negated);
      case LESS_THAN:
        return createLessThanFilter(leaf.getColumnName(),
            leaf.getType(),
            leaf.getLiteral(),
            colType,
            version,
            negated);
      case LESS_THAN_EQUALS:
        return createLessThanEqualsFilter(leaf.getColumnName(),
            leaf.getType(),
            leaf.getLiteral(),
            colType,
            version,
            negated);
      case BETWEEN:
        return createBetweenFilter(leaf.getColumnName(),
            leaf.getType(),
            leaf.getLiteralList().get(0),
            leaf.getLiteralList().get(1),
            colType,
            version,
            negated);
      case IS_NULL:
        return negated ? new IsNotNullFilter(leaf.getColumnName()) :
            new IsNullFilter(leaf.getColumnName());
      default:
        throw new FilterFactory.UnSupportedSArgException(String.format("Predicate: %s is not supported", leaf));
    }
  }
}
