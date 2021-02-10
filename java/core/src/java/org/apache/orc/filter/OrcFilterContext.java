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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This defines the input for any filter operation.
 * <p>
 * It offers a convenience method for finding the column vector from a given name, that the filters
 * can invoke to get access to the column vector.
 */
public class OrcFilterContext implements MutableFilterContext {

  VectorizedRowBatch batch = null;
  // Cache of field to ColumnVector, this is reset everytime the batch reference changes
  private final Map<String, ColumnVector> vectors;
  private final TypeDescription readSchema;

  public OrcFilterContext(TypeDescription readSchema) {
    this.readSchema = readSchema;
    this.vectors = new HashMap<>();
  }

  public OrcFilterContext setBatch(@NotNull VectorizedRowBatch batch) {
    if (batch != this.batch) {
      this.batch = batch;
      vectors.clear();
    }
    return this;
  }

  @Override
  public void setFilterContext(boolean selectedInUse, int[] selected, int selectedSize) {
    batch.setFilterContext(selectedInUse, selected, selectedSize);
  }

  @Override
  public boolean validateSelected() {
    return batch.validateSelected();
  }

  @Override
  public int[] updateSelected(int i) {
    return batch.updateSelected(i);
  }

  @Override
  public void setSelectedInUse(boolean b) {
    batch.setSelectedInUse(b);
  }

  @Override
  public void setSelected(int[] ints) {
    batch.setSelected(ints);
  }

  @Override
  public void setSelectedSize(int i) {
    batch.setSelectedSize(i);
  }

  @Override
  public void reset() {
    batch.reset();
  }

  @Override
  public boolean isSelectedInUse() {
    return batch.isSelectedInUse();
  }

  @Override
  public int[] getSelected() {
    return batch.getSelected();
  }

  @Override
  public int getSelectedSize() {
    return batch.getSelectedSize();
  }

  public ColumnVector[] getCols() {
    return batch.cols;
  }

  /**
   * Retrieves the column vector that matches the specified name. Allows support for nested struct
   * references e.g. order.date where data is a field in a struct called order.
   *
   * @param name The column name whose vector should be retrieved
   * @return The column vector
   * @throws IllegalArgumentException if the field is not found or if the nested field is not part
   *                                  of a struct
   */
  public ColumnVector findColumnVector(String name) {
    if (!vectors.containsKey(name)) {
      vectors.put(name, findVector(name));
    }

    return vectors.get(name);
  }

  private ColumnVector findVector(String name) {
    String[] refs = name.split(SPLIT_ON_PERIOD);
    TypeDescription schema = readSchema;
    List<String> fNames = schema.getFieldNames();
    ColumnVector[] cols = batch.cols;
    ColumnVector vector;

    // Try to find the required column at the top level
    int idx = findVectorIdx(refs[0], fNames);
    vector = cols[idx];

    // In case we are asking for a nested search further. Only nested struct references are allowed.
    for (int i = 1; i < refs.length; i++) {
      schema = schema.getChildren().get(idx);
      if (schema.getCategory() != TypeDescription.Category.STRUCT) {
        throw new IllegalArgumentException(String.format(
          "Field %s:%s not a struct field does not allow nested reference",
          refs[i - 1],
          schema));
      }
      cols = ((StructColumnVector) vector).fields;
      vector = cols[findVectorIdx(refs[i], schema.getFieldNames())];
    }

    return vector;
  }

  private static int findVectorIdx(String name, List<String> fieldNames) {
    int idx = fieldNames.indexOf(name);
    if (idx < 0) {
      throw new IllegalArgumentException(String.format("Field %s not found in %s",
                                                       name,
                                                       fieldNames));
    }
    return idx;
  }

  private static final String SPLIT_ON_PERIOD = Pattern.quote(".");

}
