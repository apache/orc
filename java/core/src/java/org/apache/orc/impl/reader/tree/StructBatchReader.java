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
package org.apache.orc.impl.reader.tree;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.TreeReaderFactory;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;
import java.util.Set;

/**
 * Handles the Struct rootType for batch handling. The handling assumes that the root
 * {@link org.apache.orc.impl.TreeReaderFactory.StructTreeReader} has no nulls, this is required as
 * the {@link VectorizedRowBatch} does not represent the root Struct as a vector.
 */
public class StructBatchReader extends BatchReader {
  // The reader context including row-filtering details
  private final TreeReaderFactory.Context context;
  private final OrcFilterContextImpl filterContext;

  public StructBatchReader(TreeReaderFactory.StructTreeReader rowReader, TreeReaderFactory.Context context) {
    super(rowReader);
    this.context = context;
    this.filterContext = new OrcFilterContextImpl(context.getSchemaEvolution().getReaderSchema());
  }

  private void readBatchColumn(VectorizedRowBatch batch, TypeReader[] children, int batchSize, int index)
      throws IOException {
    ColumnVector colVector = batch.cols[index];
    if (colVector != null) {
      colVector.reset();
      colVector.ensureSize(batchSize, false);
      children[index].nextVector(colVector, null, batchSize, batch);
    }
  }

  @Override
  public void nextBatch(VectorizedRowBatch batch, int batchSize) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    // Early expand fields --> apply filter --> expand remaining fields
    Set<Integer> earlyExpandCols = context.getColumnFilterIds();

    // Clear selected and early expand columns used in Filter
    batch.selectedInUse = false;
    for (int i = 0; i < children.length && !earlyExpandCols.isEmpty() &&
        (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (earlyExpandCols.contains(children[i].getColumnId())) {
        readBatchColumn(batch, children, batchSize, i);
      }
    }
    // Since we are going to filter rows based on some column values set batch.size earlier here
    batch.size = batchSize;

    // Apply filter callback to reduce number of # rows selected for decoding in the next TreeReaders
    if (!earlyExpandCols.isEmpty() && this.context.getColumnFilterCallback() != null) {
      this.context.getColumnFilterCallback().accept(filterContext.setBatch(batch));
    }

    // Read the remaining columns applying row-level filtering
    for (int i = 0; i < children.length &&
        (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (!earlyExpandCols.contains(children[i].getColumnId())) {
        readBatchColumn(batch, children, batchSize, i);
      }
    }
  }

  @Override
  public void startStripe(StripePlanner planner) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      children[i].startStripe(planner);
    }
  }

  @Override
  public void skipRows(long rows) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      children[i].skipRows(rows);
    }
  }

  @Override
  public void seek(PositionProvider[] index) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      children[i].seek(index);
    }
  }
}
