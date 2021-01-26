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
import java.util.EnumSet;

/**
 * Handles the Struct rootType for batch handling. The handling assumes that the root
 * {@link org.apache.orc.impl.TreeReaderFactory.StructTreeReader} no nulls. Root Struct vector is
 * not represented as part of the final {@link org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch}.
 */
public class StructBatchReader extends BatchReader {
  // The reader context including row-filtering details
  private final TreeReaderFactory.Context context;
  private final OrcFilterContextImpl filterContext;
  private final TreeReaderFactory.StructTreeReader structReader;

  public StructBatchReader(TypeReader rowReader, TreeReaderFactory.Context context) {
    super(rowReader);
    this.context = context;
    this.filterContext = new OrcFilterContextImpl(context.getSchemaEvolution().getReaderSchema());
    structReader = (TreeReaderFactory.StructTreeReader) rowReader;
  }

  private void readBatchColumn(VectorizedRowBatch batch,
                               TypeReader child,
                               int batchSize,
                               int index,
                               EnumSet<TypeReader.ReadLevel> readLevel)
    throws IOException {
    ColumnVector colVector = batch.cols[index];
    if (colVector != null) {
      if (readLevel.contains(child.getReadLevel())) {
        // Reset the column vector only if the current column is being processed, otherwise don't
        // reset as only its children are being processed
        colVector.reset();
        colVector.ensureSize(batchSize, false);
      }
      child.nextVector(colVector, null, batchSize, batch, readLevel);
    }
  }

  @Override
  public void nextBatch(VectorizedRowBatch batch, int batchSize, EnumSet<TypeReader.ReadLevel> readLevel)
    throws IOException {
    if (readLevel.contains(TypeReader.ReadLevel.LEAD_CHILD)) {
      // selectedInUse = true indicates that the selected vector should be used to determine
      // valid rows in the batch
      batch.selectedInUse = false;
    }
    nextBatchForLevel(batch, batchSize, readLevel);

    // Except when read level only includes FOLLOW, we can set the batch attributes of
    // selectedInUse and size.
    if (readLevel.contains(TypeReader.ReadLevel.LEAD_CHILD)) {
      // these attributes can change as part of the filter application on the batch
      batch.size = batchSize;
    }

    if (!readLevel.contains(TypeReader.ReadLevel.FOLLOW)) {
      // Apply filter callback to reduce number of # rows selected for decoding in the next
      // TreeReaders
      if (this.context.getColumnFilterCallback() != null) {
        this.context.getColumnFilterCallback().accept(filterContext.setBatch(batch));
      }
    }
  }

  private void nextBatchForLevel(VectorizedRowBatch batch, int batchSize, EnumSet<TypeReader.ReadLevel> readLevel)
    throws IOException {
    TypeReader[] children = structReader.fields;
    for (int i = 0; i < children.length
                    && (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (TypeReader.allowChild(children[i], readLevel)) {
        readBatchColumn(batch, children[i], batchSize, i, readLevel);
      }
    }
  }

  @Override
  public void startStripe(StripePlanner planner,  EnumSet<TypeReader.ReadLevel> readLevel) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (TypeReader.allowChild(children[i], readLevel)) {
        children[i].startStripe(planner, readLevel);
      }
    }
  }

  @Override
  public void skipRows(long rows,  EnumSet<TypeReader.ReadLevel> readLevel) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (TypeReader.allowChild(children[i], readLevel)) {
        children[i].skipRows(rows, readLevel);
      }
    }
  }

  @Override
  public void seek(PositionProvider[] index, EnumSet<TypeReader.ReadLevel> readLevel) throws IOException {
    TypeReader[] children = ((TreeReaderFactory.StructTreeReader) rootType).fields;
    for (int i = 0; i < children.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      if (TypeReader.allowChild(children[i], readLevel)) {
        children[i].seek(index, readLevel);
      }
    }
  }
}
