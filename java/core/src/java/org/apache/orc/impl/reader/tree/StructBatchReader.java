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
import org.apache.orc.filter.OrcFilterContext;
import org.apache.orc.impl.TreeReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StructBatchReader extends BatchReader {
  private static final Logger LOG = LoggerFactory.getLogger(StructBatchReader.class);
  // The reader context including row-filtering details
  private final TreeReaderFactory.Context context;
  private final TreeReaderFactory.StructTreeReader structReader;
  private final OrcFilterContext fc;

  public StructBatchReader(TypeReader rowReader, TreeReaderFactory.Context context) {
    super(rowReader);
    this.context = context;
    this.fc = new OrcFilterContext(context.getSchemaEvolution().getReaderSchema());
    if (rowReader instanceof TreeReaderFactory.StructTreeReader) {
      structReader = (TreeReaderFactory.StructTreeReader) rowReader;
    } else {
      structReader = (TreeReaderFactory.StructTreeReader) ((LevelTypeReader)rowReader).getReader();
    }
  }

  private void readBatchColumn(VectorizedRowBatch batch,
                               TypeReader[] children,
                               int batchSize,
                               int index,
                               ReadLevel readLevel)
    throws IOException {
    ColumnVector colVector = batch.cols[index];
    if (colVector != null) {
      colVector.reset();
      colVector.ensureSize(batchSize, false);
      children[index].nextVector(colVector, null, batchSize, batch, readLevel);
    }
  }

  @Override
  public void nextBatch(VectorizedRowBatch batch, int batchSize, ReadLevel readLevel)
    throws IOException {
    nextBatchLevel(batch, batchSize, readLevel);

    if (readLevel == ReadLevel.LEAD) {
      // Apply filter callback to reduce number of # rows selected for decoding in the next
      // TreeReaders
      if (this.context.getColumnFilterCallback() != null) {
        this.context.getColumnFilterCallback().accept(fc.setBatch(batch));
      }
    }
  }

  private void nextBatchLevel(VectorizedRowBatch batch, int batchSize, ReadLevel readLevel) throws IOException {
    TypeReader[] children = structReader.fields;

    if (readLevel != ReadLevel.FOLLOW) {
      // In case of FOLLOW we leave the selectedInUse untouched.
      batch.selectedInUse = false;
    }

    for (int i = 0; i < children.length
                    && (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      readBatchColumn(batch, children, batchSize, i, readLevel);
    }

    if (readLevel != ReadLevel.FOLLOW) {
      // Set the batch size when not dealing with follow columns
      batch.size = batchSize;
    }
  }
}
