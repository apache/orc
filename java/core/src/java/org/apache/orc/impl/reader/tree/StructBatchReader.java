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
import org.apache.orc.impl.TreeReaderFactory;

import java.io.IOException;

public class StructBatchReader extends BatchReader<TreeReaderFactory.StructTreeReader> {

  public StructBatchReader(TreeReaderFactory.StructTreeReader rowReader) {
    super(rowReader);
  }

  @Override
  public void nextBatch(VectorizedRowBatch batch, int batchSize) throws IOException {
    for (int i = 0; i < rootType.fields.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      ColumnVector colVector = batch.cols[i];
      if (colVector != null) {
        colVector.reset();
        colVector.ensureSize(batchSize, false);
        rootType.fields[i].nextVector(colVector, null, batchSize);
      }
    }
    resetBatch(batch, batchSize);
  }
}
