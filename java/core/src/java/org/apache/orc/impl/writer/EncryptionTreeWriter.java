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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DataMask;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * TreeWriter that handles column encryption.
 * We create a TreeWriter for each of the alternatives with an WriterContext
 * that creates encrypted streams.
 */
public class EncryptionTreeWriter implements TreeWriter {
  // the different writers
  private final TreeWriter[] childrenWriters;
  private final DataMask[] masks;
  // a column vector that we use to apply the masks
  private final VectorizedRowBatch scratch;

  EncryptionTreeWriter(TypeDescription schema,
                              WriterEncryptionVariant encryption,
                              WriterContext context) throws IOException {
    scratch = schema.createRowBatch();
    childrenWriters = new TreeWriterBase[2];
    masks = new DataMask[childrenWriters.length];

    // no mask, encrypted data
    masks[0] = null;
    childrenWriters[0] = Factory.createSubtree(schema, encryption, context);

    // masked unencrypted
    masks[1] = context.getUnencryptedMask(schema.getId());
    childrenWriters[1] = Factory.createSubtree(schema, null, context);
  }

  @Override
  public void writeRootBatch(VectorizedRowBatch batch, int offset,
                             int length) throws IOException {
    scratch.ensureSize(length);
    for(int alt=0; alt < childrenWriters.length; ++alt) {
      // if there is a mask, apply it to each column
      if (masks[alt] != null) {
        for(int col=0; col < scratch.cols.length; ++col) {
          masks[alt].maskData(batch.cols[col], scratch.cols[col], offset,
              length);
        }
        childrenWriters[alt].writeRootBatch(scratch, offset, length);
      } else {
        childrenWriters[alt].writeRootBatch(batch, offset, length);
      }
    }
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    for(int alt=0; alt < childrenWriters.length; ++alt) {
      // if there is a mask, apply it to each column
      if (masks[alt] != null) {
        masks[alt].maskData(vector, scratch.cols[0], offset, length);
        childrenWriters[alt].writeBatch(scratch.cols[0], offset, length);
      } else {
        childrenWriters[alt].writeBatch(vector, offset, length);
      }
    }
  }

  @Override
  public void createRowIndexEntry() throws IOException {
    for(TreeWriter child: childrenWriters) {
      child.createRowIndexEntry();
    }
  }

  @Override
  public void flushStreams() throws IOException {
    for(TreeWriter child: childrenWriters) {
      child.flushStreams();
    }
  }

  @Override
  public void writeStripe(int requiredIndexEntries) throws IOException {
    for(TreeWriter child: childrenWriters) {
      child.writeStripe(requiredIndexEntries);
    }
  }

  @Override
  public void updateFileStatistics(OrcProto.StripeStatistics stats) {
    for(TreeWriter child: childrenWriters) {
      child.updateFileStatistics(stats);
    }
  }

  @Override
  public long estimateMemory() {
    long result = 0;
    for (TreeWriter writer : childrenWriters) {
      result += writer.estimateMemory();
    }
    return result;
  }

  @Override
  public long getRawDataSize() {
    // return the size of the encrypted data
    return childrenWriters[0].getRawDataSize();
  }

  @Override
  public void prepareStripe(int stripeId) {
    for (TreeWriter writer : childrenWriters) {
      writer.prepareStripe(stripeId);
    }
  }

  @Override
  public void writeFileStatistics() throws IOException {
    for (TreeWriter child : childrenWriters) {
      child.writeFileStatistics();
    }
  }

  @Override
  public void getCurrentStatistics(ColumnStatistics[] output) {
    childrenWriters[0].getCurrentStatistics(output);
  }
}
