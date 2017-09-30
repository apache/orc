/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl.acid;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * A DeleteSet that reads through the deletes using a merge.  This depends on the fact that each
 * input file and each delete should be sorted the same on the ROW__ID struct.  Even though the
 * bucket value now contains other information it will sort properly because bucket id is in
 * more significant bits than the statement id.
 */
class SortedDeleteSet implements DeleteSet {

  private List<DeleteDeltaWrapper> deletes;

  SortedDeleteSet(List<Reader> readers) throws IOException {
    deletes = new ArrayList<>(readers.size());
    for (Reader reader : readers) deletes.add(new DeleteDeltaWrapper(reader));
  }

  @Override
  public void applyDeletesToBatch(VectorizedRowBatch batch, BitSet selectedBitSet) throws
      IOException {

    for (int i = 0; i < batch.size; i++) {
      assert !batch.selectedInUse;
      LongColumnVector origTxnCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ORIG_TXN_OFFSET];
      long origTxn = origTxnCol.isRepeating ? origTxnCol.vector[0] : origTxnCol.vector[i];
      LongColumnVector bucketCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_BUCKET_OFFSET];
      long bucket = bucketCol.isRepeating ? bucketCol.vector[0] : bucketCol.vector[i];
      LongColumnVector rowIdCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ROW_ID_OFFSET];
      long rowId = rowIdCol.isRepeating ? rowIdCol.vector[0] : rowIdCol.vector[i];
      for (DeleteDeltaWrapper delete : deletes) {
        if (delete.advanceUntil(origTxn, bucket, rowId)) {
          selectedBitSet.set(i);
          break;
        }
      }
    }

  }

  @Override
  public void release() {
    // NOP, as this isn't held in memory
  }

  private static class DeleteDeltaWrapper {
    final RecordReader rows;
    VectorizedRowBatch batch;
    int currentOffset;
    boolean batchDone; // true if we've finished reading whatever's in batch

    DeleteDeltaWrapper(Reader reader) throws IOException {
      rows = reader.rows();
      batchDone = true;
      batch = reader.getSchema().createRowBatch();
    }

    // Returns true if it found a matching record, false otherwise
    boolean advanceUntil(long insertOrigTxn, long insertBucket, long insertRowId)
        throws IOException {
      while (true) {
        if (batchDone) {
          boolean moreToDo = rows.nextBatch(batch);
          if (!moreToDo) return false;
          batchDone = false;
          currentOffset = 0;
        }

        if (currentOffset >= batch.size) {
          batchDone = true;
          continue;
        }

        int batchOffset = batch.selectedInUse ? batch.selected[currentOffset] : currentOffset;
        LongColumnVector delOrigTxnCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ORIG_TXN_OFFSET];
        long delOrigTxn = delOrigTxnCol.isRepeating ? delOrigTxnCol.vector[0] : delOrigTxnCol.vector[batchOffset];
        LongColumnVector delBucketCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_BUCKET_OFFSET];
        long delBucket = delBucketCol.isRepeating ? delBucketCol.vector[0] : delBucketCol.vector[batchOffset];
        LongColumnVector delRowIdCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ROW_ID_OFFSET];
        long delRowId = delRowIdCol.isRepeating ? delRowIdCol.vector[0] : delRowIdCol.vector[batchOffset];

        if (delOrigTxn > insertOrigTxn) {
          return false;
        } else if (delOrigTxn < insertOrigTxn) {
          currentOffset++; // continue
        } else {
          int decodedDelBucket = BucketCodec.V1.decodeWriterId((int)delBucket);
          int decodedInsertBucket = BucketCodec.V1.decodeWriterId((int)insertBucket);
          if (decodedDelBucket > decodedInsertBucket) {
            return false;
          } else if (decodedDelBucket < decodedInsertBucket) {
            currentOffset++; // continue
          } else {
            if (delRowId > insertRowId) {
              return false;
            } else if (delRowId < insertRowId) {
              currentOffset++; // continue
            } else {
              currentOffset++;
              return true;
            }
          }
        }
      }
    }
  }
}
