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
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A set of delete events.  This is not stored as a simple set but as a trie, since long runs of
 * original transaction id are very common.  Since this is a trie it is vastly more efficient to
 * access it ordered by original transaction id.  It is assumed that is the most common
 * case since insert files are ordered by this value.
 *
 * This class assumes that is it will be used for a given {@link ParsedAcidDirectory}.
 * The delete deltas that have already been read are tracked so if a request is made to reread a
 * given delta it will be ignored.
 *
 * Note that a particular DeleteSet assumes that all
 * records came from the same {@link org.apache.hadoop.hive.common.ValidTxnList}.
 */
class InMemoryDeleteSet implements DeleteSet {
  private Map<Long, Set<BucketRowId>> trie;
  private final long size;
  private boolean inUse;

  /**
   * This should only be called by {@link DeleteSetCache}.  If you need a DeleteSet call
   * {@link DeleteSetCache#getDeleteSet(ParsedAcidDirectory)}.  (Java really needs 'friend'.)
   * @param readers readers for the delete deltas
   */
  InMemoryDeleteSet(List<Reader> readers, long size) throws IOException {
    this.size = size;

    // Just to avoid a pathological case where someone has hundreds of delete deltas
    ExecutorService executor = Executors.newFixedThreadPool(Math.max(50, readers.size()));
    Set<Future<Map<Long, Set<BucketRowId>>>> tasks = new HashSet<>(readers.size(), 1.0f);
    for (final Reader reader : readers) {
      tasks.add(executor.submit(
          new Callable<Map<Long, Set<BucketRowId>>>() {
            @Override
            public Map<Long, Set<BucketRowId>> call() throws Exception {

              // On the assumption that a particular delete delta will fit in cache, all the deletes for a
              // particular original transaction might fit into cache, but this whole set will not, we
              // don't put each record directly into the trie.  Instead we build a local (hopefully
              // smaller) trie and then copy it into the master one.
              Map<Long, Set<BucketRowId>> localTrie = new HashMap<>();
              RecordReader rows = reader.rows();
              VectorizedRowBatch batch = reader.getSchema().createRowBatch();
              while (rows.nextBatch(batch)) {
                assert batch.cols.length == 5;

                for (int i = 0; i < batch.size; i++) {
                  int offset = batch.selectedInUse ? batch.selected[i] : i;
                  LongColumnVector origTxnCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ORIG_TXN_OFFSET];
                  long origTxn = origTxnCol.isRepeating ? origTxnCol.vector[0] : origTxnCol.vector[offset];
                  LongColumnVector bucketCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_BUCKET_OFFSET];
                  long bucket = bucketCol.isRepeating ? bucketCol.vector[0] : bucketCol.vector[offset];
                  LongColumnVector rowIdCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ROW_ID_OFFSET];
                  long rowId = rowIdCol.isRepeating ? rowIdCol.vector[0] : rowIdCol.vector[offset];

                  Set<BucketRowId> s = localTrie.get(origTxn);
                  if (s == null) {
                    s = new HashSet<>();
                    localTrie.put(origTxn, s);
                  }
                  s.add(new BucketRowId(bucket, rowId));
                }
              }
              return localTrie;
            }
          }));
    }

    while (!tasks.isEmpty()) {
      for (Future<Map<Long, Set<BucketRowId>>> task : tasks) {
        if (task.isDone()) {
          try {
            Map<Long, Set<BucketRowId>> localTrie = task.get();
            // If this is the first one then just use it.
            if (trie == null) {
              trie = localTrie;
            } else {
              // copy it in
              for (Map.Entry<Long, Set<BucketRowId>> e : localTrie.entrySet()) {
                Set<BucketRowId> s = trie.get(e.getKey());
                if (s == null) trie.put(e.getKey(), e.getValue());
                else s.addAll(e.getValue());
              }
            }
            tasks.remove(task);
          } catch (InterruptedException|ExecutionException e) {
            throw new IOException(e);
          }
        }
      }
    }
  }

  /**
   * Only to be called by {@link DeleteSetCache}.
   */
  long size() {
    return size;
  }

  /**
   * Only to be called by {@link DeleteSetCache}.
   */
  void setInUse() {
    inUse = true;
  }

  boolean isInUse() {
    return inUse;
  }

  // evaluate which records in a VectorizedRowBatch should be deleted
  @Override
  public void applyDeletesToBatch(VectorizedRowBatch batch, BitSet selectedBitSet) {
    long origTxn = 0;
    assert !batch.selectedInUse;
    if (batch.cols[AcidConstants.ROW_ID_ORIG_TXN_OFFSET].isRepeating) {
      origTxn = ((LongColumnVector) batch.cols[AcidConstants.ROW_ID_ORIG_TXN_OFFSET]).vector[0];
      evaluateAgainstOneOrigTxn(batch, selectedBitSet, origTxn, 0, batch.size);
    } else {
      long prevOrig = 0;
      int start = 0;
      for (int row = 0; row < batch.size; row++) {
        origTxn = ((LongColumnVector)batch.cols[AcidConstants.ROW_ID_ORIG_TXN_OFFSET]).vector[row];
        if (origTxn != prevOrig) {
          if (prevOrig != 0) {
            evaluateAgainstOneOrigTxn(batch, selectedBitSet, origTxn, start, row);
            start = row;
          }
          prevOrig = origTxn;
        }
      }
      // Call for the final batch
      evaluateAgainstOneOrigTxn(batch, selectedBitSet, origTxn, start, batch.size);
    }
  }

  @Override
  public void release() {
    inUse = false;
  }

  private void evaluateAgainstOneOrigTxn(VectorizedRowBatch batch, BitSet selectedBitSet,
                                         long origTxn, int start, int end) {
    Set<BucketRowId> s = trie.get(origTxn);
    if (s != null) {
      LongColumnVector bucketCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_BUCKET_OFFSET];
      LongColumnVector rowIdCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ROW_ID_OFFSET];

      for (int row = start; row < end; row++) {
        int bucket = bucketCol.isRepeating ? (int)bucketCol.vector[0] : (int)bucketCol.vector[row];
        long rowId = rowIdCol.isRepeating ? rowIdCol.vector[0] : rowIdCol.vector[row];
        assert BucketCodec.determineVersion(bucket) == BucketCodec.V1;
        if (s.contains(new BucketRowId(BucketCodec.V1.decodeWriterId(bucket), rowId))) {
          selectedBitSet.clear(row);
        }
      }
    }

  }

  private static class BucketRowId {
    final long bucket;
    final long rowId;

    BucketRowId(long bucket, long rowId) {
      this.bucket = bucket;
      this.rowId = rowId;
    }

    @Override
    public int hashCode() {
      return (int)(bucket * 31 + rowId);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof BucketRowId) {
        BucketRowId that = (BucketRowId)obj;
        return this.rowId == that.rowId && this.bucket == that.bucket;
      }
      return false;
    }
  }
}

