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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A set of delete events.  This is not stored as a simple set but as a trie, since long runs of an
 * original transaction id are very common.  Since this is a trie it is vastly more efficient to
 * access it ordered by original transaction id.  It is assumed that is the most common
 * case since insert files are ordered by this value.
 */
class InMemoryDeleteSet implements DeleteSet {
  private Map<Long, Set<BucketRowId>> trie;

  /**
   * @param readers readers for the delete deltas
   */
  InMemoryDeleteSet(List<Reader> readers) throws IOException {

    // Just to avoid a pathological case where someone has hundreds of delete deltas
    ExecutorService executor = Executors.newFixedThreadPool(Math.max(50, readers.size()));
    Set<Future<Map<Long, Set<BucketRowId>>>> tasks = new HashSet<>(readers.size(), 1.0f);
    for (final Reader reader : readers) {
      tasks.add(executor.submit(
          new Callable<Map<Long, Set<BucketRowId>>>() {
            @Override
            public Map<Long, Set<BucketRowId>> call() throws Exception {

              Map<Long, Set<BucketRowId>> localTrie = new HashMap<>();
              Reader.Options options = reader.options();
              options.searchArgument(null, null)  // Make sure there's no SARG push down
                .range(0, Long.MAX_VALUE) // Make sure we read the whole file
                .isDeleteDelta(true);
              RecordReader rows = reader.rows(options);
              VectorizedRowBatch batch = reader.getSchema().createRowBatch();
              long lastOrigTxn = -1;
              Set<BucketRowId> bucketRowId = null;
              while (rows.nextBatch(batch)) {
                assert batch.cols.length == 5;

                for (int i = 0; i < batch.size; i++) {
                  int offset = batch.selectedInUse ? batch.selected[i] : i;
                  LongColumnVector origTxnCol = (LongColumnVector)batch.cols[AcidConstants.ORIG_TXN_COL_OFFSET];
                  long origTxn = origTxnCol.isRepeating ? origTxnCol.vector[0] : origTxnCol.vector[offset];
                  LongColumnVector bucketCol = (LongColumnVector)batch.cols[AcidConstants.BUCKET_COL_OFFSET];
                  long bucket = bucketCol.isRepeating ? bucketCol.vector[0] : bucketCol.vector[offset];
                  LongColumnVector rowIdCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_COL_OFFSET];
                  long rowId = rowIdCol.isRepeating ? rowIdCol.vector[0] : rowIdCol.vector[offset];

                  // Take advantage of the potentially long runs of original txn ids
                  if (origTxn != lastOrigTxn) {
                    bucketRowId = localTrie.get(origTxn);
                    lastOrigTxn = origTxn;
                  }
                  if (bucketRowId == null) {
                    bucketRowId = new HashSet<>();
                    localTrie.put(origTxn, bucketRowId);
                  }
                  bucketRowId.add(new BucketRowId(BucketCodec.V1.decodeWriterId((int)bucket), rowId));
                }
              }
              return localTrie;
            }
          }));
    }

    while (!tasks.isEmpty()) {
      // Move the results out of the individual tries to the master trie as they finish.  Don't
      // wait for them in order to avoid serializing in the case where the early ones are bigger
      // (which is quite likely since minor compactions will create large files before any
      // smaller single txn deltas).
      try {
        // Dare you to get more <> in a type than this!
        while (!tasks.isEmpty()) {
          Iterator<Future<Map<Long, Set<BucketRowId>>>> iter = tasks.iterator();
          while (iter.hasNext()) {
            Future<Map<Long, Set<BucketRowId>>> task = iter.next();
            if (task.isDone()) {
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
              iter.remove();
            }
          }
        }
        Thread.sleep(10);
      } catch (InterruptedException|ExecutionException e) {
        throw new IOException(e);
      }
    }
  }

  // evaluate which records in a VectorizedRowBatch should be deleted
  @Override
  public void applyDeletesToBatch(VectorizedRowBatch batch, BitSet selectedBitSet) {
    long prevOrigTxn = -1;

    assert !batch.selectedInUse;
    Set<BucketRowId> s = null;
    LongColumnVector origTxnCol = (LongColumnVector)batch.cols[AcidConstants.ORIG_TXN_COL_OFFSET];
    LongColumnVector bucketCol = (LongColumnVector)batch.cols[AcidConstants.BUCKET_COL_OFFSET];
    LongColumnVector rowIdCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_COL_OFFSET];
    for (int row = 0; row < batch.size; row++) {
      long origTxn = origTxnCol.isRepeating ? origTxnCol.vector[0] : origTxnCol.vector[row];

      if (origTxn != prevOrigTxn) {
        // This is either the first txn or we changed origTxn numbers and we need to fetch the
        // proper part of the trie
        s = trie.get(origTxn);
        prevOrigTxn = origTxn;
      }
      if (s != null) {
        int bucket = bucketCol.isRepeating ? (int)bucketCol.vector[0] : (int)bucketCol.vector[row];
        long rowId = rowIdCol.isRepeating ? rowIdCol.vector[0] : rowIdCol.vector[row];
        assert BucketCodec.determineVersion(bucket).equals(BucketCodec.V1);
        if (s.contains(new BucketRowId(BucketCodec.V1.decodeWriterId(bucket), rowId))) {
          selectedBitSet.clear(row);
        }
      }

    }
  }

  private static class BucketRowId {
    final int bucket;
    final long rowId;

    BucketRowId(int bucket, long rowId) {
      this.bucket = bucket;
      this.rowId = rowId;
    }

    @Override
    public int hashCode() {
      return bucket * 31 + (int)rowId;
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

