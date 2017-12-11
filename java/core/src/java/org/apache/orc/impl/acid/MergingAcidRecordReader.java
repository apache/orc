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
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link RecordReader} that takes the input from multiple ACID files and merges them,
 * maintaining the ROW__ID sort order.  Note, if you are just reading ACID 2 files, you don't
 * need this.  You only need this if you need the ROW__IDs to come out in order (e.g. if you're
 * going to do updates or deletes on the data, or if you're the compactor).
 *
 * <p>A note on sizing.  This class will spin a new thread for every reader.  Each thread will
 * create a {@link VectorizedRowBatch}.  The entries in the merge queue point to these batches,
 * so it is possible that each thread has more than one batch in memory at a time.  Also, the
 * merger itself will be building a batch.  So, all that to say, make sure you have the memory
 * and the processing power to handle the number of files you combine in this merger.</p>
 *
 * TODO This class is likely completely unnecessary because we don't have to merge inputs for
 * TODO Acid2 since the inserts have new original txn ids each time.  But I'm keeping around until
 * TODO I'm certain that's correct.  Also, it will be useful if someone decides to write a version
 * TODO that compacts ACID1 files.
 * TODO Also, I haven't tested this yet so there's no way it works.
 */
public class MergingAcidRecordReader implements RecordReader {
  private static final Logger LOG = LoggerFactory.getLogger(MergingAcidRecordReader.class);

  private final List<RecordReader> recordReaders;
  private PriorityBlockingQueue<RowOffset> mergeQueue;
  private final TypeDescription schema;
  private int batchesReturned;
  private List<IOException> readerExceptions;
  private AtomicInteger finishedCount;
  private ExecutorService executor;

  /**
   *
   * @param recordReaders these must be a RecordReader returned by {@link AcidReader#rows()}
   * @param schema schema for these records.
   * @throws IOException if one or more of the passed in readers are not
   */
  public MergingAcidRecordReader(List<RecordReader> recordReaders,
                                 TypeDescription schema) throws IOException {
    this.recordReaders = recordReaders;
    this.schema = schema;
    mergeQueue = new PriorityBlockingQueue<>(VectorizedRowBatch.DEFAULT_SIZE);
    readerExceptions = new ArrayList<>();
    finishedCount = new AtomicInteger();
    executor = Executors.newFixedThreadPool(recordReaders.size());
    for (RecordReader recordReader : recordReaders) {
      if (!(recordReader instanceof AcidRecordReader) &&
          recordReader != NullReader.nullRecordReader) {
        throw new IOException("Invalid reader of type " + recordReader.getClass().getName());
      }
      executor.submit(new RecordFetcher(recordReader));
    }
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    try {
      batch.selectedInUse = false;
      int offset;
      for (offset = 0;
           finishedCount.get() < recordReaders.size() && readerExceptions.isEmpty() && offset < batch.size;
           offset++) {
        RowOffset rowOffset = mergeQueue.poll(10, TimeUnit.MILLISECONDS);
        if (rowOffset != null) {
          assert rowOffset.currentBatch.cols.length == batch.cols.length;
          // Copy the 5 ACID columns.  The ACID columns should never be null, so I don't check
          // the isNull flag.
          for (int i = AcidConstants.OPERATION_COL_OFFSET; i < AcidConstants.ROWS_STRUCT_COL_OFFSET; i++) {
            if (rowOffset.currentBatch.cols[i].isRepeating) {
              ((LongColumnVector)batch.cols[i]).vector[offset] =
                  ((LongColumnVector)rowOffset.currentBatch.cols[i]).vector[0];
            } else {
              ((LongColumnVector)batch.cols[i]).vector[offset] =
                  ((LongColumnVector)rowOffset.currentBatch.cols[i]).vector[rowOffset.currentOffset];
            }
          }
          // Now copy the ROWS struct.  This one could be null (if we're merging delete deltas).
          // setElement handles null and repeating, though it doesn't handle isSelected
          batch.cols[AcidConstants.ROWS_STRUCT_COL_OFFSET].setElement(offset,
              rowOffset.currentOffset, rowOffset.currentBatch.cols[AcidConstants.ROWS_STRUCT_COL_OFFSET]);
        }
      }

      if (!readerExceptions.isEmpty()) {
        for (IOException ioe : readerExceptions) {
          LOG.error("Exception from reader", ioe);
        }
        throw readerExceptions.get(0);
      }

      batchesReturned++;
      if (finishedCount.get() >= recordReaders.size()) {
        if (offset == 0) {
          // There's nothing left
          return false;
        }
        batch.size = offset;
      }
      return true;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long getRowNumber() throws IOException {
    return batchesReturned * VectorizedRowBatch.DEFAULT_SIZE;
  }

  /**
   * This method estimates progress based on a combination of the progress of the underlying
   * RecordReaders.  The estimation is not sophisticated and may be wrong.
   * @return estimated progress
   */
  @Override
  public float getProgress() throws IOException {
    float estimatedTotalRows = 0;
    float estimatedReadRows = 0;
    for (RecordReader recordReader : recordReaders) {
      float progress = recordReader.getProgress();
      if (progress > 0) {
        long rowNum = recordReader.getRowNumber();
        estimatedReadRows += rowNum;
        estimatedTotalRows += rowNum / progress;
      }
    }
    return estimatedTotalRows > 0 ? estimatedReadRows / estimatedTotalRows : 0;
  }

  @Override
  public void close() throws IOException {
    try {
      if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        throw new IOException("Failed to stop readers in reasonable time");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interupted halting readers", e);
    }
    for (RecordReader recordReader : recordReaders) recordReader.close();
  }

  /**
   * This method is not supported by this RecordReader.
   */
  @Override
  public void seekToRow(long rowCount) throws IOException {
    throw new UnsupportedOperationException("Can't seek efficiently in a merge stream");

  }

  private class RowOffset implements Comparable<RowOffset> {
    final VectorizedRowBatch currentBatch;
    final int currentOffset; // This takes into account whether selectedInUse is true, no need to
    // check later

    /**
     *
     * @param currentBatch current batch
     * @param currentOffset this should already deal with whether selectedInUse is true
     */
    RowOffset(VectorizedRowBatch currentBatch, int currentOffset) {
      this.currentBatch = currentBatch;
      this.currentOffset = currentOffset;
    }

    @Override
    public int compareTo(RowOffset that) {
      assert this.currentBatch != null;
      assert that.currentBatch != null;

      LongColumnVector origTxnCol = (LongColumnVector)this.currentBatch.cols[AcidConstants.ORIG_TXN_COL_OFFSET];
      long thisOrigTxn = origTxnCol.isRepeating ? origTxnCol.vector[0] : origTxnCol.vector[this.currentOffset];
      origTxnCol = (LongColumnVector)that.currentBatch.cols[AcidConstants.ORIG_TXN_COL_OFFSET];
      long thatOrigTxn = origTxnCol.isRepeating ? origTxnCol.vector[0] : origTxnCol.vector[that.currentOffset];
      if (thisOrigTxn < thatOrigTxn) {
        return -1;
      } else if (thisOrigTxn > thatOrigTxn) {
        return 1;
      } else {
        LongColumnVector bucketCol = (LongColumnVector)this.currentBatch.cols[AcidConstants.BUCKET_COL_OFFSET];
        long thisBucket = bucketCol.isRepeating ? bucketCol.vector[0] : bucketCol.vector[this.currentOffset];
        bucketCol = (LongColumnVector)that.currentBatch.cols[AcidConstants.BUCKET_COL_OFFSET];
        long thatBucket = bucketCol.isRepeating ? bucketCol.vector[0] : bucketCol.vector[that.currentOffset];
        if (thisBucket < thatBucket) {
          return -1;
        } else if (thisBucket > thatBucket) {
          return 1;
        } else {
          LongColumnVector rowIdCol = (LongColumnVector)this.currentBatch.cols[AcidConstants.ROW_ID_COL_OFFSET];
          long thisRowId = rowIdCol.isRepeating ? rowIdCol.vector[0] : rowIdCol.vector[this.currentOffset];
          rowIdCol = (LongColumnVector)that.currentBatch.cols[AcidConstants.ROW_ID_COL_OFFSET];
          long thatRowId = rowIdCol.isRepeating ? rowIdCol.vector[0] : rowIdCol.vector[that.currentOffset];
          if (thisRowId < thatRowId) {
            return -1;
          } else if (thisRowId > thatRowId) {
            return 1;
          } else {
            LongColumnVector currTxnCol = (LongColumnVector)this.currentBatch.cols[AcidConstants.CURRENT_TXN_COL_OFFSET];
            long thisCurrTxn = currTxnCol.isRepeating ? currTxnCol.vector[0] : currTxnCol.vector[this.currentOffset];
            currTxnCol = (LongColumnVector)that.currentBatch.cols[AcidConstants.CURRENT_TXN_COL_OFFSET];
            long thatCurrTxn = currTxnCol.isRepeating ? currTxnCol.vector[0] : currTxnCol.vector[that.currentOffset];
            // Note the reversal this time, as current txnid is sorted descending
            return Long.compare(thatCurrTxn, thisCurrTxn);
          }
        }
      }
    }
  }

  private class RecordFetcher implements Runnable {
    private final RecordReader recordReader;

    RecordFetcher(RecordReader recordReader) {
      this.recordReader = recordReader;
    }

    @Override
    public void run() {
      VectorizedRowBatch batch = schema.createRowBatch();
      try {
        while (recordReader.nextBatch(batch)) {
          for (int i = 0; i < batch.size; i++) {
            int offset = batch.selectedInUse ? batch.selected[i] : i;
            // PriorityBlockingQueues are unbounded, so handle the bounding on our side
            while (mergeQueue.size() >= VectorizedRowBatch.DEFAULT_SIZE * 0.99) {
              Thread.sleep(10);
            }
            mergeQueue.put(new RowOffset(batch, offset));
          }
          batch = schema.createRowBatch();
        }
        finishedCount.incrementAndGet();
      } catch (IOException e) {
        readerExceptions.add(e);
      } catch (InterruptedException e) {
        readerExceptions.add(new IOException(e));
      }
    }
  }
}
