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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderImpl;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

class AcidRecordReader extends RecordReaderImpl {
  private final ValidTxnList validTxns;
  private final ParsedAcidDirectory baseDir;
  private final Configuration conf;
  private final DeleteSet deleteSet;

  // Much of the non-trivial code in this class is taken from Hive's
  // VectorizedOrcAcidRowBatchReader.

  AcidRecordReader(ReaderImpl fileReader, Reader.Options options, ValidTxnList validTxns,
                   ParsedAcidDirectory baseDir, Configuration conf) throws IOException {
    super(fileReader, options);
    this.validTxns = validTxns;
    this.baseDir = baseDir;
    this.conf = conf;
    deleteSet = DeleteSetCache.getCache(conf).getDeleteSet(baseDir);
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    if (!super.nextBatch(batch)) return false;

    assert !batch.selectedInUse;

    BitSet selectedBitSet = new BitSet(batch.size);
    // Start with all rows selected
    selectedBitSet.set(0, batch.size);

    findRecordsWithInvalidTransactionIds(batch, selectedBitSet);

    // If there are any valid records, run the deletes against it
    if (selectedBitSet.cardinality() > 0) {
      deleteSet.applyDeletesToBatch(batch, selectedBitSet);
    }

    if (selectedBitSet.cardinality() != batch.size) {
      // Some records have been selected out, so set up the selected array
      batch.size = selectedBitSet.cardinality();
      batch.selectedInUse = true;
      batch.selected = new int[selectedBitSet.cardinality()];
      for (int setBitIndex = selectedBitSet.nextSetBit(0), selectedItr = 0;
           setBitIndex >= 0;
           setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1), ++selectedItr) {
        batch.selected[selectedItr] = setBitIndex;
      }
    }

    return true;
  }

  private void findRecordsWithInvalidTransactionIds(VectorizedRowBatch batch, BitSet selectedBitSet) {
    if (batch.cols[AcidConstants.ROW_ID_CURRENT_TXN_OFFSET].isRepeating) {
      // When we have repeating values, we can unset the whole bitset at once
      // if the repeating value is not a valid transaction.
      long currentTransactionIdForBatch = ((LongColumnVector)
          batch.cols[AcidConstants.ROW_ID_CURRENT_TXN_OFFSET]).vector[0];
      if (!validTxns.isTxnValid(currentTransactionIdForBatch)) {
        selectedBitSet.clear(0, batch.size);
      }
      return;
    }
    long[] currentTransactionVector =
        ((LongColumnVector) batch.cols[AcidConstants.ROW_ID_CURRENT_TXN_OFFSET]).vector;
    // Loop through the bits that are set to true and mark those rows as false, if their
    // current transactions are not valid.
    for (int setBitIndex = selectedBitSet.nextSetBit(0);
         setBitIndex >= 0;
         setBitIndex = selectedBitSet.nextSetBit(setBitIndex+1)) {
      if (!validTxns.isTxnValid(currentTransactionVector[setBitIndex])) {
        selectedBitSet.clear(setBitIndex);
      }
    }
  }

}
