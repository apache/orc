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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;
import java.util.BitSet;

/**
 * A set of delete events.  Callers should expect that DeleteSets are implemented to take
 * advantage of that fact that insert files are ordered first by original transaction id.  This
 * allows them to be vastly more efficient.
 *
 * This interface assumes that is it will be used for a given {@link ParsedAcidDirectory} parsed
 * with a particular {@link org.apache.hadoop.hive.common.ValidTxnList}.
 */
public interface DeleteSet {

  /**
   * Apply deletes to a batch from an input file
   * @param batch inserts to apply these deletes against
   * @param selectedBitSet bitSet that controls which values are valid.  It is assumed that
   *                       {@link VectorizedRowBatch#selectedInUse} is false and these bits refer
   *                       to direct offsets in the batch.
   * @throws IOException if thrown by underlying reads of the delete deltas.
   */
  void applyDeletesToBatch(VectorizedRowBatch batch, BitSet selectedBitSet) throws IOException;

  DeleteSet nullDeleteSet = new DeleteSet() {
    @Override
    public void applyDeletesToBatch(VectorizedRowBatch batch, BitSet selectedBitSet) throws
        IOException {

    }
  };
}
