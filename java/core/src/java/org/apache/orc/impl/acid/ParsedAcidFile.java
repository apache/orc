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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ParsedAcidFile implements Comparable<ParsedAcidFile> {
  private final long minTransaction;
  private final long maxTransaction;
  private final FileStatus fileStatus;
  //-1 is for internal (getAcidState()) purposes and means the delta dir
  //had no statement ID
  private final int statementId;
  private final boolean isDeleteDelta; // records whether delta dir is of type 'delete_delta_x_y...'

  /**
   * Build a ParsedAcidFile from a preAcid (fka original) file.  Min transaction will be set to 0
   * and max to {@link Long#MAX_VALUE}.
   * @param preAcid file
   * @return ParsedAcidFile
   */
  static ParsedAcidFile fromPreAcidFile(FileStatus preAcid) {
    return new ParsedAcidFile(0, Long.MAX_VALUE, preAcid, -1, false);
  }

  /**
   * Build a ParsedAcidFile from a base file.  Min transaction will be set to 0 and max to the
   * txn value passed in.
   * @param baseFile base file
   * @param txn highest txn associated with this file.  This will be the value returned by
   *            {@link #getMaxTransaction()}.
   * @return ParsedAcidFile
   */
  static ParsedAcidFile fromBaseFile(FileStatus baseFile, long txn) {
    return new ParsedAcidFile(0, txn, baseFile, -1, false);
  }

  /**
   * Build a ParsedAcidFile from a delta file.  Statement id will be set to -1, indicating this
   * does not have a statement id.
   * @param minTxn minimum transaction contained in this delta
   * @param maxTxn maximum transaction contained in this delta
   * @param deltaFile delta file
   * @param isDeleteDelta whether this is a delete delta
   * @return ParsedAcidFile
   */
  static ParsedAcidFile fromDeltaFile(long minTxn, long maxTxn, FileStatus deltaFile,
                                      boolean isDeleteDelta) {
    return new ParsedAcidFile(minTxn, maxTxn, deltaFile, -1, isDeleteDelta);
  }

  /**
   * Build a ParsedAcidFile from a delta file.
   * @param minTxn minimum transaction contained in this delta
   * @param maxTxn maximum transaction contained in this delta
   * @param deltaFile delta file
   * @param statementId statement id for this delta.
   * @param isDeleteDelta whether this is a delete delta
   * @return ParsedAcidFile
   */
  static ParsedAcidFile fromDeltaFile(long minTxn, long maxTxn, FileStatus deltaFile,
                                      int statementId, boolean isDeleteDelta) {
    return new ParsedAcidFile(minTxn, maxTxn, deltaFile, statementId, isDeleteDelta);
  }

  /**
   * Build a ParsedAcidFile from its parent directory.  This is useful when finding all the
   * buckets in an acid directory.
   * @param dir parent directory
   * @param file bucket file
   * @return new ParsedAcidFile with the same transaction and statement id information as its
   * parent.
   */
  static ParsedAcidFile fromDirectory(ParsedAcidFile dir, FileStatus file) {
    return new ParsedAcidFile(dir.minTransaction, dir.maxTransaction, file, dir.statementId,
        dir.isDeleteDelta);
  }


  private ParsedAcidFile(long min, long max, FileStatus fileStatus, int statementId, boolean isDeleteDelta) {
    this.minTransaction = min;
    this.maxTransaction = max;
    this.fileStatus = fileStatus;
    this.statementId = statementId;
    this.isDeleteDelta = isDeleteDelta;
  }

  public long getMinTransaction() {
    return minTransaction;
  }

  public long getMaxTransaction() {
    return maxTransaction;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public int getStatementId() {
    return statementId;
  }

  public boolean isDeleteDelta() {
    return isDeleteDelta;
  }

  public boolean isBase() {
    return minTransaction == 0 && maxTransaction < Long.MAX_VALUE;
  }

  public boolean isDelta() {
    return minTransaction > 0;
  }

  public boolean isPreAcid() {
    return minTransaction == 0 && maxTransaction == Long.MAX_VALUE;
  }

  /**
   * Compactions (Major/Minor) merge deltas/bases but delete of old files
   * happens in a different process; thus it's possible to have bases/deltas with
   * overlapping txnId boundaries.  The sort order helps figure out the "best" set of files
   * to use to get data.
   * This sorts "wider" delta before "narrower" i.e. delta_5_20 sorts before delta_5_10 (and delta_11_20)
   */
  @Override
  public int compareTo(ParsedAcidFile other) {
    if (minTransaction != other.minTransaction) {
      if (minTransaction < other.minTransaction) {
        return -1;
      } else {
        return 1;
      }
    } else if (maxTransaction != other.maxTransaction) {
      if (maxTransaction < other.maxTransaction) {
        return 1;
      } else {
        return -1;
      }
    }
    else if(statementId != other.statementId) {
      /*
       * We want deltas after minor compaction (w/o statementId) to sort
       * earlier so that getAcidState() considers compacted files (into larger ones) obsolete
       * Before compaction, include deltas with all statementIds for a given txnId
       * in a {@link org.apache.hadoop.hive.ql.io.AcidUtils.Directory}
       */
      if(statementId < other.statementId) {
        return -1;
      }
      else {
        return 1;
      }
    }
    else {
      return fileStatus.compareTo(other.fileStatus);
    }
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ParsedAcidFile && compareTo((ParsedAcidFile) other) == 0;
  }

  @Override
  public int hashCode() {
    return fileStatus.hashCode();
  }
}
