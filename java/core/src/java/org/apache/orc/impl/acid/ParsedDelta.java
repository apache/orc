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

import org.apache.hadoop.fs.FileStatus;

class ParsedDelta implements Comparable<ParsedDelta> {
  private final long minTransaction;
  private final long maxTransaction;
  private final FileStatus fileStatus;
  //-1 is for internal (getAcidState()) purposes and means the delta dir
  //had no statement ID
  private final int statementId;
  private final boolean isDeleteDelta; // records whether delta dir is of type 'delete_delta_x_y...'

  /**
   * for pre 1.3.x delta files
   */
  ParsedDelta(long min, long max, FileStatus fileStatus, boolean isDeleteDelta) {
    this(min, max, fileStatus, -1, isDeleteDelta);
  }

  ParsedDelta(long min, long max, FileStatus fileStatus, int statementId, boolean isDeleteDelta) {
    this.minTransaction = min;
    this.maxTransaction = max;
    this.fileStatus = fileStatus;
    this.statementId = statementId;
    this.isDeleteDelta = isDeleteDelta;
  }

  long getMinTransaction() {
    return minTransaction;
  }

  long getMaxTransaction() {
    return maxTransaction;
  }

  FileStatus getFileStatus() {
    return fileStatus;
  }

  int getStatementId() {
    return statementId;
  }

  boolean isDeleteDelta() {
    return isDeleteDelta;
  }

  /**
   * Compactions (Major/Minor) merge deltas/bases but delete of old files
   * happens in a different process; thus it's possible to have bases/deltas with
   * overlapping txnId boundaries.  The sort order helps figure out the "best" set of files
   * to use to get data.
   * This sorts "wider" delta before "narrower" i.e. delta_5_20 sorts before delta_5_10 (and delta_11_20)
   */
  @Override
  public int compareTo(ParsedDelta parsedDelta) {
    if (minTransaction != parsedDelta.minTransaction) {
      if (minTransaction < parsedDelta.minTransaction) {
        return -1;
      } else {
        return 1;
      }
    } else if (maxTransaction != parsedDelta.maxTransaction) {
      if (maxTransaction < parsedDelta.maxTransaction) {
        return 1;
      } else {
        return -1;
      }
    }
    else if(statementId != parsedDelta.statementId) {
      /*
       * We want deltas after minor compaction (w/o statementId) to sort
       * earlier so that getAcidState() considers compacted files (into larger ones) obsolete
       * Before compaction, include deltas with all statementIds for a given txnId
       * in a {@link org.apache.hadoop.hive.ql.io.AcidUtils.Directory}
       */
      if(statementId < parsedDelta.statementId) {
        return -1;
      }
      else {
        return 1;
      }
    }
    else {
      return fileStatus.compareTo(parsedDelta.fileStatus);
    }
  }

  // The below overrides are just to keep findbugs happy, I don't think they're useful.
  @Override
  public boolean equals(Object other) {
    return other instanceof ParsedDelta && compareTo((ParsedDelta) other) == 0;
  }

  @Override
  public int hashCode() {
    return fileStatus.hashCode();
  }
}
