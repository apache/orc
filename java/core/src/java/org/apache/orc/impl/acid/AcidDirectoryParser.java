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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.orc.impl.acid.AcidConstants.BASE_PREFIX;
import static org.apache.orc.impl.acid.AcidConstants.DELETE_DELTA_PREFIX;
import static org.apache.orc.impl.acid.AcidConstants.DELTA_PREFIX;

/**
 * A utility class to parse ACID directories.
 */
public class AcidDirectoryParser {
  private static final Logger LOG = LoggerFactory.getLogger(AcidDirectoryParser.class);

  private static final PathFilter hiddenFileFilter = new PathFilter(){
    @Override
    public boolean accept(Path p){
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  /**
   * Parse an ACID directory based on a particular transaction list.
   * @param directory Directory to parse.
   * @param conf Configuration used to interact with the file system.
   * @param txnList valid transaction list.
   * @return parsed acid directory
   * @throws IOException if thrown by underlying file operations or there is something wrong in
   * the directory layout.
   */
  public static AcidVersionedDirectory parseDirectory(Path directory,
                                                      Configuration conf,
                                                      ValidTxnList txnList) throws IOException {
    final List<FileStatus> inputs = new ArrayList<>();
    final List<FileStatus> deletes = new ArrayList<>();
    final List<FileStatus> original = new ArrayList<>();

    List<ParsedDelta> insertDeltas = new ArrayList<>();
    List<ParsedDelta> deleteDeltas = new ArrayList<>();
    List<FileStatus> originalDirectories = new ArrayList<>();

    FileSystem fs = directory.getFileSystem(conf);
    TxnBase bestBase = new TxnBase();
    List<FileStatus> children = listLocatedStatus(fs, directory, hiddenFileFilter);
    for (FileStatus child : children) {
      getChildState(child, txnList, insertDeltas, deleteDeltas, originalDirectories, original,
          bestBase);
    }

    if (bestBase.status != null) inputs.add(bestBase.status);

    // If we didn't find a base but we found original directories, we need to use those originals.
    if (bestBase.status == null && originalDirectories.size() > 0) {
      for (FileStatus origDir : originalDirectories) {
        findOriginals(fs, origDir, original);
      }
      // We need to consistently sort the originals so that every reader thinks of them in the
      // same order when assigning delta ids.
      Collections.sort(original);
    }

    // Decide on our deltas
    pickBestDeltas(insertDeltas, inputs, bestBase, txnList);
    pickBestDeltas(deleteDeltas, deletes, bestBase, txnList);


    return new AcidVersionedDirectory() {
      @Override
      public List<FileStatus> getInputFiles() {
        return inputs;
      }

      @Override
      public List<FileStatus> getDeleteFiles() {
        return deletes;
      }

      @Override
      public List<FileStatus> getPreAcidFiles() {
        return original;
      }
    };
  }

  /*
  static AcidVersionedDirectory parse(Path directory, Configuration conf, ValidTxnList txnList)
      throws IOException {


    if(bestBase.oldestBase != null && bestBase.status == null) {
      /*
       * If here, it means there was a base_x (> 1 perhaps) but none were suitable for given
       * {@link txnList}.  Note that 'original' files are logically a base_Long.MIN_VALUE and thus
       * cannot have any data for an open txn.  We could check {@link deltas} has files to cover
       * [1,n] w/o gaps but this would almost never happen...*/
  /*
      long[] exceptions = txnList.getInvalidTransactions();
      String minOpenTxn = exceptions != null && exceptions.length > 0 ?
          Long.toString(exceptions[0]) : "x";
      throw new IOException("Not enough history available for (" + txnList.getHighWatermark() +
        "," + minOpenTxn + ").  Oldest available base:  " + bestBase.oldestBase);
    }

    final Path base = bestBase.status == null ? null : bestBase.status.getPath();
    LOG.debug("in directory " + directory.toUri().toString() + " base = " + base + " deltas = " +
        deltas.size());
    /*
     * If this sort order is changed and there are tables that have been converted to transactional
     * and have had any update/delete/merge operations performed but not yet MAJOR compacted, it
     * may result in data loss since it may change how
     * {@link org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger.OriginalReaderPair} assigns
     * {@link RecordIdentifier#rowId} for read (that have happened) and compaction (yet to happen).
     */
  /*Collections.sort(original, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        return o1.compareTo(o2);
      }
    });

    return new AcidVersionedDirectory(){

      @Override
      public Path getBaseDirectory() {
        return base;
      }

      @Override
      public List<FileStatus> getOriginalFiles() {
        return original;
      }

      @Override
      public List<ParsedDelta> getCurrentDirectories() {
        return deltas;
      }

      @Override
      public List<FileStatus> getObsolete() {
        return obsolete;
      }
    };
  }
  */

  private static List<FileStatus> listLocatedStatus(final FileSystem fs,
                                                    final Path path,
                                                    final PathFilter filter) throws IOException {
    FileStatus[] statusArray = fs.listStatus(path);
    List<FileStatus> result = new ArrayList<>();
    for (FileStatus status : statusArray) {
      if (filter == null || filter.accept(status.getPath())) {
        result.add(status);
      }
    }
    return result;
  }

  /**
   * Figure out the status of a child directory.
   * @param child directory we are investigating
   * @param txnList our valid txns
   * @param insertDeltas set of valid insert deltas we have found so far
   * @param deleteDeltas set of valid delete deltas we have found so far
   * @param originalDirectories set of original directories we have found so far
   * @param original set of original files we have found so far
   * @param bestBase best base file we have found so far
   * @throws IOException if the underlying fs calls throw it
   */
  private static void getChildState(FileStatus child,
                                    ValidTxnList txnList,
                                    List<ParsedDelta> insertDeltas,
                                    List<ParsedDelta> deleteDeltas,
                                    List<FileStatus> originalDirectories,
                                    List<FileStatus> original,
                                    TxnBase bestBase) throws IOException {
    Path p = child.getPath();
    String fn = p.getName();
    if (fn.startsWith(BASE_PREFIX) && child.isDirectory()) {
      long txn = parseBase(fn);
      if (bestBase.status == null) {
        if (isValidBase(txn, txnList)) {
          bestBase.status = child;
          bestBase.txn = txn;
        }
      } else if (bestBase.txn < txn) {
        if (isValidBase(txn, txnList)) {
          // Use this one instead of the earlier base
          bestBase.status = child;
          bestBase.txn = txn;
        }
      }
      // Ignore this one as it's not as good as what we already have
    } else if (fn.startsWith(DELTA_PREFIX) && child.isDirectory()) {
      ParsedDelta delta = parseDelta(child, false);
      if (txnList.isTxnRangeValid(delta.getMinTransaction(), delta.getMaxTransaction()) !=
          ValidTxnList.RangeResponse.NONE) {
        insertDeltas.add(delta);
      }
    } else if (fn.startsWith(DELETE_DELTA_PREFIX) && child.isDirectory()) {
      ParsedDelta delta = parseDelta(child, true);
      if (txnList.isTxnRangeValid(delta.getMinTransaction(), delta.getMaxTransaction()) !=
          ValidTxnList.RangeResponse.NONE) {
        deleteDeltas.add(delta);
      }
    } else if (child.isDirectory()) {
      // This is just the directory.  We need to recurse and find the actual files.  But don't
      // do this until we have determined there is no base.  This saves time.  Plus,
      // it is possible that the cleaner is running and removing these original files,
      // in which case recursing through them could cause us to get an error.
      originalDirectories.add(child);
    } else if (child.getLen() != 0){
      original.add(child);
    }
  }

  /**
   * Find the original files (non-ACID layout) recursively under the partition directory.
   * @param fs the file system
   * @param stat the directory to add
   * @param original the list of original files
   */
  private static void findOriginals(FileSystem fs,
                                    FileStatus stat,
                                    List<FileStatus> original) throws IOException {
    assert stat.isDirectory();
    List<FileStatus> children = listLocatedStatus(fs, stat.getPath(), hiddenFileFilter);
    for (FileStatus child : children) {
      if (child.isDirectory()) {
        findOriginals(fs, child, original);
      } else {
        if(child.getLen() > 0) {
          original.add(child);
        }
      }
    }
  }

  /**
   * Get the transaction id from a base directory name.
   * @param filename of the base
   * @return the maximum transaction id that is included
   */
  private static long parseBase(String filename) {
    return Long.parseLong(filename.substring(BASE_PREFIX.length()));
  }

  /*
   * We can only use a 'base' if it doesn't have an open txn (from specific reader's point of view)
   * A 'base' with open txn in its range doesn't have 'enough history' info to produce a correct
   * snapshot for this reader.
   * Note that such base is NOT obsolete.  Obsolete files are those that are "covered" by other
   * files within the snapshot.
   */
  private static boolean isValidBase(long baseTxnId, ValidTxnList txnList) {
    //such base is created by 1st compaction in case of non-acid to acid table conversion
    //By definition there are no open txns with id < 1.
    return baseTxnId == Long.MIN_VALUE || txnList.isValidBase(baseTxnId);
  }

  private static ParsedDelta parseDelta(FileStatus fs, boolean isDeleteDelta) {
      String rest = fs.getPath().getName().substring(isDeleteDelta ? DELETE_DELTA_PREFIX.length() :
          DELTA_PREFIX.length());
      int split = rest.indexOf('_');
      int split2 = rest.indexOf('_', split + 1);//may be -1 if no statementId
      long min = Long.parseLong(rest.substring(0, split));
      long max = split2 == -1 ?
          Long.parseLong(rest.substring(split + 1)) :
          Long.parseLong(rest.substring(split + 1, split2));
      if (split2 == -1) {
        return new ParsedDelta(min, max, fs, isDeleteDelta);
      }
      int statementId = Integer.parseInt(rest.substring(split2 + 1));
      return new ParsedDelta(min, max, fs, statementId, isDeleteDelta);
  }

  /**
   * Given the list of all valid deltas, pick the "best" ones.  Best is defined as the smallest
   * set of files that can satisfy the requirements.  This means that compacted deltas with
   * multiple transactions will be picked over single transaction deltas.
   * @param candidates list of all valid deltas for this transaction
   * @param finalists select list of best deltas
   * @param bestBase the base file being used with these deltas
   * @param txnList the valid transaction list
   */
  private static void pickBestDeltas(List<ParsedDelta> candidates,
                                     List<FileStatus> finalists,
                                     TxnBase bestBase,
                                     ValidTxnList txnList) {
    Collections.sort(candidates);
    //so now, 'working' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
    //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
    //subject to list of 'exceptions' in 'txnList' (not show in above example).
    long current = bestBase.txn;
    int lastStmtId = -1;
    ParsedDelta prev = null;
    for(ParsedDelta next: candidates) {
      if (next.getMaxTransaction() > current) {
        // are any of the new transactions ones that we care about?
        if (txnList.isTxnRangeValid(current+1, next.getMaxTransaction()) !=
            ValidTxnList.RangeResponse.NONE) {
          finalists.add(next.getFileStatus());
          current = next.getMaxTransaction();
          lastStmtId = next.getStatementId();
          prev = next;
        }
      }
      else if(next.getMaxTransaction() == current && lastStmtId >= 0) {
        //make sure to get all deltas within a single transaction;  multi-statement txn
        //generate multiple delta files with the same txnId range
        //of course, if maxTransaction has already been minor compacted, all per statement deltas are obsolete
        finalists.add(next.getFileStatus());
        prev = next;
      }
      else if (prev != null && next.getMaxTransaction() == prev.getMaxTransaction()
          && next.getMinTransaction() == prev.getMinTransaction()
          && next.getStatementId() == prev.getStatementId()) {
        // The 'next' parsedDelta may have everything equal to the 'prev' parsedDelta, except
        // the path. This may happen when we have split update and we have two types of delta
        // directories- 'delta_x_y' and 'delete_delta_x_y' for the SAME txn range.

        // Also note that any delete_deltas in between a given delta_x_y range would be made
        // obsolete. For example, a delta_30_50 would make delete_delta_40_40 obsolete.
        // This is valid because minor compaction always compacts the normal deltas and the delete
        // deltas for the same range. That is, if we had 3 directories, delta_30_30,
        // delete_delta_40_40 and delta_50_50, then running minor compaction would produce
        // delta_30_50 and delete_delta_30_50.

        finalists.add(next.getFileStatus());
        prev = next;
      }
      // If it doesn't match any of these, just drop it on the floor.
    }

  }

  private static class TxnBase {
    private FileStatus status;
    private long txn = 0;
  }
}
