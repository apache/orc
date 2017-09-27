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
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.orc.impl.acid.AcidConstants.BASE_PREFIX;
import static org.apache.orc.impl.acid.AcidConstants.DELETE_DELTA_PREFIX;
import static org.apache.orc.impl.acid.AcidConstants.DELTA_PREFIX;

/**
 * A utility class to parse ACID directories.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
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
    final List<ParsedAcidFile> inputs = new ArrayList<>();
    final List<ParsedAcidFile> deletes = new ArrayList<>();
    final List<ParsedAcidFile> original = new ArrayList<>();

    List<ParsedAcidFile> deltas = new ArrayList<>();
    List<FileStatus> originalDirectories = new ArrayList<>();

    FileSystem fs = directory.getFileSystem(conf);
    AtomicReference<ParsedAcidFile> bestBase = new AtomicReference<>();
    List<FileStatus> children = listLocatedStatus(fs, directory, hiddenFileFilter);
    for (FileStatus child : children) {
      getChildState(child, txnList, deltas, originalDirectories, original, bestBase);
    }

    if (bestBase.get() != null) inputs.add(bestBase.get());

    // If we didn't find a base but we found original directories, we need to use those originals.
    if (bestBase.get() == null && originalDirectories.size() > 0) {
      for (FileStatus origDir : originalDirectories) {
        findOriginals(fs, origDir, original);
      }
      // We need to consistently sort the originals so that every reader thinks of them in the
      // same order when assigning delta ids.
      Collections.sort(original);
    }

    // Decide on our deltas
    pickBestDeltas(deltas, inputs, deletes, bestBase.get(), txnList);


    return new AcidVersionedDirectory() {
      @Override
      public List<ParsedAcidFile> getInputFiles() {
        return inputs;
      }

      @Override
      public List<ParsedAcidFile> getDeleteFiles() {
        return deletes;
      }

      @Override
      public List<ParsedAcidFile> getPreAcidFiles() {
        return original;
      }
    };
  }

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
   * @param deltas set of valid deltas we have found so far
   * @param originalDirectories set of original directories we have found so far
   * @param original set of original files we have found so far
   * @param bestBase reference to best base file we have found so far.  The referenced value can
   *                 be null until the bestBase is found.  It can also be null once all parsing
   *                 is done if no base was found.
   * @throws IOException if the underlying fs calls throw it
   */
  private static void getChildState(FileStatus child,
                                    ValidTxnList txnList,
                                    List<ParsedAcidFile> deltas,
                                    List<FileStatus> originalDirectories,
                                    List<ParsedAcidFile> original,
                                    AtomicReference<ParsedAcidFile> bestBase) throws IOException {
    Path p = child.getPath();
    String fn = p.getName();
    if (fn.startsWith(BASE_PREFIX) && child.isDirectory()) {
      long txn = parseBase(fn);
      if (bestBase.get() == null) {
        if (isValidBase(txn, txnList)) {
          bestBase.set(ParsedAcidFile.fromBaseFile(child, txn));
        }
      } else if (bestBase.get().getMaxTransaction() < txn) {
        if (isValidBase(txn, txnList)) {
          // Use this one instead of the earlier base
          bestBase.set(ParsedAcidFile.fromBaseFile(child, txn));
        }
      }
      // Ignore this one as it's not as good as what we already have
    } else if (fn.startsWith(DELTA_PREFIX) && child.isDirectory()) {
      ParsedAcidFile delta = parseDelta(child, false);
      if (txnList.isTxnRangeValid(delta.getMinTransaction(), delta.getMaxTransaction()) !=
          ValidTxnList.RangeResponse.NONE) {
        deltas.add(delta);
      }
    } else if (fn.startsWith(DELETE_DELTA_PREFIX) && child.isDirectory()) {
      ParsedAcidFile delta = parseDelta(child, true);
      if (txnList.isTxnRangeValid(delta.getMinTransaction(), delta.getMaxTransaction()) !=
          ValidTxnList.RangeResponse.NONE) {
        deltas.add(delta);
      }
    } else if (child.isDirectory()) {
      // This is just the directory.  We need to recurse and find the actual files.  But don't
      // do this until we have determined there is no base.  This saves time.  Plus,
      // it is possible that the cleaner is running and removing these original files,
      // in which case recursing through them could cause us to get an error.
      originalDirectories.add(child);
    } else if (child.getLen() != 0){
      original.add(ParsedAcidFile.fromPreAcidFile(child));
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
                                    List<ParsedAcidFile> original) throws IOException {
    assert stat.isDirectory();
    List<FileStatus> children = listLocatedStatus(fs, stat.getPath(), hiddenFileFilter);
    for (FileStatus child : children) {
      if (child.isDirectory()) {
        findOriginals(fs, child, original);
      } else {
        if(child.getLen() > 0) {
          original.add(ParsedAcidFile.fromPreAcidFile(child));
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

  private static ParsedAcidFile parseDelta(FileStatus fs, boolean isDeleteDelta) {
      String rest = fs.getPath().getName().substring(isDeleteDelta ? DELETE_DELTA_PREFIX.length() :
          DELTA_PREFIX.length());
      int split = rest.indexOf('_');
      int split2 = rest.indexOf('_', split + 1);//may be -1 if no statementId
      long min = Long.parseLong(rest.substring(0, split));
      long max = split2 == -1 ?
          Long.parseLong(rest.substring(split + 1)) :
          Long.parseLong(rest.substring(split + 1, split2));
      if (split2 == -1) {
        return ParsedAcidFile.fromDeltaFile(min, max, fs, isDeleteDelta);
      }
      int statementId = Integer.parseInt(rest.substring(split2 + 1));
      return ParsedAcidFile.fromDeltaFile(min, max, fs, statementId, isDeleteDelta);
  }

  /**
   * Given the list of all valid deltas, pick the "best" ones.  Best is defined as the smallest
   * set of files that can satisfy the requirements.  This means that compacted deltas with
   * multiple transactions will be picked over single transaction deltas.
   * @param candidates list of all valid deltas for this transaction
   * @param inputs select list of best insert deltas
   * @param deletes select list of best delete deltas
   * @param bestBase the base file being used with these deltas
   * @param txnList the valid transaction list
   */
  private static void pickBestDeltas(List<ParsedAcidFile> candidates,
                                     List<ParsedAcidFile> inputs,
                                     List<ParsedAcidFile> deletes,
                                     ParsedAcidFile bestBase,
                                     ValidTxnList txnList) {
    Collections.sort(candidates);
    //so now, 'working' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
    //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
    //subject to list of 'exceptions' in 'txnList' (not show in above example).
    long current = bestBase == null ? 0 : bestBase.getMaxTransaction();
    int lastStmtId = -1;
    ParsedAcidFile prev = null;
    for (ParsedAcidFile next: candidates) {
      List<ParsedAcidFile> finalists = next.isDeleteDelta() ? deletes : inputs;
      if (next.getMaxTransaction() > current) {
        // are any of the new transactions ones that we care about?
        if (txnList.isTxnRangeValid(current+1, next.getMaxTransaction()) !=
            ValidTxnList.RangeResponse.NONE) {
          finalists.add(next);
          current = next.getMaxTransaction();
          lastStmtId = next.getStatementId();
          prev = next;
        }
      }
      else if (next.getMaxTransaction() == current && lastStmtId >= 0) {
        //make sure to get all deltas within a single transaction;  multi-statement txn
        //generate multiple delta files with the same txnId range
        //of course, if maxTransaction has already been minor compacted, all per statement deltas are obsolete
        finalists.add(next);
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

        finalists.add(next);
        prev = next;
      }
      // If it doesn't match any of these, just drop it on the floor.
    }

  }
}
