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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A view of a directory of ACID files.  This view has been created with a particular
 * {@link org.apache.hadoop.hive.common.ValidTxnList}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ParsedAcidDirectory {
  private final Path baseDir;
  private final ValidTxnList validTxns;
  private final Configuration conf;
  private final List<ParsedAcidFile> inputFiles;
  private final List<ParsedAcidFile> deleteFiles;
  private DeleteSet deleteSet;
  private boolean needsSortedDeleteSet;

  private Map<FileStatus, ParsedAcidFile> inputFileStats;

  ParsedAcidDirectory(Path baseDir, ValidTxnList validTxns, Configuration conf,
                      List<ParsedAcidFile> inputFiles, List<ParsedAcidFile> deleteFiles) {
    this.baseDir = baseDir;
    this.validTxns = validTxns;
    this.inputFiles = inputFiles;
    this.deleteFiles = deleteFiles;
    this.conf = conf;
    needsSortedDeleteSet = false;
  }

  /**
   * Get a list of all files that contain records to read.  This may include
   * base, delta, and pre-acid files.  Delete deltas are not included in this list.
   * @return list of files to read.
   */
  public List<ParsedAcidFile> getInputFiles() {
    return inputFiles;
  }

  /**
   * Get a list of all delete files.
   * @return delete deltas.
   */
  public List<ParsedAcidFile> getDeleteFiles() {
    return deleteFiles;
  }

  public Path getBaseDir() {
    return baseDir;
  }

  public ValidTxnList getValidTxns() {
    return validTxns;
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public int hashCode() {
    // Note on this and equals(), I don't think ValidReadTxnList implements equals or hashCode
    // which means I'll be doing object equality.  But I think that's ok.
    return baseDir.hashCode() * 31 + validTxns.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ParsedAcidDirectory) {
      ParsedAcidDirectory that = (ParsedAcidDirectory)obj;
      return baseDir.equals(that.baseDir) && validTxns.equals(that.validTxns);
    }
    return false;
  }

  /**
   * Determines whether this file is one that should be read.  This is necessary because callers
   * who are generating splits may not understand which files are and aren't valid, and thus may
   * pass in files from the directory that are out of transaction range or otherwise redundant.
   * @param file file to check
   * @return whether this file should be read.
   */
  boolean shouldBeUsedForInput(FileStatus file) {
    fillOutInputFileStats();
    return inputFileStats.containsKey(file);
  }

  /**
   * Given an inputFile, find all of the delete deltas that should be applied to this input file.
   * @param stat file with inserts to read
   * @return list of delete files that should be read for this file
   */
  List<ParsedAcidFile> getRelevantDeletes(FileStatus stat) {
    if (deleteFiles == null || deleteFiles.isEmpty()) return Collections.emptyList();

    if (!shouldBeUsedForInput(stat)) return Collections.emptyList();
    // The preceding if should guarantee that the following get never returns null
    ParsedAcidFile inputFile = inputFileStats.get(stat);
    assert inputFile != null;
    // All deltas always apply to base and preAcid files.  For preAcid this is true because these
    // are essentially base files with maxTxn = MAX_LONG.  For Base this is true because the
    // directory parser will have removed any delta files that don't apply to the base.
    if (inputFile.isPreAcid() || inputFile.isBase()) {
      return deleteFiles;
    }

    List<ParsedAcidFile> relevantDirectories = new ArrayList<>();

    // We do not have to worry about delete deltas that are outside our transaction range.  The
    // parsing has already removed these.

    for (ParsedAcidFile deleteFile : deleteFiles) {
      // There's no way deletes that happened before an insert can apply.
      if (inputFile.isDelta() && deleteFile.getMaxTransaction() < inputFile.getMinTransaction()) {
        continue;
      }

      relevantDirectories.add(deleteFile);
    }
    return relevantDirectories;
  }

  /**
   * Get the delete set for this directory.  If there is room to build it in memory that will be
   * done and the result cached so that other readers of this directory can share it.  If there
   * is not room to do it in memory a DeleteSet will be returned that does a merge on the deletes
   * and applies them to the input.  This type cannot be shared across threads and thus will not
   * be stored for other readers.
   * @return a DeleteSet
   */
  synchronized DeleteSet getDeleteSet() throws IOException {
    if (deleteSet != null) return deleteSet;

    if (deleteFiles == null || deleteFiles.isEmpty()) return DeleteSet.nullDeleteSet;

    // Figure out if I can fit this in memory or not.  To do this, we have to first get a size
    // estimate.
    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf)
        .acidDir(this)
        .validTxnList(validTxns)
        .filesystem(deleteFiles.get(0).getFileStatus().getPath().getFileSystem(conf));

    long size = 0;
    List<Reader> readers = new ArrayList<>();
    for (final ParsedAcidFile deleteDelta : deleteFiles) {
      Reader reader = new AcidReader(deleteDelta.getFileStatus().getPath(), options);
      if (!needsSortedDeleteSet) size += reader.getNumberOfRows();
      readers.add(reader);
    }

    if (needsSortedDeleteSet || size > OrcConf.MAX_DELETE_ENTRIES_IN_MEMORY.getLong(conf)) {
      // Remember this for later, so we don't keep checking the size every time someone reads
      // this directory.
      needsSortedDeleteSet = true;
      return new SortedDeleteSet(readers);
    }

    deleteSet = new InMemoryDeleteSet(readers);
    return deleteSet;
  }

  private void fillOutInputFileStats() {
    if (inputFileStats == null) {
      synchronized (this) {
        if (inputFileStats == null) {
          inputFileStats = new HashMap<>(inputFiles.size());
          for (ParsedAcidFile inputFile : inputFiles) {
            inputFileStats.put(inputFile.getFileStatus(), inputFile);
          }
        }
      }
    }
  }

}
