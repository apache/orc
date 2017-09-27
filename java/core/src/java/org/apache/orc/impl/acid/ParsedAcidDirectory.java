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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A view of a directory of ACID files.  This view has been created with a particular
 * {@link org.apache.hadoop.hive.common.ValidTxnList}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ParsedAcidDirectory {
  private final List<ParsedAcidFile> inputFiles;
  private final List<ParsedAcidFile> deleteFiles;

  private Set<FileStatus> inputFileStats;

  ParsedAcidDirectory(List<ParsedAcidFile> inputFiles, List<ParsedAcidFile> deleteFiles) {
    this.inputFiles = inputFiles;
    this.deleteFiles = deleteFiles;
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

  /**
   * Determines whether this file is one that should be read.  This is necessary because callers
   * who are generating splits may not understand which files are and aren't valid, and thus may
   * pass in files from the directory that are out of transaction range or otherwise redundant.
   * @param file file to check
   * @return whether this file should be read.
   */
  public boolean shouldBeUsedForInput(FileStatus file) {
    if (inputFileStats == null) {
      inputFileStats = new HashSet<>(inputFiles.size());
      for (ParsedAcidFile inputFile : inputFiles) inputFileStats.add(inputFile.getFileStatus());
    }
    return inputFileStats.contains(file);
  }

  /**
   * Given an inputFile, find all of the delete deltas that should be applied to this input file.
   * @param inputFile file with inserts to read
   * @return list of delete files that should be read for this file
   */
  public List<ParsedAcidFile> getRelevantDeletes(ParsedAcidFile inputFile) {
    if (deleteFiles == null || deleteFiles.isEmpty()) return Collections.emptyList();

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

}
