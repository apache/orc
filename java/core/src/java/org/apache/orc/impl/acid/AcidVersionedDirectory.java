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
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * A view of a directory of ACID files.  This view has been created with a particular
 * {@link org.apache.hadoop.hive.common.ValidTxnList}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface AcidVersionedDirectory {
  /**
   * Get a list of all files that contain records to read.  This may include
   * base and delta files.  Delete deltas are not included in this list.
   * @return list of files to read.
   */
  List<FileStatus> getInputFiles();

  /**
   * Get a list of all delete files.
   * @return delete deltas.
   */
  List<FileStatus> getDeleteFiles();

  /**
   * Get a list of all files that are pre-ACID (e.g. they do not contain the ROW__ID column).
   * Readers will need to handle these differently.
   * @return list of all original files.
   */
  List<FileStatus> getPreAcidFiles();
}
