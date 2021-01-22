/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.shims;

import java.io.IOException;


/**
 * A writable stream that tracks the current position in the file.
 */
public interface FileIO {

  /**
   * Does the given file exist?
   * @param name the filename
   * @return true if it exists
   */
  boolean exists(String name) throws IOException;

  /**
   * Get the file's (or directory's) metadata.
   * @param name the pathname of the file
   * @return the current state of the file
   * @throws IOException if the file does not exist.
   */
  FileStatus getFileStatus(String name) throws IOException;

  /**
   * Open an input file
   * @param name the filename to open
   * @return an input stream
   * @throws IOException
   */
  SeekableInputStream createInputFile(String name) throws IOException;

  /**
   * Create an output file.
   * @param name the filename to use
   * @param overwrite if the file currently exists, should it be overwritten
   * @param blockSize the block size in bytes to use for the file
   * @return An output stream
   * @throws IOException
   */
  PositionOutputStream createOutputFile(String name,
                                        boolean overwrite,
                                        long blockSize) throws IOException;

  /**
   * Delete a file.
   * @param name the filename to delete
   * @throws IOException
   */
  void delete(String name) throws IOException;
}
