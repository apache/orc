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

package org.apache.orc;

import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 * The backwards compatible version of MemoryManagerCore.
 */
public interface MemoryManager extends org.apache.orc.core.MemoryManager {

  /**
   * Add a new writer's memory allocation to the pool. We use the path
   * as a unique key to ensure that we don't get duplicates.
   * @param path the file that is being written
   * @param requestedAllocation the requested buffer size
   */
  void addWriter(Path path, long requestedAllocation,
                 Callback callback) throws IOException;

  /**
   * Remove the given writer from the pool.
   * @param path the file that has been closed
   */
  void removeWriter(Path path) throws IOException;

  default void addWriter(String path, long requestedAllocation,
                         Callback callback) throws IOException {
    addWriter(new Path(path), requestedAllocation, callback);
  }

  default void removeWriter(String path) throws IOException {
    removeWriter(new Path(path));
  }
}
