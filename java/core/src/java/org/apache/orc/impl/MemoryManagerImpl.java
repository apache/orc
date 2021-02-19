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

package org.apache.orc.impl;

import java.io.IOException;
import org.apache.orc.MemoryManager;
import org.apache.orc.OrcConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.lang.management.ManagementFactory;

/**
 * Implements a memory manager that keeps a global context of how many ORC
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 *
 * This class is not thread safe, but is re-entrant - ensure creation and all
 * invocations are triggered from the same thread.
 */
public class MemoryManagerImpl extends MemoryManagerImplCore implements MemoryManager {

  /**
   * Create the memory manager.
   * @param conf use the configuration to find the maximum size of the memory
   *             pool.
   */
  public MemoryManagerImpl(Configuration conf) {
    this(Math.round(ManagementFactory.getMemoryMXBean().
        getHeapMemoryUsage().getMax() * OrcConf.MEMORY_POOL.getDouble(conf)));
  }

  /**
   * Create the memory manager
   * @param poolSize the size of memory to use
   */
  public MemoryManagerImpl(long poolSize) {
    super(poolSize);
  }

  /**
   * Add a new writer's memory allocation to the pool. We use the path
   * as a unique key to ensure that we don't get duplicates.
   * @param path the file that is being written
   * @param requestedAllocation the requested buffer size
   */
  @Override
  public void addWriter(Path path, long requestedAllocation,
                              Callback callback) {
    addWriter(path.toString(), requestedAllocation, callback);
  }

  /**
   * Remove the given writer from the pool.
   * @param path the file that has been closed
   */
  @Override
  public void removeWriter(Path path) throws IOException {
    removeWriter(path.toString());
  }
}
