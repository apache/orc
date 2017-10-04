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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.util.CloseableLock;
import org.apache.orc.util.SizedRefCountingLRU;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache {@link InMemoryDeleteSet}s, keyed by the base directory being read and the valid transaction
 * list used to parse it.  This also handles releasing DeleteSets if we're under memory pressure.
 */
class DeleteSetCache {

  private static DeleteSetCache self;

  private final DeleteSet nullDeleteSet;
  private SizedRefCountingLRU<ParsedAcidDirectory, InMemoryDeleteSet> cache;
  private Map<ParsedAcidDirectory, Lock> locks;
  private long currentSize;

  static DeleteSetCache getCache(Configuration conf) {
    if (self == null) {
      synchronized (DeleteSetCache.class) {
        if (self == null) {
          self = new DeleteSetCache(conf);
        }
      }
    }
    return self;
  }

  // Just for testing, not for real use.
  static void dumpCache() {
    synchronized (DeleteSetCache.class) {
      self = null;
    }
  }


  private DeleteSetCache(Configuration conf) {
    cache = new SizedRefCountingLRU<>(OrcConf.MAX_DELETE_ENTRIES_IN_MEMORY.getLong(conf));
    locks = new HashMap<>();
    currentSize = 0;
    nullDeleteSet = new DeleteSet() {
      @Override
      public void applyDeletesToBatch(VectorizedRowBatch batch, BitSet selectedBitSet)
          throws IOException {

      }
    };
  }

  DeleteSet getNullDeleteSet() {
    return nullDeleteSet;
  }

  /**
   * Get a delete set for a given ParsedAcidDirectory.  The cache is keyed by directory, which
   * means it is for a specific combination of file system directory and ValidTxnList.  This
   * method is not synchronized but access to a particular value in the cache is.  This prevents
   * multiple threads from accidentally building the same entry simultaneously.
   * @param dir directory to get delete delta from
   * @return Set of deletes for this directory.  This will never be null.  In the case where
   * there are no delete files it will return a nullDeleteSet that never filters records.
   * @throws IOException if the underlying file operations throw an exception.
   */
  DeleteSet getDeleteSet(ParsedAcidDirectory dir) throws IOException {
    List<ParsedAcidFile> deletes = dir.getDeleteFiles();
    if (deletes == null || deletes.isEmpty()) {
      return nullDeleteSet;
    }

    InMemoryDeleteSet deleteSet = cache.get(dir);
    if (deleteSet != null) return deleteSet;

    // We didn't find it in the cache, so we have to build it.  First lock just this key so we
    // don't have multiple builders at the same time.
    Lock lockForThisDir;
    synchronized (this) {
      lockForThisDir = locks.get(dir);
      if (lockForThisDir == null) {
        lockForThisDir = new ReentrantLock();
        locks.put(dir, lockForThisDir);
      }
    }

    lockForThisDir.lock();
    try (CloseableLock cl = new CloseableLock(lockForThisDir)) {
      // Check that someone else didn't build it meantime
      deleteSet = cache.get(dir);
      if (deleteSet != null) return deleteSet;

      // Figure out if I can fit this in memory or not.  To do this, we have to first get a size
      // estimate.
      OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(dir.getConf())
          .acidDir(dir)
          .validTxnList(dir.getValidTxns())
          .filesystem(
              dir.getDeleteFiles().get(0).getFileStatus().getPath().getFileSystem(dir.getConf()));

      long addedSize = 0;
      List<Reader> readers = new ArrayList<>();
      for (final ParsedAcidFile deleteDelta : dir.getDeleteFiles()) {
        Reader reader = new AcidReader(deleteDelta.getFileStatus().getPath(), options);
        addedSize += reader.getNumberOfRows();
        readers.add(reader);
      }

      if (addedSize > cache.getMaxSize()) {
        // Don't dump the cache for no purpose
        return new SortedDeleteSet(readers);
      }
      while (true) {
        if (addedSize + currentSize < maxSize) {
          currentSize += addedSize;
          InMemoryDeleteSet ds = new InMemoryDeleteSet(readers, addedSize);
          cache.put(dir, ds);
          return ds;
        } else {
          // Ok, look for ones we can release
          Iterator<Map.Entry<ParsedAcidDirectory, InMemoryDeleteSet>> iter =
              cache.entrySet().iterator();
          while (iter.hasNext() &&
              currentSize + addedSize > maxSize) {
            Map.Entry<ParsedAcidDirectory, InMemoryDeleteSet> e = iter.next();
            if (!e.getValue().isInUse()) {
              currentSize -= e.getValue().size();
              iter.remove();
            }
          }
          // Did we achive our goal?
          if (currentSize + addedSize < maxSize) {
            continue;  // go back through the loop, we should be able to load now.
          }
          // Bummer, we can't find enough stuff to clean up.  Just do a sorted read
          return new SortedDeleteSet(readers);
        }
      }
    }
  }
}
