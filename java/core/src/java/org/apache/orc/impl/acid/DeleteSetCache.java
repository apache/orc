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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Cache {@link InMemoryDeleteSet}s, keyed by the base directory being read and the valid transaction
 * list used to parse it.  This also handles releasing DeleteSets if we're under memory pressure.
 */
class DeleteSetCache {

  private static DeleteSetCache self;

  private final Configuration conf;
  private final DeleteSet nullDeleteSet;
  private Map<ParsedAcidDirectory, InMemoryDeleteSet> cache;
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


  private DeleteSetCache(Configuration conf) {
    this.conf = conf;
    cache = new HashMap<>();
    currentSize = 0;
    nullDeleteSet = new DeleteSet() {
      @Override
      public void applyDeletesToBatch(VectorizedRowBatch batch, BitSet selectedBitSet) throws
          IOException {

      }

      @Override
      public void release() {

      }
    };
  }

  // TODO see if I can break up the synchronization here
  synchronized DeleteSet getDeleteSet(ParsedAcidDirectory dir) throws IOException {
    List<ParsedAcidFile> deletes = dir.getDeleteFiles();
    if (deletes == null || deletes.isEmpty()) {
      return nullDeleteSet;
    }

    InMemoryDeleteSet deleteSet = cache.get(dir);
    if (deleteSet != null) {
      deleteSet.setInUse();
      return deleteSet;
    }

    // Figure out if I can fit this in memory or not.  To do this, we have to first get a size
    // estimate.
    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(dir.getConf())
        .acidDir(dir)
        .validTxnList(dir.getValidTxns())
        .filesystem(dir.getDeleteFiles().get(0).getFileStatus().getPath().getFileSystem(dir .getConf()));

    long addedSize = 0;
    List<Reader> readers = new ArrayList<>();
    for (final ParsedAcidFile deleteDelta : dir.getDeleteFiles()) {
      Reader reader =
          new AcidReader(deleteDelta.getFileStatus().getPath(), options,
              Collections.<ParsedAcidFile>emptyList());
      addedSize += reader.getNumberOfRows();
      readers.add(reader);
    }

    long maxSize = OrcConf.MAX_DELETE_ENTRIES_IN_MEMORY.getLong(conf);
    if (addedSize > maxSize) {
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
