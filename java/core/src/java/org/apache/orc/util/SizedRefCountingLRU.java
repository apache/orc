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
package org.apache.orc.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A least recently used cache that has a capped size and reference counts it elements so as not
 * to remove any that are in use.  This class is thread safe.
 *
 * <p>
 *   The sizing is not based on number of entries but the aggregate size of the entries.  The
 *   size of each entry is defined by the {@link Measurable} interface.
 * </p>
 * <p>
 *   The reference counting is done by forcing the caller to release the entry when finished with
 *   it.  This prevents entries from being dropped while they are in use.
 * </p>
 * <p>
 *   If the caller attempts to add a value to the cache and there is not sufficient room, first
 *   an attempt will be made to find records with a ref count of zero, which will then be dropped.
 *   If no such (or not enough) records can be found, then the caller will be told he cannot
 *   add the value to the LRU.
 * </p>
 */
public class SizedRefCountingLRU<K, V extends Measurable> {
  private final long maxSize;
  private long currentSize;
  private Map<K, ValueWrapper> entries;

  public SizedRefCountingLRU(long maxSize) {
    this.maxSize = maxSize;
    currentSize = 0;
    entries = new HashMap<>();
  }

  public long getMaxSize() {
    return maxSize;
  }

  /**
   * Release an entry.  This must be called once the caller is done with the object obtained by
   * {@link #get(Object)} or placed into the LRU via {@link #put(Object, Measurable)}, otherwise
   * the LRU will fill up with entries it cannot remove.
   * @param key key of the entry to release.
   */
  public synchronized void release(K key) {
    ValueWrapper wrapper = entries.get(key);
    if (wrapper == null) {
      throw new RuntimeException("Attempt to release key " + key.toString() + " that was not in " +
          "the LRU");
    }
    assert wrapper.references >= 1;
    wrapper.references--;
  }

  /**
   * Get the entry.  After you are finished you <b>must</b> call {@link #release(Object)} to tell
   * the LRU you are done with the object and it can be forgotten if necessary.
   * @param key key for the entry
   * @return the entry if it is contained, or null otherwise.
   */
  public synchronized V get(K key) {
    ValueWrapper wrapper = entries.get(key);
    if (wrapper != null) {
      wrapper.references++;
      wrapper.lastReferenced = System.currentTimeMillis();
      return wrapper.value;
    }
    return null;
  }

  /**
   * Try to add an entry.  This is not guaranteed to succeed, you must pay attention to the return
   * value.  If an attempt is made to add an element and there is not room (as measured by the
   * sum of the sizes of the values), the LRU will look for entries that are not in use, and
   * remove them in least recently used order.  If sufficient room still cannot be found then the
   * caller will be told the entry could not be added.
   * <p>If the call succeeds you must call {@link #release(Object)} once you are finished with
   * the object you put into the LRU.</p>
   * @param key key of the new entry
   * @param value The value of the new entry. It is a Measurable so that the LRU can understand
   *              the size of it.
   * @return true if there was room and the value was successfully added.  False if room could
   * not be found.
   */
  public synchronized boolean put(K key, V value) {
    long addedSize = value.size();

    if (addedSize > maxSize) return false;

    List<ValueWrapper> sortedValues = null;
    while (addedSize + currentSize > maxSize) {
      if (sortedValues == null) {
        sortedValues = new ArrayList<>(entries.values());
        Collections.sort(sortedValues, new Comparator<ValueWrapper>() {
          @Override
          public int compare(ValueWrapper o1,
                             ValueWrapper o2) {
            return Long.compare(o1.lastReferenced, o2.lastReferenced);
          }
        });

      }
      if (!removeLastRemovableOne(sortedValues)) return false; // This means we couldn't find anymore room.
    }
    ValueWrapper v = new ValueWrapper(key, value, 1, System.currentTimeMillis());
    currentSize += addedSize;
    entries.put(key, v);
    return true;
  }

  private boolean removeLastRemovableOne(List<ValueWrapper> sortedValues) {
    Iterator<ValueWrapper> iter = sortedValues.iterator();
    while (iter.hasNext()) {
      ValueWrapper wrapper = iter.next();
      if (wrapper.references < 1) {
        currentSize -= wrapper.value.size();
        entries.remove(wrapper.key);
        iter.remove();
        return true;
      }
    }
    return false;
  }

  private class ValueWrapper {
    final K key;
    final V value;
    int references;
    long lastReferenced;

    ValueWrapper(K key, V value, int references, long lastReferenced) {
      this.key = key;
      this.value = value;
      this.references = references;
      this.lastReferenced = lastReferenced;
    }
  }

}
