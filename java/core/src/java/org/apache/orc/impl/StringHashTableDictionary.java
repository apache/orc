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
import java.util.Arrays;

import org.apache.hadoop.io.Text;


/**
 * Using HashTable to represent a dictionary. The strings are stored as UTF-8 bytes
 * and an offset for each entry. It is using chaining for collision resolution.
 *
 * This implementation is not thread-safe. It also assumes there's no reduction in the size of hash-table
 * as it shouldn't happen in the use cases for this class.
 */
public class StringHashTableDictionary implements Dictionary {

  private final DynamicByteArray byteArray = new DynamicByteArray();
  // starting offset of the key (in byte) in the byte array.
  private final DynamicIntArray keyOffsets;

  private final Text newKey = new Text();

  private DynamicIntArray[] hashArray;

  private int capacity;

  private int threshold;

  private float loadFactor;

  private static float DEFAULT_LOAD_FACTOR = 0.75f;

  private static final int BUCKET_SIZE = 20;

  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  public StringHashTableDictionary(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public StringHashTableDictionary(int initialCapacity, float loadFactor) {
    this.capacity = initialCapacity;
    this.loadFactor = loadFactor;
    this.keyOffsets = new DynamicIntArray(initialCapacity);
    this.hashArray = initHashArray(initialCapacity);
    this.threshold = (int)Math.min(initialCapacity * loadFactor, MAX_ARRAY_SIZE + 1);
  }

  private DynamicIntArray[] initHashArray(int capacity) {
    DynamicIntArray[] buckets = new DynamicIntArray[capacity];
    for (int i = 0; i < capacity; i++) {
      // We don't need large bucket: If we have more than a handful of collisions,
      // then the table is too small or the function isn't good.
      buckets[i] = createBucket();
    }
    return buckets;
  }

  private DynamicIntArray createBucket() {
    return new DynamicIntArray(BUCKET_SIZE);
  }

  @Override
  public void visit(Visitor visitor)
      throws IOException {
    traverse(visitor, new VisitorContextImpl(this.byteArray, this.keyOffsets));
  }

  private void traverse(Visitor visitor, VisitorContextImpl context) throws IOException {
    for (DynamicIntArray intArray : hashArray) {
      for (int i = 0; i < intArray.size() ; i ++) {
        context.setPosition(intArray.get(i));
        visitor.visit(context);
      }
    }
  }

  @Override
  public void clear() {
    byteArray.clear();
    keyOffsets.clear();
    Arrays.fill(hashArray, null);
  }

  @Override
  public void getText(Text result, int position) {
    DictionaryUtils.getTextInternal(result, position, this.keyOffsets, this.byteArray);
  }

  @Override
  public int add(byte[] bytes, int offset, int length) {
    newKey.set(bytes, offset, length);
    return add(newKey);
  }

  public int add(Text text) {
    resizeIfNeeded();
    newKey.set(text);

    int index = getIndex(text);
    DynamicIntArray candidateArray = hashArray[index];

    Text tmpText = new Text();
    for (int i = 0; i < candidateArray.size(); i++) {
      getText(tmpText, candidateArray.get(i));
      if (tmpText.equals(newKey)) {
        return candidateArray.get(i);
      }
    }

    // if making it here, it means no match.
    int len = newKey.getLength();
    int currIdx = keyOffsets.size();
    keyOffsets.add(byteArray.add(newKey.getBytes(), 0, len));
    candidateArray.add(currIdx);
    return currIdx;
  }

  private void resizeIfNeeded() {
    if (keyOffsets.size() >= threshold) {
      int oldCapacity = keyOffsets.size();
      int newCapacity = (oldCapacity << 1) + 1;
      doResize(newCapacity);
      this.threshold = (int)Math.min(newCapacity * loadFactor, MAX_ARRAY_SIZE + 1);
    }
  }

  @Override
  public int size() {
    return keyOffsets.size();
  }

  /**
   * Compute the hash value and find the corresponding index.
   *
   */
  int getIndex(Text text) {
    return Math.floorMod(text.hashCode(), capacity);
  }

  // Resize the hash table, re-hash all the existing keys.
  // byteArray and keyOffsetsArray don't have to be re-filled.
  private void doResize(int newSize) {
    DynamicIntArray[] resizedHashArray = new DynamicIntArray[newSize];
    for (int i = 0; i < newSize; i++) {
      resizedHashArray[i] = createBucket();
    }

    Text tmpText = new Text();
    for (int i = 0; i < capacity; i++) {
      DynamicIntArray intArray = hashArray[i];
      int bucketSize = intArray.size();
      for (int j = 0; j < bucketSize; j++) {
        getText(tmpText, intArray.get(j));
        int newIndex = getIndex(tmpText);
        resizedHashArray[newIndex].add(intArray.get(j));
      }
    }

    Arrays.fill(hashArray, null);
    hashArray = resizedHashArray;
  }

  @Override
  public long getSizeInBytes() {
    long bucketTotalSize = 0L;
    for (DynamicIntArray dynamicIntArray : hashArray) {
      bucketTotalSize += dynamicIntArray.size();
    }

    return byteArray.getSizeInBytes() + keyOffsets.getSizeInBytes() + bucketTotalSize ;
  }
}
