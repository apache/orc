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

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A red-black tree that stores strings. The strings are stored as UTF-8 bytes
 * and an offset for each entry.
 */
public class StringRedBlackTree extends RedBlackTree implements Dictionary {
  private final DynamicByteArray byteArray = new DynamicByteArray();
  private final DynamicIntArray keyOffsets;

  public StringRedBlackTree(int initialCapacity) {
    super(initialCapacity);
    keyOffsets = new DynamicIntArray(initialCapacity);
  }

  public int add(String value) {
    byte[] b = value.getBytes(StandardCharsets.UTF_8);
    return add(b, 0, b.length);
  }

  @Deprecated
  public int add(Text value) {
    return add(value.getBytes(), 0, value.getLength());
  }

  @Override
  public int add(byte[] bytes, int offset, int length) {
    // if the newKey is actually new, add it to our byteArray and store the offset & length
    if (doAdd(bytes, offset, length)) {
      keyOffsets.add(byteArray.add(bytes, offset, length));
    }
    return lastAdd;
  }

  @Override
  protected int compareValue(int position, byte[] bytes, int offset, int length) {
    int start = keyOffsets.get(position);
    int end;
    if (position + 1 == keyOffsets.size()) {
      end = byteArray.size();
    } else {
      end = keyOffsets.get(position+1);
    }
    return byteArray.compare(bytes, offset, length, start, end - start);
  }



  private void recurse(int node, Dictionary.Visitor visitor,
      VisitorContextImpl context) throws IOException {
    if (node != NULL) {
      recurse(getLeft(node), visitor, context);
      context.setPosition(node);
      visitor.visit(context);
      recurse(getRight(node), visitor, context);
    }
  }

  /**
   * Visit all of the nodes in the tree in sorted order.
   * @param visitor the action to be applied to each node
   * @throws IOException
   */
  @Override
  public void visit(Dictionary.Visitor visitor) throws IOException {
    recurse(root, visitor,
        new VisitorContextImpl(this.byteArray, this.keyOffsets));
  }

  /**
   * Reset the table to empty.
   */
  @Override
  public void clear() {
    super.clear();
    byteArray.clear();
    keyOffsets.clear();
  }

  @Override
  public void getText(Text result, int originalPosition) {
    DictionaryUtils.getTextInternal(result, originalPosition, this.keyOffsets, this.byteArray);
  }

  @Override
  public ByteBuffer getText(int positionInKeyOffset) {
    return DictionaryUtils.getTextInternal(positionInKeyOffset, this.keyOffsets, this.byteArray);
  }

  @Override
  public int writeTo(OutputStream out, int position) throws IOException {
    return DictionaryUtils.writeToTextInternal(out, position, this.keyOffsets,
        this.byteArray);
  }

  /**
   * Get the size of the character data in the table.
   * @return the bytes used by the table
   */
  public int getCharacterSize() {
    return byteArray.size();
  }

  /**
   * Calculate the approximate size in memory.
   * @return the number of bytes used in storing the tree.
   */
  @Override
  public long getSizeInBytes() {
    return byteArray.getSizeInBytes() + keyOffsets.getSizeInBytes() +
      super.getSizeInBytes();
  }
}
