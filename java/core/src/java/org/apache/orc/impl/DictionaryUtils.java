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


public class DictionaryUtils {
  private DictionaryUtils() {
    // Utility class does nothing in constructor
  }

  /**
   * Obtain the UTF8 string from the byteArray using the offset in index-array.
   * @param result Container for the UTF8 String.
   * @param position position in the keyOffsets
   * @param keyOffsets starting offset of the key (in byte) in the byte array.
   * @param byteArray storing raw bytes of all key seen in dictionary
   */
  public static void getTextInternal(Text result, int position,
      DynamicIntArray keyOffsets, DynamicByteArray byteArray) {
    int offset = keyOffsets.get(position);
    int length;
    if (position + 1 == keyOffsets.size()) {
      length = byteArray.size() - offset;
    } else {
      length = keyOffsets.get(position + 1) - offset;
    }
    byteArray.setText(result, offset, length);
  }
}
