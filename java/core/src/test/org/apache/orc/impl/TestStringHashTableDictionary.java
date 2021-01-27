/**
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
import org.junit.Assert;
import org.junit.Test;


public class TestStringHashTableDictionary {

  /**
   * A extension for {@link StringHashTableDictionary} for testing purpose by overwriting the hash function.
   *
   */
  private class SimpleHashDictionary extends StringHashTableDictionary {
    public SimpleHashDictionary(int initialCapacity) {
      super(initialCapacity);
    }

    /**
     * Obtain the prefix for each string as the hash value.
     * All the string being used in this test suite will contains its hash value as the prefix for the string content.
     * this way we know the order of the traverse() method.
     */
    @Override
    int getIndex(Text text) {
      String s = text.toString();
      int underscore = s.indexOf("_");
      return Integer.parseInt(text.toString().substring(0, underscore));
    }
  }

  @Test
  public void test1()
      throws Exception {
    SimpleHashDictionary hashTableDictionary = new SimpleHashDictionary(5);
    // Non-resize trivial cases
    Assert.assertEquals(0, hashTableDictionary.getSizeInBytes());
    Assert.assertEquals(0, hashTableDictionary.add(new Text("2_Alice")));
    Assert.assertEquals(1, hashTableDictionary.add(new Text("3_Bob")));
    Assert.assertEquals(0, hashTableDictionary.add(new Text("2_Alice")));
    Assert.assertEquals(1, hashTableDictionary.add(new Text("3_Bob")));
    Assert.assertEquals(2, hashTableDictionary.add(new Text("1_Cindy")));

    Text text = new Text();
    hashTableDictionary.getText(text, 0);
    Assert.assertEquals("2_Alice", text.toString());
    hashTableDictionary.getText(text, 1);
    Assert.assertEquals("3_Bob", text.toString());
    hashTableDictionary.getText(text, 2);
    Assert.assertEquals("1_Cindy", text.toString());

    // entering the fourth and fifth element which triggers rehash
    Assert.assertEquals(3, hashTableDictionary.add(new Text("0_David")));
    hashTableDictionary.getText(text, 3);
    Assert.assertEquals("0_David", text.toString());
    Assert.assertEquals(4, hashTableDictionary.add(new Text("4_Eason")));
    hashTableDictionary.getText(text, 4);
    Assert.assertEquals("4_Eason", text.toString());

    // Re-ensure no all previously existed string still have correct encoded value
    hashTableDictionary.getText(text, 0);
    Assert.assertEquals("2_Alice", text.toString());
    hashTableDictionary.getText(text, 1);
    Assert.assertEquals("3_Bob", text.toString());
    hashTableDictionary.getText(text, 2);
    Assert.assertEquals("1_Cindy", text.toString());


    // The order of words are based on each string's prefix given their index in the hashArray will be based on that.
    TestStringRedBlackTree
        .checkContents(hashTableDictionary, new int[]{3, 2, 0, 1, 4}, "0_David", "1_Cindy", "2_Alice", "3_Bob",
            "4_Eason");

    hashTableDictionary.clear();
    Assert.assertEquals(0, hashTableDictionary.size());
  }
}