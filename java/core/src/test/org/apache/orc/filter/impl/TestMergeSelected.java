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

package org.apache.orc.filter.impl;

import org.junit.Assert;
import org.junit.Test;

public class TestMergeSelected {
  private final Selected src = new Selected();
  private final Selected tgt = new Selected();

  @Test
  public void testBothEmpty() {
    // Both are empty
    src.sel = new int[10];
    tgt.sel = new int[10];
    OrFilter.merge(src, tgt);
    Assert.assertArrayEquals(new int[10], tgt.sel);
    Assert.assertEquals(0, tgt.selSize);
  }

  @Test
  public void testTgtEmpty() {
    // tgt has no selection
    src.sel = new int[] {1, 3, 7, 0, 0};
    src.selSize = 3;
    tgt.sel = new int[5];
    tgt.selSize = 0;
    OrFilter.merge(src, tgt);
    Assert.assertEquals(src.selSize, tgt.selSize);
    Assert.assertArrayEquals(src.sel, tgt.sel);
  }

  @Test
  public void testSrcEmpty() {
    // current size is zero
    src.sel = new int[5];
    src.selSize = 0;
    tgt.sel = new int[] {1, 3, 7, 0, 0};
    tgt.selSize = 3;
    OrFilter.merge(src, tgt);
    Assert.assertEquals(3, tgt.selSize);
    Assert.assertArrayEquals(new int[] {1, 3, 7, 0, 0}, tgt.sel);
  }

  @Test
  public void testCurrSmallerThanAdd() {
    // current size is zero
    src.sel = new int[] {7, 0, 0, 0, 0};
    src.selSize = 1;
    tgt.sel = new int[] {1, 3, 0, 0, 0};
    tgt.selSize = 2;
    OrFilter.merge(src, tgt);
    Assert.assertEquals(3, tgt.selSize);
    Assert.assertArrayEquals(new int[] {1, 3, 7, 0, 0}, tgt.sel);
  }

  @Test
  public void testAddSmallerThanCurr() {
    // current size is zero
    src.sel = new int[] {1, 7, 0, 0, 0};
    src.selSize = 2;
    tgt.sel = new int[] {3, 0, 0, 0, 0};
    tgt.selSize = 1;
    OrFilter.merge(src, tgt);
    Assert.assertEquals(3, tgt.selSize);
    Assert.assertArrayEquals(new int[] {1, 3, 7, 0, 0}, tgt.sel);
  }

  @Test
  public void testNoChange() {
    // current size is zero
    src.sel = new int[] {0, 0, 0, 0, 0};
    src.selSize = 0;
    tgt.sel = new int[] {1, 3, 7, 0, 0};
    tgt.selSize = 3;
    OrFilter.merge(src, tgt);
    Assert.assertEquals(3, tgt.selSize);
    Assert.assertArrayEquals(new int[] {1, 3, 7, 0, 0}, tgt.sel);
  }

  @Test
  public void testNewEnclosed() {
    // current size is zero
    src.sel = new int[] {1, 7, 0, 0, 0};
    src.selSize = 2;
    tgt.sel = new int[] {3, 4, 0, 0, 0};
    tgt.selSize = 2;
    OrFilter.merge(src, tgt);
    Assert.assertEquals(4, tgt.selSize);
    Assert.assertArrayEquals(new int[] {1, 3, 4, 7, 0}, tgt.sel);
  }

  @Test
  public void testPrevEnclosed() {
    // current size is zero
    src.sel = new int[] {3, 4, 0, 0, 0};
    src.selSize = 2;
    tgt.sel = new int[] {1, 7, 0, 0, 0};
    tgt.selSize = 2;
    OrFilter.merge(src, tgt);
    Assert.assertEquals(4, tgt.selSize);
    Assert.assertArrayEquals(new int[] {1, 3, 4, 7, 0}, tgt.sel);
  }
}