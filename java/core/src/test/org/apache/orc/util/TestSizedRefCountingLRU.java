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

import org.junit.Assert;
import org.junit.Test;

public class TestSizedRefCountingLRU {

  @Test
  public void simplePutGetRelease() {
    SizedRefCountingLRU<Long, MeasurableLong> lru = new SizedRefCountingLRU<>(100);
    for (long i = 0; i < 10; i++) {
      Assert.assertTrue(lru.put(i, new MeasurableLong(i, i)));
    }
    for (long i = 0; i < 10; i++) {
      Assert.assertEquals(i, lru.get(i).value);
      lru.release(i);
    }
  }

  @Test
  public void tooBigRejected() {
    SizedRefCountingLRU<Long, MeasurableLong> lru = new SizedRefCountingLRU<>(100);
    Assert.assertFalse(lru.put(5L, new MeasurableLong(1, 1000)));
  }

  @Test
  public void fillAndRelease() {
    MeasurableLong m = new MeasurableLong(1, 10);
    SizedRefCountingLRU<Long, MeasurableLong> lru = new SizedRefCountingLRU<>(101);
    for (long i = 0; i < 10; i++) {
      Assert.assertTrue(lru.put(i, m));
    }

    Assert.assertFalse(lru.put(11L, m));

    lru.release(1L);

    Assert.assertTrue(lru.put(12L, m));
  }

  @Test
  public void removeTwoToFindSlot() throws InterruptedException {
    SizedRefCountingLRU<Long, MeasurableLong> lru = new SizedRefCountingLRU<>(101);
    for (long i = 0; i < 10; i++) {
      Assert.assertTrue(lru.put(i, new MeasurableLong(i, 10)));
      Thread.sleep(5);
    }

    for (long i = 0; i < 10; i++) lru.release(i);

    Assert.assertTrue(lru.put(10L, new MeasurableLong(10, 20)));

    Assert.assertNull(lru.get(0L));
    Assert.assertNull(lru.get(1L));
    for (long i = 2; i < 11; i++) Assert.assertEquals(i, lru.get(i).value);
  }

  @Test
  public void removeTwoToFindSlotInMiddle() throws InterruptedException {
    SizedRefCountingLRU<Long, MeasurableLong> lru = new SizedRefCountingLRU<>(101);
    for (long i = 0; i < 10; i++) {
      Assert.assertTrue(lru.put(i, new MeasurableLong(i, 10)));
      Thread.sleep(5);
    }

    lru.release(3L);
    lru.release(4L);
    lru.release(5L);

    Assert.assertTrue(lru.put(10L, new MeasurableLong(10, 20)));

    for (long i = 0; i < 3; i++) Assert.assertEquals(i, lru.get(i).value);
    Assert.assertNull(lru.get(3L));
    Assert.assertNull(lru.get(4L));
    for (long i = 5; i < 11; i++) Assert.assertEquals(i, lru.get(i).value);
  }

  private static class MeasurableLong implements Measurable {
    final long value;
    final long size;

    MeasurableLong(long value, long size) {
      this.value = value;
      this.size = size;
    }

    @Override
    public long size() {
      return size;
    }
  }
}
