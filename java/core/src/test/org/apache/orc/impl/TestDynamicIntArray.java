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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestDynamicIntArray {

    @Test
    public void simpleDynamicIntArrayTest() {
        DynamicIntArray dynamicIntArray = new DynamicIntArray();
        dynamicIntArray.add(10);
        assertEquals(10, dynamicIntArray.get(0));
        assertEquals(1, dynamicIntArray.size());
        assertEquals(32768, dynamicIntArray.getSizeInBytes());
        dynamicIntArray.add(20);
        assertEquals(20, dynamicIntArray.get(1));
        assertEquals(2, dynamicIntArray.size());
        assertEquals(32768, dynamicIntArray.getSizeInBytes());
        dynamicIntArray.clear();
        assertEquals(0, dynamicIntArray.size());
        assertEquals(0, dynamicIntArray.getSizeInBytes());
    }

    @Test
    public void testDynamicIntArrayGrow() {
        // keep chunk size small to test
        DynamicIntArray dynamicIntArray = new DynamicIntArray(10);

        for(int i = 0; i < 25; i++) {
            dynamicIntArray.add(i);
        }
        assertEquals(25, dynamicIntArray.size());
        assertEquals(120, dynamicIntArray.getSizeInBytes());
    }

    @Test
    public void testIncrement() {
        DynamicIntArray dynamicIntArray = new DynamicIntArray(10);
        dynamicIntArray.add(10);
        assertEquals(10, dynamicIntArray.get(0));
        dynamicIntArray.increment(0, 10);
        assertEquals(20, dynamicIntArray.get(0));
    }

    @Test
    public void testSet() {
        DynamicIntArray dynamicIntArray = new DynamicIntArray(10);
        dynamicIntArray.add(10);
        assertEquals(10, dynamicIntArray.get(0));
        dynamicIntArray.set(0, 25);
        assertEquals(25, dynamicIntArray.get(0));
    }

    @Test
    public void testInvalidGetIndex() {
        DynamicIntArray dynamicIntArray = new DynamicIntArray(10);
        dynamicIntArray.add(10);
        dynamicIntArray.add(11);
        IndexOutOfBoundsException indexOutOfBoundsException =
            assertThrows(IndexOutOfBoundsException.class, () -> dynamicIntArray.get(11));
        assertEquals("Index 11 is outside of 0..1", indexOutOfBoundsException.getMessage());

        indexOutOfBoundsException =
            assertThrows(IndexOutOfBoundsException.class, () -> dynamicIntArray.get(-1));
        assertEquals("Index -1 is outside of 0..1", indexOutOfBoundsException.getMessage());

    }
}
