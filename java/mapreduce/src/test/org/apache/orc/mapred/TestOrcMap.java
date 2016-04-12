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

package org.apache.orc.mapred;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestOrcMap {

  @Test
  public void testRead() throws IOException {
    TypeDescription type =
        TypeDescription.createMap(TypeDescription.createInt(),
            TypeDescription.createLong());
    OrcMap<IntWritable, LongWritable> expected = new OrcMap<>(type);
    OrcMap<IntWritable, LongWritable> actual = new OrcMap<>(type);
    expected.put(new IntWritable(999), new LongWritable(1111));
    expected.put(new IntWritable(888), new LongWritable(2222));
    expected.put(new IntWritable(777), new LongWritable(3333));
    assertNotEquals(expected, actual);
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.clear();
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.put(new IntWritable(666), null);
    expected.put(new IntWritable(1), new LongWritable(777));
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
  }
}
