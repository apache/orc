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
import org.apache.hadoop.io.Writable;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestOrcList {

  static void cloneWritable(Writable source,
                            Writable destination) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer(1024);
    source.write(out);
    out.flush();
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    destination.readFields(in);
  }

  @Test
  public void testRead() throws IOException {
    TypeDescription type =
        TypeDescription.createList(TypeDescription.createInt());
    OrcList<IntWritable> expected = new OrcList<>(type);
    OrcList<IntWritable> actual = new OrcList<>(type);
    expected.add(new IntWritable(123));
    expected.add(new IntWritable(456));
    expected.add(new IntWritable(789));
    assertNotEquals(expected, actual);
    cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.clear();
    cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.add(null);
    expected.add(new IntWritable(500));
    cloneWritable(expected, actual);
    assertEquals(expected, actual);
  }
}
