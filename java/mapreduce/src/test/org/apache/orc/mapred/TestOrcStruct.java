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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.TypeDescription;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestOrcStruct {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRead() throws IOException {
    TypeDescription type =
        TypeDescription.createStruct()
          .addField("f1", TypeDescription.createInt())
          .addField("f2", TypeDescription.createLong())
          .addField("f3", TypeDescription.createString());
    OrcStruct expected = new OrcStruct(type);
    OrcStruct actual = new OrcStruct(type);
    assertEquals(3, expected.getNumFields());
    expected.setFieldValue(0, new IntWritable(1));
    expected.setFieldValue(1, new LongWritable(2));
    expected.setFieldValue(2, new Text("wow"));
    assertEquals(147710, expected.hashCode());
    assertNotEquals(expected, actual);
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    expected.setFieldValue(0, null);
    expected.setFieldValue(1, null);
    expected.setFieldValue(2, null);
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
    assertEquals(3, expected.hashCode());
    expected.setFieldValue(1, new LongWritable(111));
    assertEquals(111, ((LongWritable) expected.getFieldValue(1)).get());
    TestOrcList.cloneWritable(expected, actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testFieldAccess() {
    OrcStruct struct = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:double,k:string>"));
    struct.setFieldValue("j", new DoubleWritable(1.5));
    struct.setFieldValue("k", new Text("Moria"));
    struct.setFieldValue(0, new IntWritable(42));
    assertEquals(new IntWritable(42), struct.getFieldValue("i"));
    assertEquals(new DoubleWritable(1.5), struct.getFieldValue(1));
    assertEquals(new Text("Moria"), struct.getFieldValue("k"));
    struct.setAllFields(new IntWritable(123), new DoubleWritable(4.5),
        new Text("ok"));
    assertEquals("123", struct.getFieldValue(0).toString());
    assertEquals("4.5", struct.getFieldValue(1).toString());
    assertEquals("ok", struct.getFieldValue(2).toString());
  }

  @Test
  public void testBadFieldRead() {
    OrcStruct struct = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:double,k:string>"));
    thrown.expect(IllegalArgumentException.class);
    struct.getFieldValue("bad");
  }

  @Test
  public void testBadFieldWrite() {
    OrcStruct struct = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:double,k:string>"));
    thrown.expect(IllegalArgumentException.class);
    struct.setFieldValue("bad", new Text("foobar"));
  }

  @Test
  public void testCompare() {
    OrcStruct left = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:string>"));
    assertEquals(-1 ,left.compareTo(null));
    OrcStruct right = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:string,k:int>"));
    left.setFieldValue(0, new IntWritable(10));
    right.setFieldValue(0, new IntWritable(12));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
    right.setFieldValue(0, new IntWritable(10));
    left.setFieldValue(1, new Text("a"));
    right.setFieldValue(1, new Text("b"));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
    right.setFieldValue(1, new Text("a"));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
    right = new OrcStruct(TypeDescription.fromString
        ("struct<i:int,j:string>"));
    left.setFieldValue(0, null);
    left.setFieldValue(1, null);
    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));
    right.setFieldValue(0, new IntWritable(12));
    assertEquals(1 , left.compareTo(right));
    assertEquals(-1, right.compareTo(left));
  }

  @Test
  public void testSchemaInCompare() {
    TypeDescription leftType = TypeDescription.fromString("struct<s:string,i:int>");
    TypeDescription rightType = TypeDescription.fromString("struct<s:string,j:bigint>");
    OrcStruct left = new OrcStruct(leftType);
    OrcStruct right = new OrcStruct(rightType);
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
    left.setAllFields(new Text("123"), new IntWritable(123));
    right.setAllFields(new Text("123"), new LongWritable(456));
    assertEquals(-1, left.compareTo(right));
    assertEquals(1, right.compareTo(left));
  }
}
