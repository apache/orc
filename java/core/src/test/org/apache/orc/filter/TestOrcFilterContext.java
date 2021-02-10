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

package org.apache.orc.filter;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestOrcFilterContext {

  private final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createStruct()
      .addField("f2a", TypeDescription.createLong())
      .addField("f2b", TypeDescription.createString()))
    .addField("f3", TypeDescription.createString());

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSuccessfulRetrieval() {
    VectorizedRowBatch b = createBatch();
    OrcFilterContext fc = new OrcFilterContext(schema);
    fc.setBatch(b);

    validateF1Vector(fc.findColumnVector("f1"), 1);
    validateF2Vector(fc.findColumnVector("f2"));
    validateF2AVector(fc.findColumnVector("f2.f2a"));
    validateF2BVector(fc.findColumnVector("f2.f2b"));
    validateF3Vector(fc.findColumnVector("f3"));
  }

  @Test
  public void testSuccessfulRetrievalWithBatchChange() {
    VectorizedRowBatch b1 = createBatch();
    VectorizedRowBatch b2 = createBatch();
    ((LongColumnVector) b2.cols[0]).vector[0] = 100;
    OrcFilterContext fc = new OrcFilterContext(schema);
    fc.setBatch(b1);
    validateF1Vector(fc.findColumnVector("f1"), 1);
    // Change the batch
    fc.setBatch(b2);
    validateF1Vector(fc.findColumnVector("f1"), 100);
  }

  @Test
  public void testMissingFieldTopLevel() {
    VectorizedRowBatch b = createBatch();
    OrcFilterContext fc = new OrcFilterContext(schema);
    fc.setBatch(b);

    // Missing field at top level
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Field f4 not found in");
    fc.findColumnVector("f4");
  }

  @Test
  public void testMissingFieldNestedLevel() {
    VectorizedRowBatch b = createBatch();
    OrcFilterContext fc = new OrcFilterContext(schema);
    fc.setBatch(b);

    // Missing field at top level
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Field c not found in [f2a, f2b]");
    fc.findColumnVector("f2.c");
  }

  @Test
  public void testNestingNonStructField() {
    VectorizedRowBatch b = createBatch();
    OrcFilterContext fc = new OrcFilterContext(schema);
    fc.setBatch(b);

    // Missing field at top level
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Field f3:string not a struct field does not allow nested reference");
    fc.findColumnVector("f3.c");
  }

  @Test
  public void testPropagations() {
    OrcFilterContext fc = new OrcFilterContext(schema);
    assertNull(fc.batch);
    fc.setBatch(schema.createRowBatch());
    assertNotNull(fc.batch);
    assertFalse(fc.isSelectedInUse());

    // Set selections
    fc.setSelectedInUse(true);
    fc.getSelected()[0] = 5;
    fc.setSelectedSize(1);
    assertTrue(fc.isSelectedInUse());
    assertEquals(1, fc.getSelectedSize());
    assertEquals(fc.batch.getMaxSize(), fc.getSelected().length);
    assertArrayEquals(new int[] {5}, Arrays.copyOf(fc.getSelected(), fc.getSelectedSize()));
    assertTrue(fc.validateSelected());
    fc.setSelectedSize(2);
    assertFalse(fc.validateSelected());

    // Use a new selected vector
    fc.setSelected(new int[fc.batch.getMaxSize()]);
    assertArrayEquals(new int[] {0, 0}, Arrays.copyOf(fc.getSelected(), fc.getSelectedSize()));

    // Increase the size of the vector
    fc.reset();
    assertFalse(fc.isSelectedInUse());
    int currSize = fc.batch.getMaxSize();
    assertEquals(currSize, fc.getSelected().length);
    fc.updateSelected(currSize+ 1);
    assertEquals(currSize + 1, fc.getSelected().length);

    // Set the filter context
    fc.setFilterContext(true, new int[3], 1);
    assertTrue(fc.isSelectedInUse());
    assertEquals(3, fc.batch.getMaxSize());
    assertEquals(1, fc.getSelectedSize());
  }

  private VectorizedRowBatch createBatch() {
    VectorizedRowBatch b = schema.createRowBatch();
    LongColumnVector v1 = (LongColumnVector) b.cols[0];
    StructColumnVector v2 = (StructColumnVector) b.cols[1];
    LongColumnVector v2a = (LongColumnVector) v2.fields[0];
    BytesColumnVector v2b = (BytesColumnVector) v2.fields[1];
    BytesColumnVector v3 = (BytesColumnVector) b.cols[2];

    v1.vector[0] = 1;
    v2a.vector[0] = 2;
    v2b.setVal(0, "3".getBytes(StandardCharsets.UTF_8));
    v3.setVal(0, "4".getBytes(StandardCharsets.UTF_8));
    return b;
  }

  private void validateF1Vector(ColumnVector v, long headValue) {
    LongColumnVector l = (LongColumnVector) v;
    Assert.assertEquals(headValue, l.vector[0]);
  }

  private void validateF2Vector(ColumnVector v) {
    StructColumnVector s = (StructColumnVector) v;
    validateF2AVector(s.fields[0]);
    validateF2BVector(s.fields[1]);
  }

  private void validateF2AVector(ColumnVector v) {
    LongColumnVector l = (LongColumnVector) v;
    Assert.assertEquals(2, l.vector[0]);
  }

  private void validateF2BVector(ColumnVector v) {
    BytesColumnVector b = (BytesColumnVector) v;
    Assert.assertEquals("3", b.toString(0));
  }

  private void validateF3Vector(ColumnVector v) {
    BytesColumnVector b = (BytesColumnVector) v;
    Assert.assertEquals("4", b.toString(0));
  }
}