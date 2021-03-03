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

package org.apache.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestOrcFilterContext {
  private final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createString())
    .addField("f3",
              TypeDescription.createStruct()
                .addField("a", TypeDescription.createInt())
                .addField("b", TypeDescription.createLong())
                .addField("c",
                          TypeDescription.createMap(TypeDescription.createInt(),
                                                    TypeDescription.createDate())))
    .addField("f4",
              TypeDescription.createList(TypeDescription.createStruct()
                                           .addField("a", TypeDescription.createChar())
                                           .addField("b", TypeDescription.createBoolean())))
    .addField("f5",
              TypeDescription.createMap(TypeDescription.createInt(),
                                        TypeDescription.createDate()))
    .addField("f6",
              TypeDescription.createUnion()
                .addUnionChild(TypeDescription.createInt())
                .addUnionChild(TypeDescription.createStruct()
                                 .addField("a", TypeDescription.createDate())
                                 .addField("b",
                                           TypeDescription.createList(TypeDescription.createChar()))
                )
    );
  private final OrcFilterContext filterContext = new OrcFilterContextImpl(schema)
    .setBatch(schema.createRowBatch());

  @Before
  public void setup() {
    filterContext.reset();
  }

  @Test
  public void testTopLevelElementaryType() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f1");
    assertEquals(1, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(LongColumnVector.class));
  }

  @Test
  public void testTopLevelCompositeType() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3");
    assertEquals(1, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(StructColumnVector.class));

    vectorBranch = filterContext.findColumnVector("f4");
    assertEquals(1, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(ListColumnVector.class));

    vectorBranch = filterContext.findColumnVector("f5");
    assertEquals(1, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(MapColumnVector.class));

    vectorBranch = filterContext.findColumnVector("f6");
    assertEquals(1, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(UnionColumnVector.class));
  }

  @Test
  public void testNestedType() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3.a");
    assertEquals(2, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(StructColumnVector.class));
    assertThat(vectorBranch[1], instanceOf(LongColumnVector.class));

    vectorBranch = filterContext.findColumnVector("f3.c");
    assertEquals(2, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(StructColumnVector.class));
    assertThat(vectorBranch[1], instanceOf(MapColumnVector.class));

    vectorBranch = filterContext.findColumnVector("f6.1.b");
    assertEquals(3, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(UnionColumnVector.class));
    assertThat(vectorBranch[1], instanceOf(StructColumnVector.class));
    assertThat(vectorBranch[2], instanceOf(ListColumnVector.class));
  }

  @Test
  public void testTopLevelVector() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3");
    vectorBranch[0].noNulls = true;
    assertTrue(OrcFilterContext.noNulls(vectorBranch));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 0));

    vectorBranch[0].noNulls = false;
    vectorBranch[0].isNull[0] = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
  }

  @Test
  public void testNestedVector() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = true;
    vectorBranch[1].noNulls = true;
    assertTrue(OrcFilterContext.noNulls(vectorBranch));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 2));

    vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = false;
    vectorBranch[0].isNull[0] = true;
    vectorBranch[1].noNulls = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 2));

    vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = true;
    vectorBranch[1].noNulls = false;
    vectorBranch[1].isNull[2] = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 2));

    vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = false;
    vectorBranch[0].isNull[0] = true;
    vectorBranch[1].noNulls = false;
    vectorBranch[1].isNull[2] = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 2));
  }

  @Test
  public void testTopLevelList() {
    TypeDescription topListSchema = TypeDescription.createList(
      TypeDescription.createStruct()
        .addField("a", TypeDescription.createChar())
        .addField("b", TypeDescription
          .createBoolean()));
    OrcFilterContext fc = new OrcFilterContextImpl(topListSchema)
      .setBatch(topListSchema.createRowBatch());
    ColumnVector[] vectorBranch = fc.findColumnVector("_elem");
    assertEquals(2, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(ListColumnVector.class));
    assertThat(vectorBranch[1], instanceOf(StructColumnVector.class));
  }

  @Test
  public void testUnsupportedIsNullUse() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f4._elem.a");
    assertEquals(3, vectorBranch.length);
    assertThat(vectorBranch[0], instanceOf(ListColumnVector.class));
    assertThat(vectorBranch[1], instanceOf(StructColumnVector.class));
    assertThat(vectorBranch[2], instanceOf(BytesColumnVector.class));

    assertTrue(OrcFilterContext.noNulls(vectorBranch));
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                                                      () -> OrcFilterContext.isNull(vectorBranch,
                                                                                    0));
    assertThat(exception.getMessage(), containsString("ListColumnVector"));
    assertThat(exception.getMessage(), containsString("List and Map vectors are not supported"));
  }

  @Test
  public void testRepeatingVector() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = true;
    vectorBranch[0].isRepeating = true;
    vectorBranch[1].noNulls = true;
    assertTrue(OrcFilterContext.noNulls(vectorBranch));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 2));

    vectorBranch[0].noNulls = false;
    vectorBranch[0].isRepeating = true;
    vectorBranch[0].isNull[0] = true;
    vectorBranch[1].noNulls = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 0));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 1));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 2));
  }
}