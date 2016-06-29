/**
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
package org.apache.orc.impl;

import org.apache.orc.TypeDescription;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertSame;

public class TestSchemaEvolution {

  private boolean[] includeAll(TypeDescription readerType) {
    int numColumns = readerType.getMaximumId() + 1;
    boolean[] result = new boolean[numColumns];
    Arrays.fill(result, true);
    return result;
  }

  @Test
  public void testAddFieldToEnd() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,b:string,c:double>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // b -> b
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);

    // c -> null
    reader = readerType.getChildren().get(2);
    mapped = transition.getFileType(reader);
    original = null;
    assertSame(original, mapped);
  }

  @Test
  public void testAddFieldBeforeEnd() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,c:double,b:string>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // c -> null
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = null;
    assertSame(original, mapped);

    // b -> b
    reader = readerType.getChildren().get(2);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testRemoveLastField() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string,c:double>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,b:string>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // b -> b
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testRemoveFieldBeforeEnd() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string,c:double>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,c:double>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // c -> b
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(2);
    assertSame(original, mapped);

  }

  @Test
  public void testRemoveAndAddField() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:int,c:double>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // c -> null
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = null;
    assertSame(original, mapped);
  }

  @Test
  public void testReorderFields() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:int,b:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<b:string,a:int>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // b -> b
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(1);
    assertSame(original, mapped);

    // a -> a
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(0);
    assertSame(original, mapped);
  }

  @Test
  public void testAddFieldEndOfStruct() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:struct<b:int>,c:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:struct<b:int,d:double>,c:string>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    TypeDescription readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    TypeDescription originalChild = original.getChildren().get(0);
    assertSame(originalChild, mapped);

    // a.d -> null
    readerChild = reader.getChildren().get(1);
    mapped = transition.getFileType(readerChild);
    originalChild = null;
    assertSame(originalChild, mapped);

    // c -> c
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testAddFieldBeforeEndOfStruct() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:struct<b:int>,c:string>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:struct<d:double,b:int>,c:string>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    TypeDescription readerChild = reader.getChildren().get(1);
    mapped = transition.getFileType(readerChild);
    TypeDescription originalChild = original.getChildren().get(0);
    assertSame(originalChild, mapped);

    // a.d -> null
    readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    originalChild = null;
    assertSame(originalChild, mapped);

    // c -> c
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = fileType.getChildren().get(1);
    assertSame(original, mapped);
  }

  /**
   * Two structs can be equal but in different locations. They can converge to this.
   */
  @Test
  public void testAddSimilarField() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:struct<b:int>>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:struct<b:int>,c:struct<b:int>>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    TypeDescription readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    TypeDescription originalChild = original.getChildren().get(0);
    assertSame(originalChild, mapped);

    // c -> null
    reader = readerType.getChildren().get(1);
    mapped = transition.getFileType(reader);
    original = null;
    assertSame(original, mapped);

    // c.b -> null
    readerChild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerChild);
    original = null;
    assertSame(original, mapped);
  }

  /**
   * Two structs can be equal but in different locations. They can converge to this.
   */
  @Test
  public void testConvergentEvolution() {
    TypeDescription fileType = TypeDescription
        .fromString("struct<a:struct<a:int,b:string>,c:struct<a:int>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<a:struct<a:int,b:string>,c:struct<a:int,b:string>>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // c -> c
    TypeDescription reader = readerType.getChildren().get(1);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(1);
    assertSame(original, mapped);

    // c.a -> c.a
    TypeDescription readerchild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerchild);
    original = original.getChildren().get(0);
    assertSame(original, mapped);

    // c.b -> null
    readerchild = reader.getChildren().get(1);
    mapped = transition.getFileType(readerchild);
    original = null;
    assertSame(original, mapped);
  }

  @Test
  public void testMapEvolution() {
    TypeDescription fileType =
        TypeDescription
            .fromString("struct<a:map<struct<a:int>,struct<a:int>>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<a:map<struct<a:int,b:string>,struct<a:int,c:string>>>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.key -> a.key
    TypeDescription readerchild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerchild);
    original = original.getChildren().get(0);
    assertSame(original, mapped);

    // a.value -> a.value
    readerchild = reader.getChildren().get(1);
    mapped = transition.getFileType(readerchild);
    original = fileType.getChildren().get(0).getChildren().get(1);
    assertSame(original, mapped);
  }

  @Test
  public void testListEvolution() {
    TypeDescription fileType =
        TypeDescription.fromString("struct<a:array<struct<b:int>>>");
    TypeDescription readerType =
        TypeDescription.fromString("struct<a:array<struct<b:int,c:string>>>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);

    // a -> a
    TypeDescription reader = readerType.getChildren().get(0);
    TypeDescription mapped = transition.getFileType(reader);
    TypeDescription original = fileType.getChildren().get(0);
    assertSame(original, mapped);

    // a.element -> a.element
    TypeDescription readerchild = reader.getChildren().get(0);
    mapped = transition.getFileType(readerchild);
    original = original.getChildren().get(0);
    assertSame(original, mapped);

    // a.b -> a.b
    readerchild = reader.getChildren().get(0).getChildren().get(0);
    mapped = transition.getFileType(readerchild);
    original = original.getChildren().get(0);
    assertSame(original, mapped);

    // a.c -> null
    readerchild = reader.getChildren().get(0).getChildren().get(1);
    mapped = transition.getFileType(readerchild);
    original = null;
    assertSame(original, mapped);
  }

  @Test(expected = SchemaEvolution.IllegalEvolutionException.class)
  public void testIncompatibleTypes() {
    TypeDescription fileType = TypeDescription.fromString("struct<a:int>");
    TypeDescription readerType = TypeDescription.fromString("struct<a:date>");
    boolean[] included = includeAll(readerType);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, false, included);
  }
}
