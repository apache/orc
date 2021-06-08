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

package org.apache.orc.gen;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class FilterGeneratorTest {

  private final FilterGenerator fg;

  public FilterGeneratorTest() {
    this.fg = new FilterGenerator();
    fg.setTemplateDir("../core/src/gen/filters/");
  }


  @Test
  public void simpleGenTest() throws IOException, IllegalAccessException {
    String content = fg.genSimpleOpFilter(FilterGenerator.FilterType.LONG,
                                          FilterGenerator.FilterOp.EQUALS,
                                          false);
    Assert.assertNotNull(content);
    Assert.assertFalse(content.contains("<ClassName>"));
    Assert.assertFalse(content.contains("<LeafType>"));
    Assert.assertFalse(content.contains("<LeafVector>"));
    Assert.assertFalse(content.contains("<Operator>"));
    Assert.assertFalse(content.contains(FilterGenerator.FilterOp.EQUALS.notOp));
  }

  @Test
  public void simpleGenNotTest() throws IOException, IllegalAccessException {
    String content = fg.genSimpleOpFilter(FilterGenerator.FilterType.LONG,
                                          FilterGenerator.FilterOp.EQUALS,
                                          true);
    Assert.assertNotNull(content);
    Assert.assertFalse(content.contains("<ClassName>"));
    Assert.assertFalse(content.contains("<LeafType>"));
    Assert.assertFalse(content.contains("<LeafVector>"));
    Assert.assertFalse(content.contains("<Operator>"));
    Assert.assertTrue(content.contains(FilterGenerator.FilterOp.EQUALS.notOp));
  }

  @Test
  public void pascalCaseTest() {
    Assert.assertNull(FilterGenerator.toPascalCase(null));
    Assert.assertEquals("", FilterGenerator.toPascalCase(""));
    Assert.assertEquals("Abcd", FilterGenerator.toPascalCase("abcd"));
    Assert.assertEquals("Abcda", FilterGenerator.toPascalCase("Abcda"));
    Assert.assertEquals("A", FilterGenerator.toPascalCase("a"));
    Assert.assertEquals("LessThan", FilterGenerator.toPascalCase("LESS_THAN"));
    Assert.assertEquals("__LessThan", FilterGenerator.toPascalCase("__LESS_THAN"));
  }

  @Test
  public void camelCaseTest() {
    Assert.assertNull(FilterGenerator.toCamelCase(null));
    Assert.assertEquals("", FilterGenerator.toCamelCase(""));
    Assert.assertEquals("abcd", FilterGenerator.toCamelCase("abcd"));
    Assert.assertEquals("abcda", FilterGenerator.toCamelCase("Abcda"));
    Assert.assertEquals("a", FilterGenerator.toCamelCase("a"));
    Assert.assertEquals("lessThan", FilterGenerator.toCamelCase("LESS_THAN"));
    Assert.assertEquals("__LessThan", FilterGenerator.toCamelCase("__LESS_THAN"));
  }
}