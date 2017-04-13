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
package org.apache.orc.pojowriter;

import static org.junit.Assert.assertEquals;

import org.apache.orc.TypeDescription;
import org.apache.orc.pojowriter.PojoSchemaBuilder;
import org.apache.orc.pojowriter.writer.WriterMappingRegistry;
import org.junit.Test;

public class PojoSchemaBuilderTest extends PojoSchemaBuilder {

  @Test
  public void testBuildClassOfQ() {
    TypeDescription expected = TypeDescription
        .fromString("struct<bigDecimal:decimal(38,10),"
            + "booleanValue:boolean,date:date,doubleValue:double,intValue:int,longValue:bigint,"
            + "string:string,timestamp:timestamp>");
    TypeDescription actual = build(DummyObject.class, new WriterMappingRegistry());
    assertEquals(expected, actual);
  }

  @Test
  public void testBuildClassOfQComparatorOfField() {
    TypeDescription expected = TypeDescription
        .fromString("struct<string:string,intValue:int>");
    TypeDescription actual = build(AnnotatedDummyObject.class, new WriterMappingRegistry());
    assertEquals(expected, actual);
  }
}
