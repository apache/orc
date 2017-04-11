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
package org.apache.orc;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class TestTypeDescription {
  @Rule
  public ExpectedException thrown= ExpectedException.none();

  @Test
  public void testJson() {
    TypeDescription bin = TypeDescription.createBinary();
    assertEquals("{\"category\": \"binary\", \"id\": 0, \"max\": 0}",
        bin.toJson());
    assertEquals("binary", bin.toString());
    TypeDescription struct = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal());
    assertEquals("struct<f1:int,f2:string,f3:decimal(38,10)>",
        struct.toString());
    assertEquals("{\"category\": \"struct\", \"id\": 0, \"max\": 3, \"fields\": [\n"
            + "  \"f1\": {\"category\": \"int\", \"id\": 1, \"max\": 1},\n"
            + "  \"f2\": {\"category\": \"string\", \"id\": 2, \"max\": 2},\n"
            + "  \"f3\": {\"category\": \"decimal\", \"id\": 3, \"max\": 3, \"precision\": 38, \"scale\": 10}]}",
        struct.toJson());
    struct = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    assertEquals("struct<f1:uniontype<tinyint,decimal(20,10)>,f2:struct<f3:date,f4:double,f5:boolean>,f6:char(100)>",
        struct.toString());
    assertEquals(
        "{\"category\": \"struct\", \"id\": 0, \"max\": 8, \"fields\": [\n" +
            "  \"f1\": {\"category\": \"uniontype\", \"id\": 1, \"max\": 3, \"children\": [\n" +
            "    {\"category\": \"tinyint\", \"id\": 2, \"max\": 2},\n" +
            "    {\"category\": \"decimal\", \"id\": 3, \"max\": 3, \"precision\": 20, \"scale\": 10}]},\n" +
            "  \"f2\": {\"category\": \"struct\", \"id\": 4, \"max\": 7, \"fields\": [\n" +
            "    \"f3\": {\"category\": \"date\", \"id\": 5, \"max\": 5},\n" +
            "    \"f4\": {\"category\": \"double\", \"id\": 6, \"max\": 6},\n" +
            "    \"f5\": {\"category\": \"boolean\", \"id\": 7, \"max\": 7}]},\n" +
            "  \"f6\": {\"category\": \"char\", \"id\": 8, \"max\": 8, \"length\": 100}]}",
        struct.toJson());
  }

  @Test
  public void testSpecialFieldNames() {
    TypeDescription type = TypeDescription.createStruct()
        .addField("foo bar", TypeDescription.createInt())
        .addField("`some`thing`", TypeDescription.createInt())
        .addField("èœ", TypeDescription.createInt())
        .addField("1234567890_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", TypeDescription.createInt())
        .addField("'!@#$%^&*()-=_+", TypeDescription.createInt());
    assertEquals("struct<`foo bar`:int,```some``thing```:int,`èœ`:int," +
        "1234567890_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ:int," +
        "`'!@#$%^&*()-=_+`:int>", type.toString());
  }

  @Test
  public void testParserSimple() {
    TypeDescription expected = TypeDescription.createStruct()
        .addField("b1", TypeDescription.createBinary())
        .addField("b2", TypeDescription.createBoolean())
        .addField("b3", TypeDescription.createByte())
        .addField("c", TypeDescription.createChar().withMaxLength(10))
        .addField("d1", TypeDescription.createDate())
        .addField("d2", TypeDescription.createDecimal().withScale(5).withPrecision(20))
        .addField("d3", TypeDescription.createDouble())
        .addField("fff", TypeDescription.createFloat())
        .addField("int", TypeDescription.createInt())
        .addField("l", TypeDescription.createList
            (TypeDescription.createLong()))
        .addField("map", TypeDescription.createMap
            (TypeDescription.createShort(), TypeDescription.createString()))
        .addField("str", TypeDescription.createStruct()
           .addField("u", TypeDescription.createUnion()
               .addUnionChild(TypeDescription.createTimestamp())
               .addUnionChild(TypeDescription.createVarchar()
                   .withMaxLength(100))));
    String expectedStr =
        "struct<b1:binary,b2:boolean,b3:tinyint,c:char(10),d1:date," +
            "d2:decimal(20,5),d3:double,fff:float,int:int,l:array<bigint>," +
            "map:map<smallint,string>,str:struct<u:uniontype<timestamp," +
            "varchar(100)>>>";
    assertEquals(expectedStr, expected.toString());
    TypeDescription actual = TypeDescription.fromString(expectedStr);
    assertEquals(expected, actual);
    assertEquals(expectedStr, actual.toString());
  }

  @Test
  public void testParserUpper() {
    TypeDescription type = TypeDescription.fromString("BIGINT");
    assertEquals(TypeDescription.Category.LONG, type.getCategory());
    type = TypeDescription.fromString("STRUCT<MY_FIELD:INT>");
    assertEquals(TypeDescription.Category.STRUCT, type.getCategory());
    assertEquals("MY_FIELD", type.getFieldNames().get(0));
    assertEquals(TypeDescription.Category.INT,
        type.getChildren().get(0).getCategory());
    type = TypeDescription.fromString("UNIONTYPE<STRING>");
    assertEquals(TypeDescription.Category.UNION, type.getCategory());
    assertEquals(TypeDescription.Category.STRING,
        type.getChildren().get(0).getCategory());
  }

  @Test
  public void testSpecialFieldNameParser() {
    TypeDescription type = TypeDescription.fromString("struct<`foo bar`:int," +
        "```quotes```:double,`abc``def````ghi`:float>");
    assertEquals(TypeDescription.Category.STRUCT, type.getCategory());
    List<String> fields = type.getFieldNames();
    assertEquals(3, fields.size());
    assertEquals("foo bar", fields.get(0));
    assertEquals("`quotes`", fields.get(1));
    assertEquals("abc`def``ghi", fields.get(2));
  }

  @Test
  public void testMissingField() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing name at 'struct<^'");
    TypeDescription.fromString("struct<");
  }

  @Test
  public void testQuotedField1() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unmatched quote at 'struct<^`abc'");
    TypeDescription.fromString("struct<`abc");
  }

  @Test
  public void testQuotedField2() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Empty quoted field name at 'struct<``^:int>'");
    TypeDescription.fromString("struct<``:int>");
  }

  @Test
  public void testParserUnknownCategory() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Can't parse category at 'FOOBAR^'");
    TypeDescription.fromString("FOOBAR");
  }

  @Test
  public void testParserEmptyCategory() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Can't parse category at '^<int>'");
    TypeDescription.fromString("<int>");
  }

  @Test
  public void testParserMissingInt() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing integer at 'char(^)'");
    TypeDescription.fromString("char()");
  }

  @Test
  public void testParserMissingSize() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Missing required char '(' at 'struct<c:char^>'");
    TypeDescription.fromString("struct<c:char>");
  }

  @Test
  public void testParserExtraStuff() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Extra characters at 'struct<i:int>^,'");
    TypeDescription.fromString("struct<i:int>,");
  }

  @Test
  public void testConnectedListSubtrees() {
    TypeDescription type =
        TypeDescription.fromString("struct<field1:array<struct<field2:int>>>");
    TypeDescription leaf = type.getChildren().get(0)
        .getChildren().get(0)
        .getChildren().get(0);
    assertEquals(3, leaf.getId());
    assertEquals(0, type.getId());
    assertEquals(3, leaf.getId());
  }

  @Test
  public void testConnectedMapSubtrees() {
    TypeDescription type =
        TypeDescription.fromString("struct<field1:map<string,int>>");
    TypeDescription leaf = type.getChildren().get(0).getChildren().get(0);
    assertEquals(2, leaf.getId());
    assertEquals(0, type.getId());
    assertEquals(2, leaf.getId());
  }
}
