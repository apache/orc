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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
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
                   .withMaxLength(100))))
        .addField("tz", TypeDescription.createTimestampInstant())
        .addField("ts", TypeDescription.createTimestamp());
    String expectedStr =
        "struct<b1:binary,b2:boolean,b3:tinyint,c:char(10),d1:date," +
            "d2:decimal(20,5),d3:double,fff:float,int:int,l:array<bigint>," +
            "map:map<smallint,string>,str:struct<u:uniontype<timestamp," +
            "varchar(100)>>,tz:timestamp with local time zone,ts:timestamp>";
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
    type = TypeDescription.fromString("UNIONTYPE<   TIMESTAMP WITH LOCAL   TIME ZONE    >");
    assertEquals(TypeDescription.Category.UNION, type.getCategory());
    assertEquals(TypeDescription.Category.TIMESTAMP_INSTANT,
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

  @Test
  public void testFindSubtype() {
    TypeDescription type = TypeDescription.fromString(
        "struct<a:int," +
            "b:struct<c:array<int>,d:map<string,struct<e:string>>>," +
            "f:string," +
            "g:uniontype<string,int>>");
    assertEquals(0, type.findSubtype("0").getId());
    assertEquals(1, type.findSubtype("a").getId());
    assertEquals(2, type.findSubtype("b").getId());
    assertEquals(3, type.findSubtype("b.c").getId());
    assertEquals(4, type.findSubtype("b.c._elem").getId());
    assertEquals(5, type.findSubtype("b.d").getId());
    assertEquals(6, type.findSubtype("b.d._key").getId());
    assertEquals(7, type.findSubtype("b.d._value").getId());
    assertEquals(8, type.findSubtype("b.d._value.e").getId());
    assertEquals(9, type.findSubtype("f").getId());
    assertEquals(10, type.findSubtype("g").getId());
    assertEquals(11, type.findSubtype("g.0").getId());
    assertEquals(12, type.findSubtype("g.1").getId());
  }

  @Test
  public void testBadFindSubtype() {
    TypeDescription type = TypeDescription.fromString(
        "struct<a:int," +
            "b:struct<c:array<int>,d:map<string,struct<e:string>>>," +
            "f:string," +
            "g:uniontype<string,int>>");
    try {
      type.findSubtype("13");
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      // PASS
    }
    try {
      type.findSubtype("aa");
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      // PASS
    }
    try {
      type.findSubtype("b.a");
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      // PASS
    }
    try {
      type.findSubtype("g.2");
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      // PASS
    }
    try {
      type.findSubtype("b.c.d");
      assertTrue(false);
    } catch (IllegalArgumentException e) {
      // PASS
    }
  }

  @Test
  public void testFindSubtypes() {
    TypeDescription type = TypeDescription.fromString(
        "struct<a:int," +
            "b:struct<c:array<int>,d:map<string,struct<e:string>>>," +
            "f:string," +
            "g:uniontype<string,int>>");
    List<TypeDescription> results = type.findSubtypes("a");
    assertEquals(1, results.size());
    assertEquals(1, results.get(0).getId());

    results = type.findSubtypes("b.d._value.e,3,g.0");
    assertEquals(3, results.size());
    assertEquals(8, results.get(0).getId());
    assertEquals(3, results.get(1).getId());
    assertEquals(11, results.get(2).getId());

    results = type.findSubtypes("");
    assertEquals(0, results.size());
  }

  @Test
  public void testFindSubtypesAcid() {
    TypeDescription type = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<col0:int,col1:struct<z:int,x:double,y:string>," +
            "col2:double>>");
    List<TypeDescription> results = type.findSubtypes("col0");
    assertEquals(1, results.size());
    assertEquals(7, results.get(0).getId());

    results = type.findSubtypes("col1,col2,col1.x,col1.z");
    assertEquals(4, results.size());
    assertEquals(8, results.get(0).getId());
    assertEquals(12, results.get(1).getId());
    assertEquals(10, results.get(2).getId());
    assertEquals(9, results.get(3).getId());

    results = type.findSubtypes("");
    assertEquals(0, results.size());
  }

  @Test
  public void testAttributes() throws IOException {
    TypeDescription schema = TypeDescription.fromString(
        "struct<" +
            "name:struct<first:string,last:string>," +
            "address:struct<street:string,city:string,country:string,post_code:string>," +
            "credit_cards:array<struct<card_number:string,expire:date,ccv:string>>>");
    // set some attributes
    schema.findSubtype("name").setAttribute("iceberg.id", "12");
    schema.findSubtype("address.street").setAttribute("mask", "nullify")
        .setAttribute("context", "pii");

    TypeDescription clone = schema.clone();
    assertEquals("12", clone.findSubtype("name").getAttributeValue("iceberg.id"));
    clone.findSubtype("name").removeAttribute("iceberg.id");
    assertEquals(0, clone.findSubtype("name").getAttributeNames().size());
    assertEquals(1, schema.findSubtype("name").getAttributeNames().size());

    // write a file with those attributes
    Path path = new Path(System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp"), "attribute.orc");
    Configuration conf = new Configuration();
    Writer writer = OrcFile.createWriter(path,
        OrcFile.writerOptions(conf).setSchema(schema).overwrite(true));
    writer.close();

    // read the file back again
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    TypeDescription readerSchema = reader.getSchema();

    // make sure that the read types have the attributes
    TypeDescription nameCol = readerSchema.findSubtype("name");
    assertArrayEquals(new Object[]{"iceberg.id"},
        nameCol.getAttributeNames().toArray());
    assertEquals("12", nameCol.getAttributeValue("iceberg.id"));
    TypeDescription street = readerSchema.findSubtype("address.street");
    assertArrayEquals(new Object[]{"context", "mask"},
        street.getAttributeNames().toArray());
    assertEquals("pii", street.getAttributeValue("context"));
    assertEquals("nullify", street.getAttributeValue("mask"));
    assertEquals(null, street.getAttributeValue("foobar"));
  }

  static int clearAttributes(TypeDescription schema) {
    int result = 0;
    for(String attribute: schema.getAttributeNames()) {
      schema.removeAttribute(attribute);
      result += 1;
    }
    List<TypeDescription> children = schema.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        result += clearAttributes(child);
      }
    }
    return result;
  }

  @Test
  public void testEncryption() {
    String schemaString =  "struct<" +
        "name:struct<first:string,last:string>," +
        "address:struct<street:string,city:string,country:string,post_code:string>," +
        "credit_cards:array<struct<card_number:string,expire:date,ccv:string>>>";
    TypeDescription schema = TypeDescription.fromString(schemaString);
    TypeDescription copy = TypeDescription.fromString(schemaString);
    assertEquals(copy, schema);

    // set some encryption
    schema.annotateEncryption("pii:name,address.street;credit:credit_cards", null);
    assertEquals("pii",
        schema.findSubtype("name").getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE));
    assertEquals("pii",
        schema.findSubtype("address.street").getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE));
    assertEquals("credit",
        schema.findSubtype("credit_cards").getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE));
    assertNotEquals(copy, schema);
    assertEquals(3, clearAttributes(schema));
    assertEquals(copy, schema);

    schema.annotateEncryption("pii:name.first", "redact,Yy:name.first");
    // check that we ignore if already set
    schema.annotateEncryption("pii:name.first", "redact,Yy:name.first,credit_cards");
    assertEquals("pii",
        schema.findSubtype("name.first").getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE));
    assertEquals("redact,Yy",
        schema.findSubtype("name.first").getAttributeValue(TypeDescription.MASK_ATTRIBUTE));
    assertEquals("redact,Yy",
        schema.findSubtype("credit_cards").getAttributeValue(TypeDescription.MASK_ATTRIBUTE));
    assertEquals(3, clearAttributes(schema));

    schema.annotateEncryption("pii:name", "redact:name.first;nullify:name.last");
    assertEquals("pii",
        schema.findSubtype("name").getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE));
    assertEquals("redact",
        schema.findSubtype("name.first").getAttributeValue(TypeDescription.MASK_ATTRIBUTE));
    assertEquals("nullify",
        schema.findSubtype("name.last").getAttributeValue(TypeDescription.MASK_ATTRIBUTE));
    assertEquals(3, clearAttributes(schema));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncryptionConflict() {
    TypeDescription schema = TypeDescription.fromString(
        "struct<" +
            "name:struct<first:string,last:string>," +
            "address:struct<street:string,city:string,country:string,post_code:string>," +
            "credit_cards:array<struct<card_number:string,expire:date,ccv:string>>>");
    // set some encryption
    schema.annotateEncryption("pii:address,personal:address",null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaskConflict() {
    TypeDescription schema = TypeDescription.fromString(
        "struct<" +
            "name:struct<first:string,last:string>," +
            "address:struct<street:string,city:string,country:string,post_code:string>," +
            "credit_cards:array<struct<card_number:string,expire:date,ccv:string>>>");
    // set some encryption
    schema.annotateEncryption(null,"nullify:name;sha256:name");
  }
}
