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
package org.apache.orc.impl;

import static junit.framework.TestCase.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestSchemaEvolution {

  @Rule
  public TestName testCaseName = new TestName();

  Configuration conf;
  Reader.Options options;
  Path testFilePath;
  FileSystem fs;
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    options = new Reader.Options(conf);
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testDataTypeConversion1() throws IOException {
    TypeDescription fileStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution same1 = new SchemaEvolution(fileStruct1, null, options);
    assertFalse(same1.hasConversion());
    TypeDescription readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1 = new SchemaEvolution(fileStruct1, readerStruct1, options);
    assertFalse(both1.hasConversion());
    TypeDescription readerStruct1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1diff = new SchemaEvolution(fileStruct1, readerStruct1diff, options);
    assertTrue(both1diff.hasConversion());
    TypeDescription readerStruct1diffPrecision = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(12).withScale(10));
    SchemaEvolution both1diffPrecision = new SchemaEvolution(fileStruct1,
        readerStruct1diffPrecision, options);
    assertTrue(both1diffPrecision.hasConversion());
  }

  @Test
  public void testDataTypeConversion2() throws IOException {
    TypeDescription fileStruct2 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution same2 = new SchemaEvolution(fileStruct2, null, options);
    assertFalse(same2.hasConversion());
    TypeDescription readerStruct2 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution both2 = new SchemaEvolution(fileStruct2, readerStruct2, options);
    assertFalse(both2.hasConversion());
    TypeDescription readerStruct2diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createByte()))
        .addField("f6", TypeDescription.createChar().withMaxLength(100));
    SchemaEvolution both2diff = new SchemaEvolution(fileStruct2, readerStruct2diff, options);
    assertTrue(both2diff.hasConversion());
    TypeDescription readerStruct2diffChar = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createByte())
            .addUnionChild(TypeDescription.createDecimal()
                .withPrecision(20).withScale(10)))
        .addField("f2", TypeDescription.createStruct()
            .addField("f3", TypeDescription.createDate())
            .addField("f4", TypeDescription.createDouble())
            .addField("f5", TypeDescription.createBoolean()))
        .addField("f6", TypeDescription.createChar().withMaxLength(80));
    SchemaEvolution both2diffChar = new SchemaEvolution(fileStruct2, readerStruct2diffChar, options);
    assertTrue(both2diffChar.hasConversion());
  }

  @Test
  public void testFloatToDoubleEvolution() throws Exception {
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    TypeDescription schema = TypeDescription.createFloat();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000));
    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    DoubleColumnVector dcv = new DoubleColumnVector(1024);
    batch.cols[0] = dcv;
    batch.reset();
    batch.size = 1;
    dcv.vector[0] = 74.72f;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schemaOnRead = TypeDescription.createDouble();
    RecordReader rows = reader.rows(reader.options().schema(schemaOnRead));
    batch = schemaOnRead.createRowBatch();
    rows.nextBatch(batch);
    assertEquals(74.72, ((DoubleColumnVector) batch.cols[0]).vector[0], 0.00000000001);
    rows.close();
  }

  @Test
  public void testSafePpdEvaluation() throws IOException {
    TypeDescription fileStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution same1 = new SchemaEvolution(fileStruct1, null, options);
    assertTrue(same1.isPPDSafeConversion(0));
    assertFalse(same1.hasConversion());
    TypeDescription readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1 = new SchemaEvolution(fileStruct1, readerStruct1, options);
    assertFalse(both1.hasConversion());
    assertTrue(both1.isPPDSafeConversion(0));
    assertTrue(both1.isPPDSafeConversion(1));
    assertTrue(both1.isPPDSafeConversion(2));
    assertTrue(both1.isPPDSafeConversion(3));

    // int -> long
    TypeDescription readerStruct1diff = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10));
    SchemaEvolution both1diff = new SchemaEvolution(fileStruct1, readerStruct1diff, options);
    assertTrue(both1diff.hasConversion());
    assertFalse(both1diff.isPPDSafeConversion(0));
    assertTrue(both1diff.isPPDSafeConversion(1));
    assertTrue(both1diff.isPPDSafeConversion(2));
    assertTrue(both1diff.isPPDSafeConversion(3));

    // decimal(38,10) -> decimal(12, 10)
    TypeDescription readerStruct1diffPrecision = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(12).withScale(10));
    options.include(new boolean[] {true, false, false, true});
    SchemaEvolution both1diffPrecision = new SchemaEvolution(fileStruct1,
        readerStruct1diffPrecision, options);
    assertTrue(both1diffPrecision.hasConversion());
    assertFalse(both1diffPrecision.isPPDSafeConversion(0));
    assertFalse(both1diffPrecision.isPPDSafeConversion(1)); // column not included
    assertFalse(both1diffPrecision.isPPDSafeConversion(2)); // column not included
    assertFalse(both1diffPrecision.isPPDSafeConversion(3));

    // add columns
    readerStruct1 = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt())
        .addField("f2", TypeDescription.createString())
        .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(10))
        .addField("f4", TypeDescription.createBoolean());
    options.include(null);
    both1 = new SchemaEvolution(fileStruct1, readerStruct1, options);
    assertTrue(both1.hasConversion());
    assertFalse(both1.isPPDSafeConversion(0));
    assertTrue(both1.isPPDSafeConversion(1));
    assertTrue(both1.isPPDSafeConversion(2));
    assertTrue(both1.isPPDSafeConversion(3));
    assertFalse(both1.isPPDSafeConversion(4));
  }

  @Test
  public void testSafePpdEvaluationForInts() throws IOException {
    // byte -> short -> int -> long
    TypeDescription fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    SchemaEvolution schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertFalse(schemaEvolution.hasConversion());

    // byte -> short
    TypeDescription readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // byte -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // byte -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // short -> int -> long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion short -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // short -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // short -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // int -> long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion int -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion int -> short
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // int -> long
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // long
    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createLong());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // unsafe conversion long -> byte
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createByte());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion long -> short
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createShort());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // unsafe conversion long -> int
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createFloat());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createTimestamp());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));
  }

  @Test
  public void testSafePpdEvaluationForStrings() throws IOException {
    TypeDescription fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    SchemaEvolution schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // string -> char
    TypeDescription readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // string -> varchar
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // char -> string
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // char -> varchar
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    fileSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createVarchar());
    schemaEvolution = new SchemaEvolution(fileSchema, null, options);
    assertTrue(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.hasConversion());

    // varchar -> string
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createString());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertTrue(schemaEvolution.isPPDSafeConversion(1));

    // varchar -> char
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createChar());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createDecimal());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createDate());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));

    // invalid
    readerSchema = TypeDescription.createStruct()
        .addField("f1", TypeDescription.createInt());
    schemaEvolution = new SchemaEvolution(fileSchema, readerSchema, options);
    assertTrue(schemaEvolution.hasConversion());
    assertFalse(schemaEvolution.isPPDSafeConversion(0));
    assertFalse(schemaEvolution.isPPDSafeConversion(1));
  }

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));

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
    options.tolerateMissingSchema(false);
    SchemaEvolution transition =
        new SchemaEvolution(fileType, readerType, options.include(included));
  }

  @Test
  public void testAcidNamedEvolution() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<x:int,z:bigint,y:string>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<x:int,y:string,z:bigint>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertTrue(evo.isAcid());
    assertEquals("struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<x:int,y:string,z:bigint>>", evo.getReaderSchema().toString());
    assertEquals("struct<x:int,y:string,z:bigint>",
        evo.getReaderBaseSchema().toString());
    // the first stuff should be an identity
    for(int c=0; c < 8; ++c) {
      assertEquals("column " + c, c, evo.getFileType(c).getId());
    }
    // y and z should swap places
    assertEquals(9, evo.getFileType(8).getId());
    assertEquals(8, evo.getFileType(9).getId());
  }

  @Test
  public void testAcidPositionEvolutionAddField() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<_col0:int,_col1:string>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<x:int,y:string,z:bigint>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertTrue(evo.isAcid());
    assertEquals("struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<x:int,y:string,z:bigint>>", evo.getReaderSchema().toString());
    assertEquals("struct<x:int,y:string,z:bigint>",
        evo.getReaderBaseSchema().toString());
    // the first stuff should be an identity
    for(int c=0; c < 9; ++c) {
      assertEquals("column " + c, c, evo.getFileType(c).getId());
    }
    // the file doesn't have z
    assertEquals(null, evo.getFileType(9));
  }

  @Test
  public void testAcidPositionEvolutionRemoveField() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<_col0:int,_col1:string,_col2:double>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<x:int,y:string>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertTrue(evo.isAcid());
    assertEquals("struct<operation:int,originalTransaction:bigint,bucket:int," +
        "rowId:bigint,currentTransaction:bigint," +
        "row:struct<x:int,y:string>>", evo.getReaderSchema().toString());
    assertEquals("struct<x:int,y:string>",
        evo.getReaderBaseSchema().toString());
    // the first stuff should be an identity
    boolean[] fileInclude = evo.getFileIncluded();
    for(int c=0; c < 9; ++c) {
      assertEquals("column " + c, c, evo.getFileType(c).getId());
      assertTrue("column " + c, fileInclude[c]);
    }
    // don't read the last column
    assertFalse(fileInclude[9]);
  }

  @Test
  public void testAcidPositionSubstructure() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<operation:int,originalTransaction:bigint,bucket:int," +
            "rowId:bigint,currentTransaction:bigint," +
            "row:struct<_col0:int,_col1:struct<z:int,x:double,y:string>," +
            "_col2:double>>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<a:int,b:struct<x:double,y:string,z:int>,c:double>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertTrue(evo.isAcid());
    // the first stuff should be an identity
    boolean[] fileInclude = evo.getFileIncluded();
    for(int c=0; c < 9; ++c) {
      assertEquals("column " + c, c, evo.getFileType(c).getId());
    }
    assertEquals(10, evo.getFileType(9).getId());
    assertEquals(11, evo.getFileType(10).getId());
    assertEquals(9, evo.getFileType(11).getId());
    assertEquals(12, evo.getFileType(12).getId());
    assertEquals(13, fileInclude.length);
    for(int c=0; c < fileInclude.length; ++c) {
      assertTrue("column " + c, fileInclude[c]);
    }
  }

  @Test
  public void testNonAcidPositionSubstructure() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<_col0:int,_col1:struct<x:double,z:int>," +
            "_col2:double>");
    TypeDescription readerType = TypeDescription.fromString(
        "struct<a:int,b:struct<x:double,y:string,z:int>,c:double>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readerType, options);
    assertFalse(evo.isAcid());
    // the first stuff should be an identity
    boolean[] fileInclude = evo.getFileIncluded();
    assertEquals(0, evo.getFileType(0).getId());
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(3, evo.getFileType(3).getId());
    assertEquals(null, evo.getFileType(4));
    assertEquals(4, evo.getFileType(5).getId());
    assertEquals(5, evo.getFileType(6).getId());
    assertEquals(6, fileInclude.length);
    for(int c=0; c < fileInclude.length; ++c) {
      assertTrue("column " + c, fileInclude[c]);
    }
  }

  @Test
  public void testFileIncludeWithNoEvolution() {
    TypeDescription fileType = TypeDescription.fromString(
        "struct<a:int,b:double,c:string>");
    SchemaEvolution evo = new SchemaEvolution(fileType, null,
        options.include(new boolean[]{true, false, true, false}));
    assertFalse(evo.isAcid());
    assertEquals("struct<a:int,b:double,c:string>",
        evo.getReaderBaseSchema().toString());
    boolean[] fileInclude = evo.getFileIncluded();
    assertTrue(fileInclude[0]);
    assertFalse(fileInclude[1]);
    assertTrue(fileInclude[2]);
    assertFalse(fileInclude[3]);
  }

  static void createStream(Map<StreamName, InStream> streams,
                           int id,
                           OrcProto.Stream.Kind kind,
                           int... values) throws IOException {
    StreamName name = new StreamName(id, kind);
    List<DiskRange> ranges = new ArrayList<>();
    byte[] buffer = new byte[values.length];
    for(int i=0; i < values.length; ++i) {
      buffer[i] = (byte) values[i];
    }
    ranges.add(new BufferChunk(ByteBuffer.wrap(buffer), 0));
    streams.put(name, InStream.create(name.toString(), ranges, values.length, null,
        values.length));
  }

  @Test
  public void testTypeConversion() throws IOException {
    TypeDescription fileType = TypeDescription.fromString("struct<x:int,y:string>");
    TypeDescription readType = TypeDescription.fromString("struct<z:int,y:string,x:bigint>");
    SchemaEvolution evo = new SchemaEvolution(fileType, readType, options);

    // check to make sure the fields are mapped correctly
    assertEquals(null, evo.getFileType(1));
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(1, evo.getFileType(3).getId());

    TreeReaderFactory.Context treeContext =
        new TreeReaderFactory.ReaderContext().setSchemaEvolution(evo);
    TreeReaderFactory.TreeReader reader =
        TreeReaderFactory.createTreeReader(readType, treeContext);

    // check to make sure the tree reader is built right
    assertEquals(TreeReaderFactory.StructTreeReader.class, reader.getClass());
    TreeReaderFactory.TreeReader[] children =
        ((TreeReaderFactory.StructTreeReader) reader).getChildReaders();
    assertEquals(3, children.length);
    assertEquals(TreeReaderFactory.NullTreeReader.class, children[0].getClass());
    assertEquals(TreeReaderFactory.StringTreeReader.class, children[1].getClass());
    assertEquals(ConvertTreeReaderFactory.AnyIntegerFromAnyIntegerTreeReader.class,
        children[2].getClass());

    // check to make sure the data is read correctly
    OrcProto.StripeFooter.Builder footer = OrcProto.StripeFooter.newBuilder();
    OrcProto.ColumnEncoding DIRECT =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();
    footer.addColumns(DIRECT);
    footer.addColumns(DIRECT);
    footer.addColumns(DIRECT);
    Map<StreamName, InStream> streams = new HashMap<>();
    createStream(streams, 1, OrcProto.Stream.Kind.DATA, 7, 1, 0);
    createStream(streams, 2, OrcProto.Stream.Kind.DATA,
        65, 66, 67, 68, 69, 70, 71, 72, 73, 74);
    createStream(streams, 2, OrcProto.Stream.Kind.LENGTH, 7, 0, 1);
    reader.startStripe(streams, footer.build());
    VectorizedRowBatch batch = readType.createRowBatch();
    reader.nextBatch(batch, 10);
    final String EXPECTED = "ABCDEFGHIJ";
    assertEquals(true, batch.cols[0].isRepeating);
    assertEquals(true, batch.cols[0].isNull[0]);
    for(int r=0; r < 10; ++r) {
      assertEquals("col1." + r, EXPECTED.substring(r, r+1),
          ((BytesColumnVector) batch.cols[1]).toString(r));
      assertEquals("col2." + r, r,
          ((LongColumnVector) batch.cols[2]).vector[r]);
    }
  }

  @Test
  public void testPositionalEvolution() throws IOException {
    options.forcePositionalEvolution(true);
    TypeDescription file = TypeDescription.fromString("struct<x:int,y:int,z:int>");
    TypeDescription read = TypeDescription.fromString("struct<z:int,x:int,a:int,b:int>");
    SchemaEvolution evo = new SchemaEvolution(file, read, options);
    assertEquals(1, evo.getFileType(1).getId());
    assertEquals(2, evo.getFileType(2).getId());
    assertEquals(3, evo.getFileType(3).getId());
    assertEquals(null, evo.getFileType(4));
  }
}
