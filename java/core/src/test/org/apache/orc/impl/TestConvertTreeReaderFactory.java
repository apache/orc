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
package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TestProlepticConversions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConvertTreeReaderFactory {

  private Path workDir =
      new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test" + File.separator + "tmp"));

  private Configuration conf;
  private FileSystem fs;
  private Path testFilePath;
  private int LARGE_BATCH_SIZE;

  @BeforeEach
  public void setupPath(TestInfo testInfo) throws Exception {
    // Default CV length is 1024
    this.LARGE_BATCH_SIZE = 1030;
    this.conf = new Configuration();
    this.fs = FileSystem.getLocal(conf);
    this.testFilePath = new Path(workDir, TestWriterImpl.class.getSimpleName() +
        testInfo.getTestMethod().get().getName().replaceFirst("\\[[0-9]+]", "") +
        ".orc");
    fs.delete(testFilePath, false);
  }

  public <TExpectedColumnVector extends ColumnVector> TExpectedColumnVector createORCFileWithLargeArray(
      TypeDescription schema, Class<TExpectedColumnVector> expectedColumnType, boolean useDecimal64)
      throws IOException, ParseException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    Writer w = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema));

    SimpleDateFormat dateFormat = TestProlepticConversions.createParser("yyyy-MM-dd", new GregorianCalendar());
    VectorizedRowBatch batch = schema.createRowBatch(
        useDecimal64 ? TypeDescription.RowBatchVersion.USE_DECIMAL64 : TypeDescription.RowBatchVersion.ORIGINAL,
        LARGE_BATCH_SIZE);

    ListColumnVector listCol = (ListColumnVector) batch.cols[0];
    TExpectedColumnVector dcv = (TExpectedColumnVector) (listCol).child;
    batch.size = 1;
    for (int row = 0; row < LARGE_BATCH_SIZE; ++row) {
      if (dcv instanceof DecimalColumnVector) {
        ((DecimalColumnVector) dcv).set(row, HiveDecimal.create(row * 2 + 1));
      } else if (dcv instanceof DoubleColumnVector) {
        ((DoubleColumnVector) dcv).vector[row] = row * 2 + 1;
      } else if (dcv instanceof BytesColumnVector) {
        ((BytesColumnVector) dcv).setVal(row, ((row * 2 + 1) + "").getBytes(StandardCharsets.UTF_8));
      } else if (dcv instanceof LongColumnVector) {
        ((LongColumnVector) dcv).vector[row] = row * 2 + 1;
      } else if (dcv instanceof TimestampColumnVector) {
        ((TimestampColumnVector) dcv).set(row, Timestamp.valueOf((1900 + row) + "-04-01 12:34:56.9"));
      } else if (dcv instanceof DateColumnVector) {
        String date = String.format("%04d-01-23", row * 2 + 1);
        ((DateColumnVector) dcv).vector[row] = TimeUnit.MILLISECONDS.toDays(dateFormat.parse(date).getTime());
      } else {
        throw new IllegalStateException("Writing File with a large array of "+ expectedColumnType + " is not supported!");
      }
    }

    listCol.childCount = 1;
    listCol.lengths[0] = LARGE_BATCH_SIZE;
    listCol.offsets[0] = 0;

    w.addRowBatch(batch);
    w.close();
    assertEquals(((ListColumnVector) batch.cols[0]).child.getClass(), expectedColumnType);
    return (TExpectedColumnVector) ((ListColumnVector) batch.cols[0]).child;
  }

  public <TExpectedColumnVector extends ColumnVector> TExpectedColumnVector readORCFileWithLargeArray(
      String typeString, Class<TExpectedColumnVector> expectedColumnType) throws Exception {
    Reader.Options options = new Reader.Options();
    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeString + ">>");
    options.schema(schema);
    String expected = options.toString();

    Configuration conf = new Configuration();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows(options);
    VectorizedRowBatch batch = schema.createRowBatchV2();
    while (rows.nextBatch(batch)) {
      assertTrue(batch.size > 0);
    }

    assertEquals(expected, options.toString());
    assertEquals(batch.cols.length, 1);
    assertTrue(batch.cols[0] instanceof ListColumnVector);
    assertEquals(((ListColumnVector) batch.cols[0]).child.getClass(), expectedColumnType);
    return (TExpectedColumnVector) ((ListColumnVector) batch.cols[0]).child;
  }

  public void testConvertToDecimal() throws Exception {
    Decimal64ColumnVector columnVector =
        readORCFileWithLargeArray("decimal(6,1)", Decimal64ColumnVector.class);
    assertEquals(LARGE_BATCH_SIZE, columnVector.vector.length);
  }

  public void testConvertToVarchar() throws Exception {
    BytesColumnVector columnVector = readORCFileWithLargeArray("varchar(10)", BytesColumnVector.class);
    assertEquals(LARGE_BATCH_SIZE, columnVector.vector.length);
  }

  public void testConvertToBinary() throws Exception {
    BytesColumnVector columnVector = readORCFileWithLargeArray("binary", BytesColumnVector.class);
    assertEquals(LARGE_BATCH_SIZE, columnVector.vector.length);
  }

  public void testConvertToDouble() throws Exception {
    DoubleColumnVector columnVector = readORCFileWithLargeArray("double", DoubleColumnVector.class);
    assertEquals(LARGE_BATCH_SIZE, columnVector.vector.length);
  }

  public void testConvertToInteger() throws Exception {
    LongColumnVector columnVector = readORCFileWithLargeArray("int", LongColumnVector.class);
    assertEquals(LARGE_BATCH_SIZE, columnVector.vector.length);
  }

  public void testConvertToFloat() throws Exception {
    DoubleColumnVector columnVector = readORCFileWithLargeArray("float", DoubleColumnVector.class);
    assertEquals(LARGE_BATCH_SIZE, columnVector.vector.length);
  }

  public void testConvertToTimestamp() throws Exception {
    TimestampColumnVector columnVector =
        readORCFileWithLargeArray("timestamp", TimestampColumnVector.class);
    assertEquals(LARGE_BATCH_SIZE, columnVector.time.length);
  }

  public void testConvertToDate() throws Exception {
    DateColumnVector columnVector = readORCFileWithLargeArray("date", DateColumnVector.class);
    assertEquals(LARGE_BATCH_SIZE, columnVector.vector.length);
  }

  @Test
  public void testDecimalArrayBiggerThanDefault() throws Exception {
    String typeStr = "decimal(6,1)";
    Class typeClass = DecimalColumnVector.class;

    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeStr + ">>");
    createORCFileWithLargeArray(schema, typeClass, typeClass.equals(Decimal64ColumnVector.class));
    try {
      // Test all possible conversions
      // check ConvertTreeReaderFactory.createDecimalConvertTreeReader
      testConvertToInteger();
      testConvertToDouble();
      testConvertToVarchar();
      testConvertToTimestamp();
      testConvertToDecimal();
    } finally {
      // Make sure we delete file across tests
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testDecimal64ArrayBiggerThanDefault() throws Exception {
    String typeStr = "decimal(6,1)";
    Class typeClass = Decimal64ColumnVector.class;

    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeStr + ">>");
    createORCFileWithLargeArray(schema, typeClass, typeClass.equals(Decimal64ColumnVector.class));
    try {
      // Test all possible conversions
      // check ConvertTreeReaderFactory.createDecimalConvertTreeReader
      testConvertToInteger();
      testConvertToDouble();
      testConvertToVarchar();
      testConvertToTimestamp();
      testConvertToDecimal();
    } finally {
      // Make sure we delete file across tests
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testStringArrayBiggerThanDefault() throws Exception {
    String typeStr = "varchar(10)";
    Class typeClass = BytesColumnVector.class;

    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeStr + ">>");
    createORCFileWithLargeArray(schema, typeClass, typeClass.equals(Decimal64ColumnVector.class));
    try {
      // Test all possible conversions
      // check ConvertTreeReaderFactory.createStringConvertTreeReader
      testConvertToInteger();
      testConvertToDouble();
      testConvertToDecimal();
      testConvertToVarchar();
      testConvertToBinary();
      testConvertToTimestamp();
      testConvertToDate();
    } finally {
      // Make sure we delete file across tests
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testBinaryArrayBiggerThanDefault() throws Exception {
    String typeStr = "binary";
    Class typeClass = BytesColumnVector.class;

    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeStr + ">>");
    createORCFileWithLargeArray(schema, typeClass, typeClass.equals(Decimal64ColumnVector.class));
    try {
      // Test all possible conversions
      // check ConvertTreeReaderFactory.createBinaryConvertTreeReader
      testConvertToVarchar();
    } finally {
      // Make sure we delete file across tests
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testDoubleArrayBiggerThanDefault() throws Exception {
    String typeStr = "double";
    Class typeClass = DoubleColumnVector.class;

    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeStr + ">>");
    createORCFileWithLargeArray(schema, typeClass, typeClass.equals(Decimal64ColumnVector.class));
    try {
      // Test all possible conversions
      // check ConvertTreeReaderFactory.createDoubleConvertTreeReader
      testConvertToDouble();
      testConvertToInteger();
      testConvertToFloat();
      testConvertToDecimal();
      testConvertToVarchar();
      testConvertToTimestamp();
    } finally {
      // Make sure we delete file across tests
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testIntArrayBiggerThanDefault() throws Exception {
    String typeStr = "int";
    Class typeClass = LongColumnVector.class;

    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeStr + ">>");
    createORCFileWithLargeArray(schema, typeClass, typeClass.equals(Decimal64ColumnVector.class));
    try {
      // Test all possible conversions
      // check ConvertTreeReaderFactory.createAnyIntegerConvertTreeReader
      testConvertToInteger();
      testConvertToDouble();
      testConvertToDecimal();
      testConvertToVarchar();
      testConvertToTimestamp();
    } finally {
      // Make sure we delete file across tests
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testTimestampArrayBiggerThanDefault() throws Exception {
    String typeStr = "timestamp";
    Class typeClass = TimestampColumnVector.class;

    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeStr + ">>");
    createORCFileWithLargeArray(schema, typeClass, typeClass.equals(Decimal64ColumnVector.class));
    try {
      // Test all possible conversions
      // check ConvertTreeReaderFactory.createTimestampConvertTreeReader
      testConvertToInteger();
      testConvertToDouble();
      testConvertToDecimal();
      testConvertToVarchar();
      testConvertToTimestamp();
      testConvertToDate();
    } finally {
      // Make sure we delete file across tests
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testDateArrayBiggerThanDefault() throws Exception {
    String typeStr = "date";
    Class typeClass = DateColumnVector.class;

    TypeDescription schema = TypeDescription.fromString("struct<col1:array<" + typeStr + ">>");
    createORCFileWithLargeArray(schema, typeClass, typeClass.equals(Decimal64ColumnVector.class));
    try {
      // Test all possible conversions
      // check ConvertTreeReaderFactory.createDateConvertTreeReader
      testConvertToVarchar();
      testConvertToTimestamp();
    } finally {
      // Make sure we delete file across tests
      fs.delete(testFilePath, false);
    }
  }
}
