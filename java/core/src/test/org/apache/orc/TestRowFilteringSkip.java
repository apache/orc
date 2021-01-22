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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;

import static org.junit.Assert.assertEquals;

/**
 * Types that are skipped at row-level include: Decimal, Decimal64, Double, Float, Char, VarChar, String, Boolean, Timestamp
 * For the remaining types that are not row-skipped see {@link TestRowFilteringNoSkip}
 */
public class TestRowFilteringSkip {

  private Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
      + File.separator + "tmp"));

  private Configuration conf;
  private FileSystem fs;
  private Path testFilePath;

  private static final int ColumnBatchRows = 1024;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestRowFilteringSkip." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  public static String convertTime(long time){
    Date date = new Date(time);
    Format format = new SimpleDateFormat("yyyy-MM-d HH:mm:ss.SSS");
    return format.format(date);
  }

  // Filter all rows except: 924 and 940
  public static void intAnyRowFilter(VectorizedRowBatch batch) {
    // Dummy Filter implementation passing just one Batch row
    int newSize = 2;
    batch.selected[0] = batch.size-100;
    batch.selected[1] = 940;
    batch.selectedInUse = true;
    batch.size = newSize;
  }

  // Filter all rows except the first one
  public static void intFirstRowFilter(VectorizedRowBatch batch) {
    int newSize = 0;
    for (int row = 0; row <batch.size; ++row) {
      if (row == 0) {
        batch.selected[newSize++] = row;
      }
    }
    batch.selectedInUse = true;
    batch.size = newSize;
  }

  // Filter out rows in a round-robbin fashion starting with a pass
  public static void intRoundRobbinRowFilter(VectorizedRowBatch batch) {
    int newSize = 0;
    for (int row = 0; row < batch.size; ++row) {
      if ((row % 2) == 0) {
        batch.selected[newSize++] = row;
      }
    }
    batch.selectedInUse = true;
    batch.size = newSize;
  }

  static int rowCount = 0;
  public static void intCustomValueFilter(VectorizedRowBatch batch) {
    LongColumnVector col1 = (LongColumnVector) batch.cols[0];
    int newSize = 0;
    for (int row = 0; row <batch.size; ++row) {
      long val = col1.vector[row];
      if ((val == 2) || (val == 5) || (val == 13) || (val == 29) || (val == 70)) {
        batch.selected[newSize++] = row;
      }
      rowCount++;
    }
    batch.selectedInUse = true;
    batch.size = newSize;
  }

  @Test
  public void testDecimalRepeatingFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal());

    HiveDecimalWritable passDataVal = new HiveDecimalWritable("100");
    HiveDecimalWritable nullDataVal = new HiveDecimalWritable("0");

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          col2.vector[row] = passDataVal;
        }
        col1.isRepeating = true;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r].compareTo(passDataVal) == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertEquals(col2.vector[0], passDataVal);
      Assert.assertEquals(col2.vector[511], nullDataVal);
      Assert.assertEquals(col2.vector[1020],  passDataVal);
      Assert.assertEquals(col2.vector[1021], nullDataVal);
    }
  }

  @Test
  public void testDecimalRoundRobbinFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal());

    HiveDecimalWritable failDataVal = new HiveDecimalWritable("-100");
    HiveDecimalWritable nullDataVal = new HiveDecimalWritable("0");

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = new HiveDecimalWritable(row+1);
          else
            col2.vector[row] = failDataVal;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r].getHiveDecimal().longValue() > 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertEquals(col2.vector[0].getHiveDecimal().longValue(), 1);
      Assert.assertEquals(col2.vector[511], nullDataVal);
      Assert.assertEquals(col2.vector[1020].getHiveDecimal().longValue(),  1021);
      Assert.assertEquals(col2.vector[1021], nullDataVal);
    }
  }

  @Test
  public void testDecimalNullRoundRobbinFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal());

    HiveDecimalWritable nullDataVal = new HiveDecimalWritable("0");

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = new HiveDecimalWritable(row+1);
        }
        // Make sure we trigger the nullCount path of DecimalTreeReader
        col2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r].getHiveDecimal().longValue() > 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertEquals(col2.vector[0].getHiveDecimal().longValue(), 1);
      Assert.assertEquals(col2.vector[511], nullDataVal);
      Assert.assertEquals(col2.vector[1020].getHiveDecimal().longValue(),  1021);
      Assert.assertEquals(col2.vector[1021], nullDataVal);
    }
  }


  @Test
  public void testMultiDecimalSingleFilterCallback() throws Exception {
    /// Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal())
        .addField("decimal2", TypeDescription.createDecimal());

    HiveDecimalWritable passDataVal = new HiveDecimalWritable("12");
    HiveDecimalWritable failDataVal = new HiveDecimalWritable("100");
    HiveDecimalWritable nullDataVal = new HiveDecimalWritable("0");

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      // Write 50 batches where each batch has a single value for str.
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      DecimalColumnVector col3 = (DecimalColumnVector) batch.cols[2];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if (row == 924 || row == 940) {
            col2.vector[row] = passDataVal;
            col3.vector[row] = passDataVal;
          } else {
            col2.vector[row] = failDataVal;
            col3.vector[row] = failDataVal;
          }
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intAnyRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      DecimalColumnVector col3 = (DecimalColumnVector) batch.cols[2];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r].compareTo(passDataVal) == 0 && col3.vector[r].compareTo(passDataVal) == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 2, noNullCnt);
      Assert.assertEquals(924, batch.selected[0]);
      Assert.assertEquals(940, batch.selected[1]);
      Assert.assertEquals(0, batch.selected[2]);
      Assert.assertEquals(col2.vector[0],  nullDataVal);
      Assert.assertEquals(col3.vector[0],  nullDataVal);
      Assert.assertEquals(col2.vector[511], nullDataVal);
      Assert.assertEquals(col3.vector[511], nullDataVal);
      Assert.assertEquals(col2.vector[924], passDataVal);
      Assert.assertEquals(col3.vector[940], passDataVal);
      Assert.assertEquals(col2.vector[1020], nullDataVal);
      Assert.assertEquals(col3.vector[1020], nullDataVal);
      Assert.assertEquals(col2.vector[1021], nullDataVal);
      Assert.assertEquals(col3.vector[1021], nullDataVal);
    }
  }

  @Test
  public void testDecimal64RoundRobbinFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal().withPrecision(10).withScale(2));

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = row + 1;
          else
            col2.vector[row] = -1 * row;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] != 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertEquals(col2.vector[0], 1);
      Assert.assertEquals(col2.vector[511], 0);
      Assert.assertEquals(col2.vector[1020],  1021);
      Assert.assertEquals(col2.vector[1021], 0);
    }
  }

  @Test
  public void testDecimal64NullRoundRobbinFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal().withPrecision(10).withScale(2));

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = row + 1;
        }
        col2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertEquals(col2.vector[0], 1);
      Assert.assertEquals(col2.vector[511], 0);
      Assert.assertEquals(col2.vector[1020],  1021);
      Assert.assertEquals(col2.vector[1021], 0);
    }
  }

  @Test
  public void testDoubleRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("double2", TypeDescription.createDouble());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DoubleColumnVector col2 = (DoubleColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) ==0 )
            col2.vector[row] = 100;
          else
            col2.vector[row] = 999;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DoubleColumnVector col2 = (DoubleColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 100)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.vector[0] == 100.0);
      Assert.assertTrue(col2.vector[511] == 0.0);
      Assert.assertTrue(col2.vector[1020] == 100);
      Assert.assertTrue(col2.vector[1021] == 0);
    }
  }

  @Test
  public void testFloatRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("float2", TypeDescription.createFloat());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DoubleColumnVector col2 = (DoubleColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) ==0 )
            col2.vector[row] = 100+row;
          else
            col2.vector[row] = 999;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DoubleColumnVector col2 = (DoubleColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] != 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.vector[0] != 999.0);
      Assert.assertTrue(col2.vector[511] == 0.0);
      Assert.assertTrue(col2.vector[1020] == 1120.0);
      Assert.assertTrue(col2.vector[1021] == 0);
    }
  }

  @Test
  public void testCharRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("char2", TypeDescription.createChar());

    byte[] passData = ("p").getBytes(StandardCharsets.UTF_8);
    byte[] failData = ("f").getBytes(StandardCharsets.UTF_8);

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.setVal(row, passData);
          else
            col2.setVal(row, failData);
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!col2.toString(r).isEmpty())
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.toString(0).equals("p"));
      Assert.assertTrue(col2.toString(511).isEmpty());
      Assert.assertTrue(col2.toString(1020).equals("p"));
      Assert.assertTrue(col2.toString(1021).isEmpty());
    }
  }

  @Test
  public void testVarCharRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("varchar2", TypeDescription.createVarchar());

    byte[] passData = ("p").getBytes(StandardCharsets.UTF_8);
    byte[] failData = ("f").getBytes(StandardCharsets.UTF_8);

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.setVal(row, passData);
          else
            col2.setVal(row, failData);
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!col2.toString(r).isEmpty())
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.toString(0).equals("p"));
      Assert.assertTrue(col2.toString(511).isEmpty());
      Assert.assertTrue(col2.toString(1020).equals("p"));
      Assert.assertTrue(col2.toString(1021).isEmpty());
    }
  }

  @Test
  public void testDirectStringRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 10 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("string1", TypeDescription.createString());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      // Write 50 batches where each batch has a single value for str.
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) ==0 )
            col2.setVal(row, ("passData-" + row).getBytes(StandardCharsets.UTF_8));
          else
            col2.setVal(row, ("failData-" + row).getBytes(StandardCharsets.UTF_8));
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!col2.toString(r).isEmpty())
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.toString(0).startsWith("pass"));
      Assert.assertTrue(col2.toString(511).isEmpty());
      Assert.assertTrue(col2.toString(1020).startsWith("pass"));
    }
  }

  @Test
  public void testDictionaryStringRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 100 * ColumnBatchRows;
    final int NUM_BATCHES = 100;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("string1", TypeDescription.createString());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if (row % 2 ==0)
            col2.setVal(row, ("passData").getBytes(StandardCharsets.UTF_8));
          else
            col2.setVal(row, ("failData").getBytes(StandardCharsets.UTF_8));
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!col2.toString(r).isEmpty())
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.toString(0).startsWith("pass"));
      Assert.assertTrue(col2.toString(511).isEmpty());
      Assert.assertTrue(col2.toString(1020).startsWith("pass"));
    }
  }

  @Test
  public void testRepeatingBooleanRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("bool2", TypeDescription.createBoolean());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          col2.vector[row] = 0;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCnt);
      Assert.assertEquals(false, col2.isRepeating);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.vector[0] == 0);
      Assert.assertTrue(col2.vector[511] == 0);
      Assert.assertTrue(col2.vector[1020] == 0);
      Assert.assertTrue(col2.vector[1021] == 0);
    }
  }

  @Test
  public void testBooleanRoundRobbinRowFilterCallback() throws Exception {
    final int INDEX_STRIDE = 0;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("bool2", TypeDescription.createBoolean());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = 1;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.vector[0] == 1);
      Assert.assertTrue(col2.vector[511] == 0);
      Assert.assertTrue(col2.vector[1020] == 1);
      Assert.assertTrue(col2.vector[1021] == 0);
    }
  }

  @Test
  public void testBooleanAnyRowFilterCallback() throws Exception {
    final int INDEX_STRIDE = 0;
    final int NUM_BATCHES = 100;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("bool2", TypeDescription.createBoolean());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if (row == 924 || row == 940)
            col2.vector[row] = 1;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intAnyRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 1)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 2, noNullCnt);
      Assert.assertEquals(924, batch.selected[0]);
      Assert.assertEquals(940, batch.selected[1]);
      Assert.assertTrue(col2.vector[0] == 0);
      Assert.assertTrue(col2.vector[511] == 0);
      Assert.assertTrue(col2.vector[1020] == 0);
      Assert.assertTrue(col2.vector[924] == 1);
      Assert.assertTrue(col2.vector[940] == 1);
    }
  }

  @Test
  public void testTimestampRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("ts2", TypeDescription.createTimestamp());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.set(row, Timestamp.valueOf((1900+row)+"-04-01 12:34:56.9"));
          else {
            col2.isNull[row] = true;
            col2.set(row, null);
          }
        }
        col1.isRepeating = true;
        col1.noNulls = false;
        col2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.getTime(r) == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      Assert.assertEquals(NUM_BATCHES * 512, noNullCnt);
      Assert.assertEquals(false, col2.isRepeating);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(convertTime(col2.getTime(0)).compareTo("1900-04-1 12:34:56.900") == 0);
      Assert.assertTrue(col2.getTime(511) == 0);
      Assert.assertTrue(convertTime(col2.getTime(1020)).compareTo("2920-04-1 12:34:56.900") == 0);
      Assert.assertTrue(col2.getTime(1021) == 0);
    }
  }

  @Test
  public void testSchemaEvolutionMissingColumn() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    TypeDescription fileSchema = TypeDescription.createStruct()
      .addField("int1", TypeDescription.createInt())
      .addField("ts2", TypeDescription.createTimestamp());

    try (Writer writer = OrcFile.createWriter(testFilePath,
                                              OrcFile.writerOptions(conf)
                                                .setSchema(fileSchema)
                                                .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = fileSchema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.set(row, Timestamp.valueOf((1900+row)+"-04-01 12:34:56.9"));
          else {
            col2.isNull[row] = true;
            col2.set(row, null);
          }
        }
        col1.isRepeating = true;
        col1.noNulls = false;
        col2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    TypeDescription readSchema = fileSchema
      .clone()
      .addField("missing", TypeDescription.createInt());
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
      reader.options()
        .schema(readSchema)
        .setRowFilter(new String[]{"missing"}, TestRowFilteringSkip::notNullFilterMissing))) {
      VectorizedRowBatch batch = readSchema.createRowBatchV2();

      long rowCount = 0;
      while (rows.nextBatch(batch)) {
        // All rows are selected as NullTreeReader does not support filters
        Assert.assertFalse(batch.selectedInUse);
        rowCount += batch.size;
      }
      Assert.assertEquals(reader.getNumberOfRows(), rowCount);
    }
  }

  private static void notNullFilterMissing(VectorizedRowBatch batch) {
    int selIdx = 0;
    for (int i = 0; i < batch.size; i++) {
      if (!batch.cols[2].isNull[i]) {
        batch.selected[selIdx++] = i;
      }
    }
    batch.selectedInUse = true;
    batch.size = selIdx;
  }

  @Test
  public void testcustomFileTimestampRoundRobbinRowFilterCallback() throws Exception {
    testFilePath = new Path(getClass().getClassLoader().
        getSystemResource("orc_split_elim.orc").getPath());

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"userid"}, TestRowFilteringSkip::intCustomValueFilter))) {

      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col5 = (TimestampColumnVector) batch.cols[4];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Rows are filtered so it should never be 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col1.vector[r] != 100)
            noNullCnt ++;
        }
      }

      // Total rows of the file should be 25k
      Assert.assertEquals(25000, rowCount);
      // Make sure that our filter worked ( 5 rows with userId != 100)
      Assert.assertEquals(5, noNullCnt);
      Assert.assertEquals(false, col5.isRepeating);
      Assert.assertEquals(544, batch.selected[0]);
      Assert.assertEquals(0, batch.selected[1]);
      Assert.assertTrue(col5.getTime(0) == 0);
      Assert.assertTrue(col5.getTime(544) != 0);
    }
  }
}