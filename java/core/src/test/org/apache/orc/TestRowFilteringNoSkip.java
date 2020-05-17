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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;

/**
 * Types that are not skipped at row-level include: Long, Short, Int, Date, Binary
 * As it turns out it is more expensive to skip non-selected rows rather that just decode all and propagate the
 * selected array. Skipping for these type breaks instruction pipelining and introduces more branch misspredictions.
 */
public class TestRowFilteringNoSkip {

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
    testFilePath = new Path(workDir, "TestRowFilteringNoSkip." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testLongRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("int2", TypeDescription.createLong());

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
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        // We applied the given filter so selected is true
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Selected Arrays is propagated -- so size is never 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        // But since this Column type is not actually filtered there will be no nulls!
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] != 0)
            noNullCnt ++;
        }
      }
      // For Int type ColumnVector filtering does not remove any data!
      Assert.assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertEquals(col2.vector[0], 100);
      Assert.assertEquals(col2.vector[511], 999);
      Assert.assertEquals(col2.vector[1020],  100);
      Assert.assertEquals(col2.vector[1021], 999);
    }
  }

  @Test
  public void testIntRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("int2", TypeDescription.createInt());

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
        col1.vector[1023] = b;
        col2.vector[1023] = 101;
        for (int row = 0; row < batch.size-1; row++) {
          col1.vector[row] = 999;
          col2.vector[row] = row+1;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intFirstRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCount = 0;
      while (rows.nextBatch(batch)) {
        // We applied the given filter so selected is true
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Selected Arrays is propagated -- so size is never 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        // But since this Column type is not actually filtered there will be no nulls!
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] != 0)
            noNullCount++;


        }
      }
      // For Int type ColumnVector filtering does not remove any data!
      Assert.assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCount);
      // check filter-selected output
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(0, batch.selected[1]);
      Assert.assertEquals(0, batch.selected[1023]);
    }
  }

  @Test
  public void testShortRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("short2", TypeDescription.createShort());

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
            col2.vector[row] = row*2+1;
          else
            col2.vector[row] = -1 * row*2;
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
        // We applied the given filter so selected is true
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Selected Arrays is propagated -- so size is never 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        // But since this Column type is not actually filtered there will be no nulls!
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] != 0)
            noNullCnt ++;
        }
      }
      // For Short type ColumnVector filtering does not remove any data!
      Assert.assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCnt);
      Assert.assertEquals(false, col2.isRepeating);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.vector[0] > 0);
      Assert.assertTrue(col2.vector[511] < 0);
      Assert.assertTrue(col2.vector[1020] > 0);
      Assert.assertTrue(col2.vector[1021] < 0);
    }
  }

  @Test
  public void testDateRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("dt2", TypeDescription.createDate());

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
            col2.vector[row] = Timestamp.valueOf("2020-04-01 12:34:56.9").toInstant().getEpochSecond();
          else
            col2.vector[row] = Timestamp.valueOf("2019-04-01 12:34:56.9").toInstant().getEpochSecond();
        }
        col2.isRepeating = false;
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
        // We applied the given filter so selected is true
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Selected Arrays is propagated -- so size is never 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        // But since this Column type is not actually filtered there will be no nulls!
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] != 0)
            noNullCnt ++;
        }
      }
      // For Date type ColumnVector filtering does not remove any data!
      Assert.assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCnt);
      Assert.assertEquals(false, col2.isRepeating);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertTrue(col2.vector[0] != 0);
      Assert.assertTrue(col2.vector[511] != 0);
      Assert.assertTrue(col2.vector[1020] != 0);
      Assert.assertTrue(col2.vector[1021] != 0);
    }
  }

  @Test
  public void testBinaryRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("binary2", TypeDescription.createBinary());

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
          if ((row % 2) == 0)
            col2.setVal(row, TestVectorOrcFile.bytesArray(0, 1, 2, 3, row));
          else
            col2.setVal(row, TestVectorOrcFile.bytesArray(1, 2, 3, 4, row));
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
        // We applied the given filter so selected is true
        Assert.assertTrue(batch.selectedInUse);
        Assert.assertTrue(batch.selected != null);
        // Selected Arrays is propagated -- so size is never 1024
        Assert.assertTrue(batch.size != ColumnBatchRows);
        // But since this Column type is not actually filtered there will be no nulls!
        assertEquals( true, col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!TestVectorOrcFile.getBinary(col2, r).equals(TestVectorOrcFile.bytes()))
            noNullCnt ++;
        }
      }
      // For Binary type ColumnVector filtering does not remove any data!
      Assert.assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCnt);
      Assert.assertEquals(0, batch.selected[0]);
      Assert.assertEquals(2, batch.selected[1]);
      Assert.assertNotEquals(TestVectorOrcFile.getBinary(col2, 0), TestVectorOrcFile.bytes());
      Assert.assertNotEquals(TestVectorOrcFile.getBinary(col2, 511), TestVectorOrcFile.bytes());
      Assert.assertNotEquals(TestVectorOrcFile.getBinary(col2, 1020), TestVectorOrcFile.bytes());
      Assert.assertNotEquals(TestVectorOrcFile.getBinary(col2, 1021), TestVectorOrcFile.bytes());
    }
  }
}
