/*
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
package org.apache.orc.impl.acid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestOrcFileAcidHelper extends AcidTestBase {

  private static long[] oneBucketZero;
  private static long[] twoBucketZero;

  @BeforeClass
  public static void setupBucketArrays() {
    oneBucketZero = new long[1];
    oneBucketZero[0] = BucketCodec.V1.encode(0, 0);

    twoBucketZero = new long[2];
    twoBucketZero[0] = twoBucketZero[1] = BucketCodec.V1.encode(0, 0);
  }

  @After
  public void clearDeleteSetCache() {
    DeleteSetCache.dumpCache();
  }

  // test no transaction info set
  @Test(expected = IOException.class)
  public void txnInfoNotSet() throws IOException {
    FileStatus base = createFile("base_10/bucket_1");

    Reader reader =
        OrcFile.createReaderForAcidFile(base.getPath(), new OrcFile.ReaderOptions(conf));
  }

  // Test that reading a file that is no longer valid (e.g. an old delta) produces a null reader.
  // This test also explicitly does pre-parse the directory to force it to happen in the read.
  @Test
  public void readInvalidFile() throws IOException {
    FileStatus oldBase = createFile("base_10/bucket_1");
    createFile("base_20/bucket_1");

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"))
        .filesystem(fs);

    Reader reader = OrcFile.createReaderForAcidFile(oldBase.getPath(), options);

    Assert.assertEquals(0, reader.getNumberOfRows());
    Assert.assertFalse(reader.rows().nextBatch(null));
  }

  // Test that reading a file where all records are valid works.  This also pre-parses the
  // directory and hands the parsed info to the reader
  @Test
  public void readAllRowsValid() throws IOException {
    Path base = multiTxnInsertFile("base_20/bucket_0");

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(5, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertFalse(batch.selectedInUse);
    Assert.assertEquals("mary had a little lamb", col.toString(0));
    Assert.assertEquals("its fleece was white as snow", col.toString(1));
    Assert.assertEquals("and everywhere that mary went", col.toString(2));
    Assert.assertEquals("the lamb was sure to go", col.toString(3));
    Assert.assertTrue(col.isNull[4]);
    rows.close();
  }

  @Test
  public void readRowWithOpenTxnEnd() throws IOException {
    Path base = multiTxnInsertFile("base_20/bucket_0");

    ValidReadTxnList validTxns = new ValidReadTxnList("21:" + Long.MAX_VALUE + ":3");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(3, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("mary had a little lamb", col.toString(batch.selected[0]));
    Assert.assertEquals("its fleece was white as snow", col.toString(batch.selected[1]));
    Assert.assertEquals("and everywhere that mary went", col.toString(batch.selected[2]));
    rows.close();
  }

  @Test
  public void readRowWithOpenTxnMiddle() throws IOException {
    Path base = multiTxnInsertFile("base_20/bucket_0");

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":2");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(3, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("mary had a little lamb", col.toString(batch.selected[0]));
    Assert.assertEquals("the lamb was sure to go", col.toString(batch.selected[1]));
    Assert.assertTrue(col.isNull[batch.selected[2]]);
    rows.close();
  }

  // Test reading of single txn input file
  @Test
  public void readAllRowsSingleTxn() throws IOException {
    Path base = singleTxnInsertFile("base_20/bucket_0");

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(5, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertFalse(batch.selectedInUse);
    Assert.assertEquals("hickery dickery dock", col.toString(0));
    Assert.assertEquals("the mouse ran up the clock", col.toString(1));
    Assert.assertEquals("the clock struck one", col.toString(2));
    Assert.assertEquals("down the mouse did run", col.toString(3));
    Assert.assertEquals("hickery dickery dock.", col.toString(4));
    rows.close();
  }

  // Test reading of single txn input file
  @Test
  public void singleTxnAllOpen() throws IOException {
    Path base = singleTxnInsertFile("base_20/bucket_0");

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":10");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(0, batch.size);
    rows.close();
  }

  @Test
  public void deleteOneTxn() throws IOException {
    Path base = multiTxnInsertFile("base_20/bucket_0");
    deleteDeltaFile("delete_delta_21_21/bucket_0", new long[]{1}, oneBucketZero, new long[]{0},
        new long[]{21});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(4, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("its fleece was white as snow", col.toString(batch.selected[0]));
    Assert.assertEquals("and everywhere that mary went", col.toString(batch.selected[1]));
    Assert.assertEquals("the lamb was sure to go", col.toString(batch.selected[2]));
    Assert.assertTrue(col.isNull[batch.selected[3]]);
    rows.close();
  }

  @Test
  public void singleTxnAllDeleted() throws IOException {
    Path base = singleTxnInsertFile("base_20/bucket_0");
    deleteDeltaFile("delete_delta_21_21/bucket_0", new long[]{10, 10}, twoBucketZero,
        new long[]{0, 1}, new long[]{21, 21});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(3, batch.size);
    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("the clock struck one", col.toString(batch.selected[0]));
    Assert.assertEquals("down the mouse did run", col.toString(batch.selected[1]));
    Assert.assertEquals("hickery dickery dock.", col.toString(batch.selected[2]));
    rows.close();
  }

  @Test
  public void deleteMultipleTxnsOneDeleteDelta() throws IOException {
    Path base = multiTxnInsertFile("base_20/bucket_0");
    deleteDeltaFile("delete_delta_21_21/bucket_0", new long[]{1, 2}, twoBucketZero,
        new long[]{0, 2}, new long[]{21, 21});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(3, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("its fleece was white as snow", col.toString(batch.selected[0]));
    Assert.assertEquals("the lamb was sure to go", col.toString(batch.selected[1]));
    Assert.assertTrue(col.isNull[batch.selected[2]]);
    rows.close();
  }

  @Test
  public void deleteMultipleTxnsMultipleDeleteDelta() throws IOException {
    Path base = multiTxnInsertFile("base_20/bucket_0");
    deleteDeltaFile("delete_delta_21_21/bucket_0", new long[]{1}, oneBucketZero, new long[]{0},
        new long[]{21});
    deleteDeltaFile("delete_delta_22_22/bucket_0", new long[]{2}, oneBucketZero, new long[]{2},
        new long[]{22});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(3, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("its fleece was white as snow", col.toString(batch.selected[0]));
    Assert.assertEquals("the lamb was sure to go", col.toString(batch.selected[1]));
    Assert.assertTrue(col.isNull[batch.selected[2]]);
    rows.close();
  }

  // Test that open delete txns are ignored
  @Test
  public void openDeleteTxn() throws IOException {
    Path base = multiTxnInsertFile("base_20/bucket_0");
    deleteDeltaFile("delete_delta_21_22/bucket_0", new long[]{1, 2}, twoBucketZero,
        new long[]{0, 1}, new long[]{21, 22});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":22");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(4, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("its fleece was white as snow", col.toString(batch.selected[0]));
    Assert.assertEquals("and everywhere that mary went", col.toString(batch.selected[1]));
    Assert.assertEquals("the lamb was sure to go", col.toString(batch.selected[2]));
    Assert.assertTrue(col.isNull[batch.selected[3]]);
    rows.close();
  }

  @Test
  public void statementIdIgnoredInDelete() throws IOException {
    long[] buckets = new long[] {
        BucketCodec.V1.encode(2, 0),
        BucketCodec.V1.encode(2, 0),
    };
    Path base = insertFile("delta_1_1_2/bucket_0", new long[] {1, 1}, buckets,
        new String[] {"abc", "def"});
    deleteDeltaFile("delete_delta_21_21/bucket_0", new long[]{1}, oneBucketZero,
        new long[]{0}, new long[]{21});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(1, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("def", col.toString(batch.selected[0]));
    rows.close();
  }

  @Test
  public void bucketHandledInDelete() throws IOException {
    long[] buckets = new long[] {
        BucketCodec.V1.encode(0, 0),
        BucketCodec.V1.encode(0, 1),
        };
    // Second record should not be deleted because bucketId won't match
    Path base = insertFile("base_20/bucket_0", new long[] {1, 1}, buckets, new String[]{"abc", "def"});
    deleteDeltaFile("delete_delta_21_21/bucket_0", new long[]{1, 1}, twoBucketZero,
        new long[]{0, 1}, new long[]{21, 21});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(conf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(1, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("def", col.toString(batch.selected[0]));
    rows.close();
  }

  @Test
  public void testSortedDeleteSet() throws IOException {
    Configuration myConf = new Configuration(conf);
    OrcConf.MAX_DELETE_ENTRIES_IN_MEMORY.setLong(myConf, 1);
    Path base = multiTxnInsertFile("base_20/bucket_0");
    deleteDeltaFile("delete_delta_21_21/bucket_0", new long[]{1}, oneBucketZero, new long[]{0},
        new long[]{21});
    deleteDeltaFile("delete_delta_22_22/bucket_0", new long[]{2}, oneBucketZero, new long[]{2},
        new long[]{22});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, myConf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(myConf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(3, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("its fleece was white as snow", col.toString(batch.selected[0]));
    Assert.assertEquals("the lamb was sure to go", col.toString(batch.selected[1]));
    Assert.assertTrue(col.isNull[batch.selected[2]]);
    rows.close();
  }

  @Test
  public void testSortedDeleteSetMultipleRows() throws IOException {
    Configuration myConf = new Configuration(conf);
    OrcConf.MAX_DELETE_ENTRIES_IN_MEMORY.setLong(myConf, 1);
    Path base = multiTxnInsertFile("base_20/bucket_0");
    deleteDeltaFile("delete_delta_21_21/bucket_0", new long[]{1,3}, twoBucketZero, new long[]{0,3},
        new long[]{21,21});
    deleteDeltaFile("delete_delta_22_22/bucket_0", new long[]{2,3}, twoBucketZero, new long[]{2,4},
        new long[]{22,22});

    ValidReadTxnList validTxns = new ValidReadTxnList("100:" + Long.MAX_VALUE + ":");
    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, myConf, validTxns);

    OrcFile.ReaderOptions options = new OrcFile.ReaderOptions(myConf);
    options.validTxnList(validTxns)
        .filesystem(fs)
        .acidDir(dir);

    Reader reader = OrcFile.createReaderForAcidFile(base, options);
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    Assert.assertTrue(rows.nextBatch(batch));
    Assert.assertEquals(1, batch.size);

    BytesColumnVector col =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];

    Assert.assertTrue(batch.selectedInUse);
    Assert.assertEquals("its fleece was white as snow", col.toString(batch.selected[0]));
    rows.close();
  }
  // TODO test file with SARGS

  // TODO test with projection pushdown

  // TODO test with schema evolution


}
