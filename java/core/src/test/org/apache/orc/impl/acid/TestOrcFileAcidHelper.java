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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestOrcFileAcidHelper extends AcidTestBase {

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
    Path base = writeAcidOrcFile("base_20/bucket_0");

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
  }

  @Test
  public void readRowWithOpenTxnEnd() throws IOException {
    Path base = writeAcidOrcFile("base_20/bucket_0");

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
  }

  @Test
  public void readRowWithOpenTxnMiddle() throws IOException {
    Path base = writeAcidOrcFile("base_20/bucket_0");

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
  }
  // TODO test on valid file with deletes

}
