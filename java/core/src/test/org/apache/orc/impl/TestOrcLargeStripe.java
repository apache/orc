/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestOrcLargeStripe {

  private static final long MB = 1024 * 1024;
  private Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
    + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  private Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Mock
  private FSDataInputStream mockDataInput;

  private DiskRangeList createRangeList(long... stripeSizes) {
    DiskRangeList.CreateHelper list = new DiskRangeList.CreateHelper();
    long prev = 0;
    for (long stripe : stripeSizes) {
      list.addOrMerge(prev, stripe, true, true);
      prev = stripe;
    }
    return list.extract();
  }

  private void verifyDiskRanges(long stripeLength, int expectedChunks) throws Exception {

    DiskRangeList rangeList = createRangeList(stripeLength);

    DiskRangeList newList = RecordReaderUtils.readDiskRanges(mockDataInput, null, 0, rangeList, false, (int) (2 * MB));
    assertEquals(expectedChunks, newList.listSize());

    newList = RecordReaderUtils.readDiskRanges(mockDataInput, null, 0, rangeList, true, (int) (2 * MB));
    assertEquals(expectedChunks, newList.listSize());

    HadoopShims.ZeroCopyReaderShim mockZcr = mock(HadoopShims.ZeroCopyReaderShim.class);
    when(mockZcr.readBuffer(anyInt(), anyBoolean())).thenReturn(ByteBuffer.allocate((int) (2 * MB)));
    newList = RecordReaderUtils.readDiskRanges(mockDataInput, mockZcr, 0, rangeList, true, (int) (2 * MB));
    assertEquals(expectedChunks, newList.listSize());
  }

  @Test
  public void testStripeSizesBelowAndGreaterThanLimit() throws Exception {
    verifyDiskRanges(MB, 1);
    verifyDiskRanges(5 * MB, 3);
  }


  @Test
  public void testConfigMaxChunkLimit() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    TypeDescription schema = TypeDescription.createTimestamp();
    testFilePath = new Path(workDir, "TestOrcLargeStripe." +
      testCaseName.getMethodName() + ".orc");
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000).bufferSize(10000)
        .version(OrcFile.Version.V_0_11).fileSystem(fs));
    writer.close();

    try {
      OrcFile.ReaderOptions opts = OrcFile.readerOptions(conf);
      Reader reader = OrcFile.createReader(testFilePath, opts);
      RecordReader recordReader = reader.rows(new Reader.Options().range(0L, Long.MAX_VALUE));
      assertTrue(recordReader instanceof RecordReaderImpl);
      assertEquals(Integer.MAX_VALUE - 1024, ((RecordReaderImpl) recordReader).getMaxDiskRangeChunkLimit());

      conf = new Configuration();
      conf.setInt(OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getHiveConfName(), 1000);
      opts = OrcFile.readerOptions(conf);
      reader = OrcFile.createReader(testFilePath, opts);
      recordReader = reader.rows(new Reader.Options().range(0L, Long.MAX_VALUE));
      assertTrue(recordReader instanceof RecordReaderImpl);
      assertEquals(1000, ((RecordReaderImpl) recordReader).getMaxDiskRangeChunkLimit());
    } finally {
      fs.delete(testFilePath, false);
    }
  }

  @Test
  public void testStringDirectGreaterThan2GB() throws IOException {
    final Runtime rt = Runtime.getRuntime();
    assumeTrue(rt.maxMemory() > 4_000_000_000L);
    TypeDescription schema = TypeDescription.createString();

    conf.setDouble("hive.exec.orc.dictionary.key.size.threshold", 0.0);
    Writer writer = OrcFile.createWriter(
      testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema)
        .compress(CompressionKind.NONE));
    // 5000 is the lower bound for a stripe
    int size = 5000;
    int width = 500_000;

    // generate a random string that is width characters long
    Random random = new Random(123);
    char[] randomChars= new char[width];
    int posn = 0;
    for(int length = 0; length < width && posn < randomChars.length; ++posn) {
      char cp = (char) random.nextInt(Character.MIN_SUPPLEMENTARY_CODE_POINT);
      // make sure we get a valid, non-surrogate
      while (Character.isSurrogate(cp)) {
        cp = (char) random.nextInt(Character.MIN_SUPPLEMENTARY_CODE_POINT);
      }
      // compute the length of the utf8
      length += cp < 0x80 ? 1 : (cp < 0x800 ? 2 : 3);
      randomChars[posn] = cp;
    }

    // put the random characters in as a repeating value.
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector string = (BytesColumnVector) batch.cols[0];
    string.setVal(0, new String(randomChars, 0, posn).getBytes(StandardCharsets.UTF_8));
    string.isRepeating = true;
    for(int rows=size; rows > 0; rows -= batch.size) {
      batch.size = Math.min(rows, batch.getMaxSize());
      writer.addRowBatch(batch);
    }
    writer.close();

    try {
      Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
      RecordReader rows = reader.rows();
      batch = reader.getSchema().createRowBatch();
      int rowsRead = 0;
      while (rows.nextBatch(batch)) {
        rowsRead += batch.size;
      }
      assertEquals(size, rowsRead);
    } finally {
      fs.delete(testFilePath, false);
    }
  }
}