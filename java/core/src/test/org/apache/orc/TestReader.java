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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestReader {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, TestReader.class.getSimpleName() + "." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testReadZeroLengthFile() throws Exception {
    FSDataOutputStream fout = fs.create(testFilePath);
    fout.close();
    assertEquals(0, fs.getFileStatus(testFilePath).getLen());
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(0, reader.getNumberOfRows());
  }

  @Test(expected=FileFormatException.class)
  public void testReadFileLengthLessThanMagic() throws Exception {
    FSDataOutputStream fout = fs.create(testFilePath);
    fout.writeBoolean(true);
    fout.close();
    assertEquals(1, fs.getFileStatus(testFilePath).getLen());
    OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
  }

  @Test(expected=FileFormatException.class)
  public void testReadFileInvalidHeader() throws Exception {
    FSDataOutputStream fout = fs.create(testFilePath);
    fout.writeLong(1);
    fout.close();
    assertEquals(8, fs.getFileStatus(testFilePath).getLen());
    OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
  }
}
