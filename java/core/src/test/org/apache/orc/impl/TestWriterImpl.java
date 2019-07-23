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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestWriterImpl {

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  TypeDescription schema;

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("testWriterImpl.orc");
    fs.create(testFilePath, true);
    schema = TypeDescription.fromString("struct<x:int,y:int>");
  }

  @After
  public void deleteTestFile() throws Exception {
    fs.delete(testFilePath, false);
  }

  @Test(expected = IOException.class)
  public void testDefaultOverwriteFlagForWriter() throws Exception {
    // default value of the overwrite flag is false, so this should fail
    Writer w = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema));
    w.close();
  }

  @Test
  public void testOverriddenOverwriteFlagForWriter() throws Exception {
    // overriding the flag should result in a successful write (no exception)
    conf.set(OrcConf.OVERWRITE_OUTPUT_FILE.getAttribute(), "true");
    Writer w = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema));
    w.close();
  }

  @Test
  public void testNoBFIfNoIndex() throws Exception {
    // overriding the flag should result in a successful write (no exception)
    conf.set(OrcConf.OVERWRITE_OUTPUT_FILE.getAttribute(), "true");
    // Enable bloomfilter, but disable index
    conf.set(OrcConf.ROW_INDEX_STRIDE.getAttribute(), "0");
    conf.set(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), "*");
    Writer w = OrcFile.createWriter(testFilePath, OrcFile.writerOptions(conf).setSchema(schema));
    w.close();
  }
}
