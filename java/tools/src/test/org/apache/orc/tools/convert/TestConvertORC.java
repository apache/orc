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

package org.apache.orc.tools.convert;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConvertORC {

  private Path workDir = new Path(
      Paths.get(System.getProperty("test.tmp.dir"), "orc-test-convert-orc").toString());
  private Configuration conf;
  private FileSystem fs;
  private Path testFilePath;

  @BeforeEach
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    fs.mkdirs(workDir);
    fs.deleteOnExit(workDir);
    testFilePath = new Path("TestConvertORC.testConvertORC.orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testConvertFromORC() throws IOException, ParseException {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");
    Map<String, Integer> fileToRowCountMap = new LinkedHashMap<>();
    fileToRowCountMap.put("test-convert-1.orc", 10000);
    fileToRowCountMap.put("test-convert-2.orc", 20000);
    Map<String, CompressionKind> fileToCompressMap = new HashMap<>();
    fileToCompressMap.put("test-convert-1.orc", CompressionKind.ZLIB);
    fileToCompressMap.put("test-convert-2.orc", CompressionKind.SNAPPY);

    for (Map.Entry<String, Integer> fileToRowCount : fileToRowCountMap.entrySet()) {
      Writer writer = OrcFile.createWriter(new Path(fileToRowCount.getKey()),
          OrcFile.writerOptions(conf)
              .compress(fileToCompressMap.get(fileToRowCount.getKey()))
              .setSchema(schema));
      VectorizedRowBatch batch = schema.createRowBatch();
      LongColumnVector x = (LongColumnVector) batch.cols[0];
      BytesColumnVector y = (BytesColumnVector) batch.cols[1];
      for (int r = 0; r < fileToRowCount.getValue(); ++r) {
        int row = batch.size++;
        x.vector[row] = r;
        byte[] buffer = ("byte-" + r).getBytes();
        y.setRef(row, buffer, 0, buffer.length);
        if (batch.size == batch.getMaxSize()) {
          writer.addRowBatch(batch);
          batch.reset();
        }
      }
      if (batch.size != 0) {
        writer.addRowBatch(batch);
      }
      writer.close();
    }

    try (Reader reader = OrcFile.createReader(new Path("test-convert-1.orc"), OrcFile.readerOptions(conf))) {
      assertEquals(schema, reader.getSchema());
      assertEquals(CompressionKind.ZLIB, reader.getCompressionKind());
      assertEquals(10000, reader.getNumberOfRows());
    }

    try (Reader reader = OrcFile.createReader(new Path("test-convert-2.orc"), OrcFile.readerOptions(conf))) {
      assertEquals(schema, reader.getSchema());
      assertEquals(CompressionKind.SNAPPY, reader.getCompressionKind());
      assertEquals(20000, reader.getNumberOfRows());
    }

    ConvertTool.main(conf, new String[]{"-o", testFilePath.toString(),
        "test-convert-1.orc", "test-convert-2.orc"});

    assertTrue(fs.exists(testFilePath));
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf))) {
      assertEquals(schema, reader.getSchema());
      assertEquals(CompressionKind.ZSTD, reader.getCompressionKind());
      assertEquals(10000 + 20000, reader.getNumberOfRows());
    }
  }
}
