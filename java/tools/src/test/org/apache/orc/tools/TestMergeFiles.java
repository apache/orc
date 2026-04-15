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

package org.apache.orc.tools;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TestConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.tools.MergeFiles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMergeFiles implements TestConf {
  private Path workDir = new Path(
      Paths.get(System.getProperty("test.tmp.dir"), "orc-test-merge").toString());
  private FileSystem fs;
  private Path testFilePath;

  @BeforeEach
  public void openFileSystem() throws Exception {
    fs = FileSystem.getLocal(conf);
    fs.delete(workDir, true);
    fs.mkdirs(workDir);
    fs.deleteOnExit(workDir);
    testFilePath = new Path(workDir + File.separator + "TestMergeFiles.testMerge.orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testMerge() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");
    Map<String, Integer> fileToRowCountMap = new LinkedHashMap<>();
    fileToRowCountMap.put(workDir + File.separator + "test-merge-1.orc", 10000);
    fileToRowCountMap.put(workDir + File.separator + "test-merge-2.orc", 20000);
    for (Map.Entry<String, Integer> fileToRowCount : fileToRowCountMap.entrySet()) {
      Writer writer = OrcFile.createWriter(new Path(fileToRowCount.getKey()),
          OrcFile.writerOptions(conf)
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

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    // replace stdout and run command
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));
    MergeFiles.main(conf, new String[]{workDir.toString(),
        "--output", testFilePath.toString()});
    System.out.flush();
    System.setOut(origOut);
    String output = myOut.toString(StandardCharsets.UTF_8);
    System.out.println(output);
    assertTrue(output.contains("Input files size: 2, Merge files size: 2"));

    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf))) {
      assertEquals(schema, reader.getSchema());
      assertEquals(CompressionKind.ZSTD, reader.getCompressionKind());
      assertEquals(2, reader.getStripes().size());
      assertEquals(10000 + 20000, reader.getNumberOfRows());
    }
  }

  /**
   * Verifies that --maxSize splits input files into multiple part files under the output
   * directory. Three source files are created; a tight size threshold forces them to be
   * written into at least two part files.
   */
  @Test
  public void testMergeWithMaxSize() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");

    // Create 3 source ORC files with different row counts.
    String[] sourceNames = {
        workDir + File.separator + "ms-1.orc",
        workDir + File.separator + "ms-2.orc",
        workDir + File.separator + "ms-3.orc"
    };
    int[] rowCounts = {5000, 5000, 5000};
    for (int f = 0; f < sourceNames.length; f++) {
      Writer writer = OrcFile.createWriter(new Path(sourceNames[f]),
          OrcFile.writerOptions(conf).setSchema(schema));
      VectorizedRowBatch batch = schema.createRowBatch();
      LongColumnVector x = (LongColumnVector) batch.cols[0];
      BytesColumnVector y = (BytesColumnVector) batch.cols[1];
      for (int r = 0; r < rowCounts[f]; ++r) {
        int row = batch.size++;
        x.vector[row] = r;
        byte[] buffer = ("val-" + r).getBytes();
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

    // Measure the size of the first source file to compute a threshold that forces a split.
    long singleFileSize = fs.getFileStatus(new Path(sourceNames[0])).getLen();
    // Threshold: slightly larger than one file so at most one file fits per part.
    long maxSize = singleFileSize + 1;

    Path outputDir = new Path(workDir + File.separator + "merge-multi-out");
    fs.delete(outputDir, true);

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));
    MergeFiles.main(conf, new String[]{workDir.toString(),
        "--output", outputDir.toString(),
        "--maxSize", String.valueOf(maxSize)});
    System.out.flush();
    System.setOut(origOut);
    String output = myOut.toString(StandardCharsets.UTF_8);
    System.out.println(output);

    assertTrue(output.contains("Input files size: 3"), "Should report 3 input files");
    assertTrue(output.contains("Merge files size: 3"), "All 3 files should be merged");
    assertTrue(fs.isDirectory(outputDir), "Output directory should be created");

    // Verify that multiple part files were created and total row count is correct.
    long totalRows = 0;
    int partCount = 0;
    for (int i = 0; ; i++) {
      Path part = new Path(outputDir, String.format(MergeFiles.PART_FILE_FORMAT, i));
      if (!fs.exists(part)) {
        break;
      }
      partCount++;
      try (Reader reader = OrcFile.createReader(part, OrcFile.readerOptions(conf))) {
        totalRows += reader.getNumberOfRows();
      }
    }
    assertTrue(partCount > 1, "Expected more than one output part file, got: " + partCount);
    assertEquals(5000 + 5000 + 5000, totalRows, "Total row count across all parts should match");
  }
}
