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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    // Create 3 source ORC files.
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

    long firstTwo = fs.getFileStatus(new Path(sourceNames[0])).getLen()
        + fs.getFileStatus(new Path(sourceNames[1])).getLen();
    long maxSize = firstTwo + 1;

    Path outputDir = new Path(workDir + File.separator + "merge-multi-out");
    fs.delete(outputDir, true);

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));
    try {
      MergeFiles.main(conf, new String[]{workDir.toString(),
          "--output", outputDir.toString(),
          "--maxSize", String.valueOf(maxSize)});
      System.out.flush();
    } finally {
      System.setOut(origOut);
    }
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
    assertEquals(2, partCount, "Expected exactly two output part files, got: " + partCount);
    assertEquals(5000 + 5000 + 5000, totalRows, "Total row count across all parts should match");
  }

  /**
   * A single input file that is larger than --maxSize must still be emitted as its
   * own part file (we never split an input).
   */
  @Test
  public void testMergeWithMaxSizeSingleGiantFile() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");

    Path inputDir = new Path(workDir, "giant-in");
    fs.mkdirs(inputDir);
    Path giant = new Path(inputDir, "giant.orc");
    writeOrcFile(giant, schema, 20000);
    long giantSize = fs.getFileStatus(giant).getLen();
    assertTrue(giantSize > 0, "Giant source file should have non-zero size");

    Path outputDir = new Path(workDir, "giant-out");
    fs.delete(outputDir, true);

    long maxSize = Math.max(1L, giantSize / 2);

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));
    try {
      MergeFiles.main(conf, new String[]{inputDir.toString(),
          "--output", outputDir.toString(),
          "--maxSize", String.valueOf(maxSize)});
      System.out.flush();
    } finally {
      System.setOut(origOut);
    }
    String output = myOut.toString(StandardCharsets.UTF_8);
    System.out.println(output);

    assertTrue(output.contains("Input files size: 1"), "Should report 1 input file");
    assertTrue(output.contains("Merge files size: 1"), "The giant file should be merged");

    Path part0 = new Path(outputDir, String.format(MergeFiles.PART_FILE_FORMAT, 0));
    Path part1 = new Path(outputDir, String.format(MergeFiles.PART_FILE_FORMAT, 1));
    assertTrue(fs.exists(part0), "Expected part-00000.orc");
    assertFalse(fs.exists(part1), "Expected exactly one part file for a single input");
    try (Reader reader = OrcFile.createReader(part0, OrcFile.readerOptions(conf))) {
      assertEquals(20000, reader.getNumberOfRows(),
          "Single giant file must be preserved intact in its own part");
    }
  }

  /**
   * By default running the merge against a non-empty output directory must fail so
   * that existing data is not silently destroyed. With --overwrite the directory's
   * contents are deleted before new part files are written.
   */
  @Test
  public void testMergeWithMaxSizeOverwriteBehavior() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");

    Path inputDir = new Path(workDir, "ow-in");
    fs.mkdirs(inputDir);
    writeOrcFile(new Path(inputDir, "a.orc"), schema, 100);

    Path outputDir = new Path(workDir, "ow-out");
    fs.delete(outputDir, true);
    fs.mkdirs(outputDir);
    Path stale = new Path(outputDir, "stale.txt");
    fs.create(stale).close();

    String[] baseArgs = {inputDir.toString(),
        "--output", outputDir.toString(),
        "--maxSize", "1048576"};

    // Without --overwrite: should reject non-empty output directory and leave it untouched.
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> MergeFiles.main(conf, baseArgs));
    assertTrue(ex.getMessage().contains("not empty") &&
            ex.getMessage().contains("--overwrite"),
        "Error should mention --overwrite: " + ex.getMessage());
    assertTrue(fs.exists(stale),
        "Existing content must be preserved when overwrite is refused");

    // With --overwrite: existing content is cleared and fresh part files are written.
    String[] owArgs = new String[baseArgs.length + 1];
    System.arraycopy(baseArgs, 0, owArgs, 0, baseArgs.length);
    owArgs[baseArgs.length] = "--overwrite";

    PrintStream origErr = System.err;
    ByteArrayOutputStream myErr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(myErr, false, StandardCharsets.UTF_8));
    try {
      MergeFiles.main(conf, owArgs);
      System.err.flush();
    } finally {
      System.setErr(origErr);
    }
    String errOut = myErr.toString(StandardCharsets.UTF_8);
    assertTrue(errOut.contains("Overwriting existing non-empty output directory"),
        "Expected an overwrite warning on stderr, got:\n" + errOut);
    assertFalse(fs.exists(stale), "Pre-existing file should be cleared by --overwrite");

    Path part0 = new Path(outputDir, String.format(MergeFiles.PART_FILE_FORMAT, 0));
    assertTrue(fs.exists(part0), "Expected part-00000.orc after overwrite");
    try (Reader reader = OrcFile.createReader(part0, OrcFile.readerOptions(conf))) {
      assertEquals(100, reader.getNumberOfRows());
    }
  }

  /**
   * Creates an ORC file at the given path with {@code rowCount} rows of the fixed
   * {@code struct<x:int,y:string>} schema used across these tests.
   */
  private void writeOrcFile(Path path, TypeDescription schema, int rowCount) throws Exception {
    Writer writer = OrcFile.createWriter(path,
        OrcFile.writerOptions(conf).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector x = (LongColumnVector) batch.cols[0];
    BytesColumnVector y = (BytesColumnVector) batch.cols[1];
    for (int r = 0; r < rowCount; ++r) {
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

  /**
   * Two-level nested partitions (d=.../h=01, h=02, h=03). With --preserveStructure,
   * each leaf partition must be merged independently and its relative path mirrored
   * under the output root. Data MUST NOT be cross-mixed between leaves.
   */
  @Test
  public void testPreserveStructureTwoLevelPartitions() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");

    Path inputRoot = new Path(workDir, "ps-in");
    Path partitionBase = new Path(inputRoot, "d=2025-04-25");

    int[] hRows = {100, 200, 300};
    for (int h = 0; h < hRows.length; h++) {
      Path leaf = new Path(partitionBase, String.format("h=%02d", h + 1));
      fs.mkdirs(leaf);
      writeOrcFile(new Path(leaf, "a.orc"), schema, hRows[h]);
      writeOrcFile(new Path(leaf, "b.orc"), schema, hRows[h] / 2);
    }

    Path outputRoot = new Path(workDir, "ps-out");
    fs.delete(outputRoot, true);

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));
    try {
      MergeFiles.main(conf, new String[]{inputRoot.toString(),
          "--output", outputRoot.toString(),
          "--preserveStructure"});
      System.out.flush();
    } finally {
      System.setOut(origOut);
    }
    String output = myOut.toString(StandardCharsets.UTF_8);
    System.out.println(output);

    assertTrue(output.contains("Leaves: 3"),
        "Expected 3 leaves in summary, got:\n" + output);

    for (int h = 0; h < hRows.length; h++) {
      Path outLeaf = new Path(outputRoot, "d=2025-04-25/" + String.format("h=%02d", h + 1));
      assertTrue(fs.isDirectory(outLeaf),
          "Expected mirrored leaf directory to exist: " + outLeaf);
      Path part0 = new Path(outLeaf, String.format(MergeFiles.PART_FILE_FORMAT, 0));
      assertTrue(fs.exists(part0),
          "Expected part-00000.orc under leaf: " + outLeaf);
      long expectedRows = hRows[h] + hRows[h] / 2;
      try (Reader reader = OrcFile.createReader(part0, OrcFile.readerOptions(conf))) {
        assertEquals(expectedRows, reader.getNumberOfRows(),
            "Row count mismatch for " + outLeaf);
        assertEquals(schema, reader.getSchema());
      }
    }
  }

  /**
   * A directory containing BOTH ORC files and subdirectories is ambiguous under
   * --preserveStructure and must be rejected before any output is written.
   */
  @Test
  public void testPreserveStructureRejectsMixedDirectory() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");

    Path inputRoot = new Path(workDir, "psmix-in");
    fs.mkdirs(inputRoot);
    writeOrcFile(new Path(inputRoot, "stray.orc"), schema, 10);
    Path subLeaf = new Path(inputRoot, "h=01");
    fs.mkdirs(subLeaf);
    writeOrcFile(new Path(subLeaf, "a.orc"), schema, 20);

    Path outputRoot = new Path(workDir, "psmix-out");
    fs.delete(outputRoot, true);

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> MergeFiles.main(conf, new String[]{inputRoot.toString(),
            "--output", outputRoot.toString(),
            "--preserveStructure"}));
    assertTrue(ex.getMessage().contains("both ORC files and subdirectories"),
        "Unexpected error message: " + ex.getMessage());
  }

  /**
   * Hidden files/directories (names starting with '_' or '.') must always be
   * ignored, so files like _SUCCESS or _temporary/*.orc don't pollute the merged
   * output.
   */
  @Test
  public void testPreserveStructureSkipsHidden() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");

    Path inputRoot = new Path(workDir, "pshidden-in");
    Path leaf = new Path(inputRoot, "h=01");
    fs.mkdirs(leaf);
    writeOrcFile(new Path(leaf, "data.orc"), schema, 30);
    // _SUCCESS-style hidden file next to the real data — should be ignored by default.
    writeOrcFile(new Path(leaf, "_hidden.orc"), schema, 999);
    // Hidden sibling directory (e.g. _temporary) whose contents would otherwise
    // appear as a second leaf — should be skipped entirely.
    Path hiddenDir = new Path(inputRoot, "_temporary");
    fs.mkdirs(hiddenDir);
    writeOrcFile(new Path(hiddenDir, "a.orc"), schema, 7);

    Path outputRoot = new Path(workDir, "pshidden-out");
    fs.delete(outputRoot, true);

    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));
    try {
      MergeFiles.main(conf, new String[]{inputRoot.toString(),
          "--output", outputRoot.toString(),
          "--preserveStructure"});
      System.out.flush();
    } finally {
      System.setOut(origOut);
    }
    String output = myOut.toString(StandardCharsets.UTF_8);
    System.out.println(output);

    assertTrue(output.contains("Leaves: 1"),
        "Expected exactly 1 leaf (hidden dir ignored), got:\n" + output);
    assertFalse(fs.exists(new Path(outputRoot, "_temporary")),
        "Hidden input directory should not be mirrored to output");

    Path part0 = new Path(outputRoot, "h=01/" + String.format(MergeFiles.PART_FILE_FORMAT, 0));
    assertTrue(fs.exists(part0));
    try (Reader reader = OrcFile.createReader(part0, OrcFile.readerOptions(conf))) {
      assertEquals(30, reader.getNumberOfRows(),
          "Hidden _hidden.orc rows must not be merged in");
    }
  }
}
