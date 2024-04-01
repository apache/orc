/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.tools.CheckTool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCheckTool {
  private Path workDir = new Path(System.getProperty("test.tmp.dir"));
  private Configuration conf;
  private FileSystem fs;
  private Path testFilePath;

  @BeforeEach
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestCheckTool.testCheckTool.orc");
    fs.delete(testFilePath, false);
    createFile();
  }

  private void createFile() throws IOException {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string,z:string>");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .bloomFilterColumns("x,y")
            .rowIndexStride(5000)
            .setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector x = (LongColumnVector) batch.cols[0];
    BytesColumnVector y = (BytesColumnVector) batch.cols[1];
    BytesColumnVector z = (BytesColumnVector) batch.cols[2];
    for (int r = 0; r < 10000; ++r) {
      int row = batch.size++;
      x.vector[row] = r;
      byte[] yBuffer = ("y-byte-" + r).getBytes();
      byte[] zBuffer = ("z-byte-" + r).getBytes();
      y.setRef(row, yBuffer, 0, yBuffer.length);
      z.setRef(row, zBuffer, 0, zBuffer.length);
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

  @Test
  public void testPredicate() throws Exception {
    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));

    CheckTool.main(conf, new String[]{
        "--type", "predicate",
        "--values", "0", "--values", "5566",
        "--column", "x",
        testFilePath.toString()});

    CheckTool.main(conf, new String[]{
        "--type", "predicate",
        "--values", "y-byte-1234", "--values", "y-byte-5566",
        "--column", "y",
        testFilePath.toString()});

    CheckTool.main(conf, new String[]{
        "--type", "predicate",
        "--values", "z-byte-1234", "--values", "z-byte-5566",
        "--column", "z",
        testFilePath.toString()});

    System.out.flush();
    System.setOut(origOut);
    String output = myOut.toString(StandardCharsets.UTF_8);

    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: 0, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: 5566, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: 0, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: 5566, test value: YES_NO"));

    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: y-byte-1234, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: y-byte-5566, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: y-byte-1234, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: y-byte-5566, test value: YES_NO"));

    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: z-byte-1234, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: z-byte-5566, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: z-byte-1234, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: z-byte-5566, test value: YES_NO"));
  }

  @Test
  public void testStatistics() throws Exception {
    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));

    CheckTool.main(conf, new String[]{
        "--type", "stat",
        "--values", "0", "--values", "5566",
        "--column", "x",
        testFilePath.toString()});

    CheckTool.main(conf, new String[]{
        "--type", "stat",
        "--values", "y-byte-1234", "--values", "y-byte-5566",
        "--column", "y",
        testFilePath.toString()});

    CheckTool.main(conf, new String[]{
        "--type", "stat",
        "--values", "z-byte-1234", "--values", "z-byte-5566",
        "--column", "z",
        testFilePath.toString()});

    System.out.flush();
    System.setOut(origOut);
    String output = myOut.toString(StandardCharsets.UTF_8);

    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: 0, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: 5566, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: 0, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: 5566, test value: YES_NO"));

    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: y-byte-1234, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: y-byte-5566, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: y-byte-1234, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: y-byte-5566, test value: YES_NO"));

    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: z-byte-1234, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: z-byte-5566, test value: YES_NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: z-byte-1234, test value: NO"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: z-byte-5566, test value: YES_NO"));
  }

  @Test
  public void testBloomFilter() throws Exception {
    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(myOut, false, StandardCharsets.UTF_8));

    CheckTool.main(conf, new String[]{
        "--type", "bloom-filter",
        "--values", "0", "--values", "5566",
        "--column", "x",
        testFilePath.toString()});

    CheckTool.main(conf, new String[]{
        "--type", "bloom-filter",
        "--values", "y-byte-1234", "--values", "y-byte-5566",
        "--column", "y",
        testFilePath.toString()});

    CheckTool.main(conf, new String[]{
        "--type", "bloom-filter",
        "--values", "z-byte-1234", "--values", "z-byte-5566",
        "--column", "z",
        testFilePath.toString()});

    System.out.flush();
    System.setOut(origOut);

    String output = myOut.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: 0, bloom filter: maybe exist"));
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: 5566, bloom filter: not exist"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: 0, bloom filter: maybe exist"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: 5566, bloom filter: maybe exist"));

    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: y-byte-1234, bloom filter: maybe exist"));
    assertTrue(output.contains("stripe: 0, rowIndex: 0, value: y-byte-5566, bloom filter: not exist"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: y-byte-1234, bloom filter: not exist"));
    assertTrue(output.contains("stripe: 0, rowIndex: 1, value: y-byte-5566, bloom filter: maybe exist"));
  }
}
