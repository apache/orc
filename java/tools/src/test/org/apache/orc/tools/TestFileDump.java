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

package org.apache.orc.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFileDump {

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestFileDump.testDump.orc");
    fs.delete(testFilePath, false);
  }

  static TypeDescription getMyRecordType() {
    return TypeDescription.createStruct()
        .addField("i", TypeDescription.createInt())
        .addField("l", TypeDescription.createLong())
        .addField("s", TypeDescription.createString());
  }

  static void appendMyRecord(VectorizedRowBatch batch,
                             int i,
                             long l,
                             String str) {
    ((LongColumnVector) batch.cols[0]).vector[batch.size] = i;
    ((LongColumnVector) batch.cols[1]).vector[batch.size] = l;
    if (str == null) {
      batch.cols[2].noNulls = false;
      batch.cols[2].isNull[batch.size] = true;
    } else {
      ((BytesColumnVector) batch.cols[2]).setVal(batch.size,
          str.getBytes());
    }
    batch.size += 1;
  }

  static TypeDescription getAllTypesType() {
    return TypeDescription.createStruct()
        .addField("b", TypeDescription.createBoolean())
        .addField("bt", TypeDescription.createByte())
        .addField("s", TypeDescription.createShort())
        .addField("i", TypeDescription.createInt())
        .addField("l", TypeDescription.createLong())
        .addField("f", TypeDescription.createFloat())
        .addField("d", TypeDescription.createDouble())
        .addField("de", TypeDescription.createDecimal())
        .addField("t", TypeDescription.createTimestamp())
        .addField("dt", TypeDescription.createDate())
        .addField("str", TypeDescription.createString())
        .addField("c", TypeDescription.createChar().withMaxLength(5))
        .addField("vc", TypeDescription.createVarchar().withMaxLength(10))
        .addField("m", TypeDescription.createMap(
            TypeDescription.createString(),
            TypeDescription.createString()))
        .addField("a", TypeDescription.createList(TypeDescription.createInt()))
        .addField("st", TypeDescription.createStruct()
                .addField("i", TypeDescription.createInt())
                .addField("s", TypeDescription.createString()));
  }

  static void appendAllTypes(VectorizedRowBatch batch,
                             boolean b,
                             byte bt,
                             short s,
                             int i,
                             long l,
                             float f,
                             double d,
                             HiveDecimalWritable de,
                             Timestamp t,
                             DateWritable dt,
                             String str,
                             String c,
                             String vc,
                             Map<String, String> m,
                             List<Integer> a,
                             int sti,
                             String sts) {
    int row = batch.size++;
    ((LongColumnVector) batch.cols[0]).vector[row] = b ? 1 : 0;
    ((LongColumnVector) batch.cols[1]).vector[row] = bt;
    ((LongColumnVector) batch.cols[2]).vector[row] = s;
    ((LongColumnVector) batch.cols[3]).vector[row] = i;
    ((LongColumnVector) batch.cols[4]).vector[row] = l;
    ((DoubleColumnVector) batch.cols[5]).vector[row] = f;
    ((DoubleColumnVector) batch.cols[6]).vector[row] = d;
    ((DecimalColumnVector) batch.cols[7]).vector[row].set(de);
    ((TimestampColumnVector) batch.cols[8]).set(row, t);
    ((LongColumnVector) batch.cols[9]).vector[row] = dt.getDays();
    ((BytesColumnVector) batch.cols[10]).setVal(row, str.getBytes());
    ((BytesColumnVector) batch.cols[11]).setVal(row, c.getBytes());
    ((BytesColumnVector) batch.cols[12]).setVal(row, vc.getBytes());
    MapColumnVector map = (MapColumnVector) batch.cols[13];
    int offset = map.childCount;
    map.offsets[row] = offset;
    map.lengths[row] = m.size();
    map.childCount += map.lengths[row];
    for(Map.Entry<String, String> entry: m.entrySet()) {
      ((BytesColumnVector) map.keys).setVal(offset, entry.getKey().getBytes());
      ((BytesColumnVector) map.values).setVal(offset++,
          entry.getValue().getBytes());
    }
    ListColumnVector list = (ListColumnVector) batch.cols[14];
    offset = list.childCount;
    list.offsets[row] = offset;
    list.lengths[row] = a.size();
    list.childCount += list.lengths[row];
    for(int e=0; e < a.size(); ++e) {
      ((LongColumnVector) list.child).vector[offset + e] = a.get(e);
    }
    StructColumnVector struct = (StructColumnVector) batch.cols[15];
    ((LongColumnVector) struct.fields[0]).vector[row] = sti;
    ((BytesColumnVector) struct.fields[1]).setVal(row, sts.getBytes());
  }

  public static void checkOutput(String expected,
                                 String actual) throws Exception {
    BufferedReader eStream =
        new BufferedReader(new FileReader
            (TestJsonFileDump.getFileFromClasspath(expected)));
    BufferedReader aStream =
        new BufferedReader(new FileReader(actual));
    String expectedLine = eStream.readLine().trim();
    while (expectedLine != null) {
      String actualLine = aStream.readLine().trim();
      Assert.assertEquals(expectedLine, actualLine);
      expectedLine = eStream.readLine();
      expectedLine = expectedLine == null ? null : expectedLine.trim();
    }
    Assert.assertNull(eStream.readLine());
    Assert.assertNull(aStream.readLine());
    eStream.close();
    aStream.close();
  }

  @Test
  public void testDump() throws Exception {
    TypeDescription schema = getMyRecordType();
    conf.set(OrcConf.ENCODING_STRATEGY.getAttribute(), "COMPRESSION");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .fileSystem(fs)
            .setSchema(schema)
            .compress(CompressionKind.ZLIB)
            .stripeSize(100000)
            .rowIndexStride(1000));
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    VectorizedRowBatch batch = schema.createRowBatch(1000);
    for(int i=0; i < 21000; ++i) {
      appendMyRecord(batch, r1.nextInt(), r1.nextLong(),
          words[r1.nextInt(words.length)]);
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size > 0) {
      writer.addRowBatch(batch);
    }
    writer.addUserMetadata("hive.acid.key.index",
      StandardCharsets.UTF_8.encode("1,1,1;2,3,5;"));
    writer.addUserMetadata("some.user.property",
      StandardCharsets.UTF_8.encode("foo#bar$baz&"));
    writer.close();
    assertEquals(2079000, writer.getRawDataSize());
    assertEquals(21000, writer.getNumberOfRows());
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=1,2,3"});
    System.out.flush();
    System.setOut(origOut);


    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }

  @Test
  public void testDataDump() throws Exception {
    TypeDescription schema = getAllTypesType();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .fileSystem(fs)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .rowIndexStride(1000));
    VectorizedRowBatch batch = schema.createRowBatch(1000);
    Map<String, String> m = new HashMap<String, String>(2);
    m.put("k1", "v1");
    appendAllTypes(batch,
        true,
        (byte) 10,
        (short) 100,
        1000,
        10000L,
        4.0f,
        20.0,
        new HiveDecimalWritable("4.2222"),
        new Timestamp(1416967764000L),
        new DateWritable(new Date(1416967764000L)),
        "string",
        "hello",
       "hello",
        m,
        Arrays.asList(100, 200),
        10, "foo");
    m.clear();
    m.put("k3", "v3");
    appendAllTypes(
        batch,
        false,
        (byte)20,
        (short)200,
        2000,
        20000L,
        8.0f,
        40.0,
        new HiveDecimalWritable("2.2222"),
        new Timestamp(1416967364000L),
        new DateWritable(new Date(1411967764000L)),
        "abcd",
        "world",
        "world",
        m,
        Arrays.asList(200, 300),
        20, "bar");
    writer.addRowBatch(batch);

    writer.close();
    assertEquals(1564, 0, writer.getRawDataSize());
    assertEquals(2, writer.getNumberOfRows());
    PrintStream origOut = System.out;
    ByteArrayOutputStream myOut = new ByteArrayOutputStream();

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "-d"});
    System.out.flush();
    System.setOut(origOut);
    String[] lines = myOut.toString().split("\n");
    Assert.assertEquals("{\"b\":true,\"bt\":10,\"s\":100,\"i\":1000,\"l\":10000,\"f\":4,\"d\":20,\"de\":\"4.2222\",\"t\":\"2014-11-25 18:09:24.0\",\"dt\":\"2014-11-25\",\"str\":\"string\",\"c\":\"hello\",\"vc\":\"hello\",\"m\":[{\"_key\":\"k1\",\"_value\":\"v1\"}],\"a\":[100,200],\"st\":{\"i\":10,\"s\":\"foo\"}}", lines[0]);
    Assert.assertEquals("{\"b\":false,\"bt\":20,\"s\":200,\"i\":2000,\"l\":20000,\"f\":8,\"d\":40,\"de\":\"2.2222\",\"t\":\"2014-11-25 18:02:44.0\",\"dt\":\"2014-09-28\",\"str\":\"abcd\",\"c\":\"world\",\"vc\":\"world\",\"m\":[{\"_key\":\"k3\",\"_value\":\"v3\"}],\"a\":[200,300],\"st\":{\"i\":20,\"s\":\"bar\"}}", lines[1]);
  }
  
  // Test that if the fraction of rows that have distinct strings is greater than the configured
  // threshold dictionary encoding is turned off.  If dictionary encoding is turned off the length
  // of the dictionary stream for the column will be 0 in the ORC file dump.
  @Test
  public void testDictionaryThreshold() throws Exception {
    TypeDescription schema = getMyRecordType();
    Configuration conf = new Configuration();
    conf.set(OrcConf.ENCODING_STRATEGY.getAttribute(), "COMPRESSION");
    conf.setFloat(OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getAttribute(), 0.49f);
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .fileSystem(fs)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.ZLIB)
            .rowIndexStride(1000)
            .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch(1000);
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    int nextInt = 0;
    for(int i=0; i < 21000; ++i) {
      // Write out the same string twice, this guarantees the fraction of rows with
      // distinct strings is 0.5
      if (i % 2 == 0) {
        nextInt = r1.nextInt(words.length);
        // Append the value of i to the word, this guarantees when an index or word is repeated
        // the actual string is unique.
        words[nextInt] += "-" + i;
      }
      appendMyRecord(batch, r1.nextInt(), r1.nextLong(), words[nextInt]);
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump-dictionary-threshold.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=1,2,3"});
    System.out.flush();
    System.setOut(origOut);

    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }

  @Test
  public void testBloomFilter() throws Exception {
    TypeDescription schema = getMyRecordType();
    conf.set(OrcConf.ENCODING_STRATEGY.getAttribute(), "COMPRESSION");
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem(fs)
        .setSchema(schema)
        .stripeSize(100000)
        .compress(CompressionKind.ZLIB)
        .bufferSize(10000)
        .rowIndexStride(1000)
        .bloomFilterColumns("S");
    Writer writer = OrcFile.createWriter(testFilePath, options);
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    VectorizedRowBatch batch = schema.createRowBatch(1000);
    for(int i=0; i < 21000; ++i) {
      appendMyRecord(batch, r1.nextInt(), r1.nextLong(),
          words[r1.nextInt(words.length)]);
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size > 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump-bloomfilter.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=3"});
    System.out.flush();
    System.setOut(origOut);


    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }

  @Test
  public void testBloomFilter2() throws Exception {
    TypeDescription schema = getMyRecordType();
    conf.set(OrcConf.ENCODING_STRATEGY.getAttribute(), "COMPRESSION");
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf)
        .fileSystem(fs)
        .setSchema(schema)
        .stripeSize(100000)
        .compress(CompressionKind.ZLIB)
        .bufferSize(10000)
        .rowIndexStride(1000)
        .bloomFilterColumns("l,s")
        .bloomFilterFpp(0.01)
        .bloomFilterVersion(OrcFile.BloomFilterVersion.ORIGINAL);
    VectorizedRowBatch batch = schema.createRowBatch(1000);
    Writer writer = OrcFile.createWriter(testFilePath, options);
    Random r1 = new Random(1);
    String[] words = new String[]{"It", "was", "the", "best", "of", "times,",
        "it", "was", "the", "worst", "of", "times,", "it", "was", "the", "age",
        "of", "wisdom,", "it", "was", "the", "age", "of", "foolishness,", "it",
        "was", "the", "epoch", "of", "belief,", "it", "was", "the", "epoch",
        "of", "incredulity,", "it", "was", "the", "season", "of", "Light,",
        "it", "was", "the", "season", "of", "Darkness,", "it", "was", "the",
        "spring", "of", "hope,", "it", "was", "the", "winter", "of", "despair,",
        "we", "had", "everything", "before", "us,", "we", "had", "nothing",
        "before", "us,", "we", "were", "all", "going", "direct", "to",
        "Heaven,", "we", "were", "all", "going", "direct", "the", "other",
        "way"};
    for(int i=0; i < 21000; ++i) {
      appendMyRecord(batch, r1.nextInt(), r1.nextLong(),
          words[r1.nextInt(words.length)]);
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size > 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-dump-bloomfilter2.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=2"});
    System.out.flush();
    System.setOut(origOut);

    checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }

  private static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for (int i = 0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  private void appendRow(VectorizedRowBatch batch, BytesWritable bytes,
                 String str) {
    int row = batch.size++;
    if (bytes == null) {
      batch.cols[0].noNulls = false;
      batch.cols[0].isNull[row] = true;
    } else {
      ((BytesColumnVector) batch.cols[0]).setVal(row, bytes.getBytes(),
          0, bytes.getLength());
    }
    if (str == null) {
      batch.cols[1].noNulls = false;
      batch.cols[1].isNull[row] = true;
    } else {
      ((BytesColumnVector) batch.cols[1]).setVal(row, str.getBytes());
    }
  }

  @Test
  public void testHasNull() throws Exception {
    TypeDescription schema =
        TypeDescription.createStruct()
            .addField("bytes1", TypeDescription.createBinary())
            .addField("string1", TypeDescription.createString());
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(1000)
            .stripeSize(10000)
            .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch(5000);
    // STRIPE 1
    // RG1
    for(int i=0; i<1000; i++) {
      appendRow(batch, bytes(1, 2, 3), "RG1");
    }
    writer.addRowBatch(batch);
    batch.reset();
    // RG2
    for(int i=0; i<1000; i++) {
      appendRow(batch, bytes(1, 2, 3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    // RG3
    for(int i=0; i<1000; i++) {
      appendRow(batch, bytes(1, 2, 3), "RG3");
    }
    writer.addRowBatch(batch);
    batch.reset();
    // RG4
    for (int i = 0; i < 1000; i++) {
      appendRow(batch, bytes(1,2,3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    // RG5
    for(int i=0; i<1000; i++) {
      appendRow(batch, bytes(1, 2, 3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    // STRIPE 2
    for (int i = 0; i < 5000; i++) {
      appendRow(batch, bytes(1,2,3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    // STRIPE 3
    for (int i = 0; i < 5000; i++) {
      appendRow(batch, bytes(1,2,3), "STRIPE-3");
    }
    writer.addRowBatch(batch);
    batch.reset();
    // STRIPE 4
    for (int i = 0; i < 5000; i++) {
      appendRow(batch, bytes(1,2,3), null);
    }
    writer.addRowBatch(batch);
    batch.reset();
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    // check the file level stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(20000, stats[0].getNumberOfValues());
    assertEquals(20000, stats[1].getNumberOfValues());
    assertEquals(7000, stats[2].getNumberOfValues());
    assertEquals(false, stats[0].hasNull());
    assertEquals(false, stats[1].hasNull());
    assertEquals(true, stats[2].hasNull());

    // check the stripe level stats
    List<StripeStatistics> stripeStats = reader.getStripeStatistics();
    // stripe 1 stats
    StripeStatistics ss1 = stripeStats.get(0);
    ColumnStatistics ss1_cs1 = ss1.getColumnStatistics()[0];
    ColumnStatistics ss1_cs2 = ss1.getColumnStatistics()[1];
    ColumnStatistics ss1_cs3 = ss1.getColumnStatistics()[2];
    assertEquals(false, ss1_cs1.hasNull());
    assertEquals(false, ss1_cs2.hasNull());
    assertEquals(true, ss1_cs3.hasNull());

    // stripe 2 stats
    StripeStatistics ss2 = stripeStats.get(1);
    ColumnStatistics ss2_cs1 = ss2.getColumnStatistics()[0];
    ColumnStatistics ss2_cs2 = ss2.getColumnStatistics()[1];
    ColumnStatistics ss2_cs3 = ss2.getColumnStatistics()[2];
    assertEquals(false, ss2_cs1.hasNull());
    assertEquals(false, ss2_cs2.hasNull());
    assertEquals(true, ss2_cs3.hasNull());

    // stripe 3 stats
    StripeStatistics ss3 = stripeStats.get(2);
    ColumnStatistics ss3_cs1 = ss3.getColumnStatistics()[0];
    ColumnStatistics ss3_cs2 = ss3.getColumnStatistics()[1];
    ColumnStatistics ss3_cs3 = ss3.getColumnStatistics()[2];
    assertEquals(false, ss3_cs1.hasNull());
    assertEquals(false, ss3_cs2.hasNull());
    assertEquals(false, ss3_cs3.hasNull());

    // stripe 4 stats
    StripeStatistics ss4 = stripeStats.get(3);
    ColumnStatistics ss4_cs1 = ss4.getColumnStatistics()[0];
    ColumnStatistics ss4_cs2 = ss4.getColumnStatistics()[1];
    ColumnStatistics ss4_cs3 = ss4.getColumnStatistics()[2];
    assertEquals(false, ss4_cs1.hasNull());
    assertEquals(false, ss4_cs2.hasNull());
    assertEquals(true, ss4_cs3.hasNull());

    // Test file dump
    PrintStream origOut = System.out;
    String outputFilename = "orc-file-has-null.out";
    FileOutputStream myOut = new FileOutputStream(workDir + File.separator + outputFilename);

    // replace stdout and run command
    System.setOut(new PrintStream(myOut));
    FileDump.main(new String[]{testFilePath.toString(), "--rowindex=2"});
    System.out.flush();
    System.setOut(origOut);
    // If called with an expression evaluating to false, the test will halt
    // and be ignored.
    assumeTrue(!System.getProperty("os.name").startsWith("Windows"));
    TestFileDump.checkOutput(outputFilename, workDir + File.separator + outputFilename);
  }
}
