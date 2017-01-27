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

import static junit.framework.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test ColumnStatisticsImpl for ORC.
 */
public class TestColumnStatistics {

  @Test
  public void testLongMerge() throws Exception {
    TypeDescription schema = TypeDescription.createInt();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateInteger(10, 2);
    stats2.updateInteger(1, 1);
    stats2.updateInteger(1000, 1);
    stats1.merge(stats2);
    IntegerColumnStatistics typed = (IntegerColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum());
    assertEquals(1000, typed.getMaximum());
    stats1.reset();
    stats1.updateInteger(-10, 1);
    stats1.updateInteger(10000, 1);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum());
    assertEquals(10000, typed.getMaximum());
  }

  @Test
  public void testDoubleMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDouble();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDouble(10.0);
    stats1.updateDouble(100.0);
    stats2.updateDouble(1.0);
    stats2.updateDouble(1000.0);
    stats1.merge(stats2);
    DoubleColumnStatistics typed = (DoubleColumnStatistics) stats1;
    assertEquals(1.0, typed.getMinimum(), 0.001);
    assertEquals(1000.0, typed.getMaximum(), 0.001);
    stats1.reset();
    stats1.updateDouble(-10);
    stats1.updateDouble(10000);
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum(), 0.001);
    assertEquals(10000, typed.getMaximum(), 0.001);
  }


  @Test
  public void testStringMerge() throws Exception {
    TypeDescription schema = TypeDescription.createString();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateString(new Text("bob"));
    stats1.updateString(new Text("david"));
    stats1.updateString(new Text("charles"));
    stats2.updateString(new Text("anne"));
    byte[] erin = new byte[]{0, 1, 2, 3, 4, 5, 101, 114, 105, 110};
    stats2.updateString(erin, 6, 4, 5);
    assertEquals(24, ((StringColumnStatistics)stats2).getSum());
    stats1.merge(stats2);
    StringColumnStatistics typed = (StringColumnStatistics) stats1;
    assertEquals("anne", typed.getMinimum());
    assertEquals("erin", typed.getMaximum());
    assertEquals(39, typed.getSum());
    stats1.reset();
    stats1.updateString(new Text("aaa"));
    stats1.updateString(new Text("zzz"));
    stats1.merge(stats2);
    assertEquals("aaa", typed.getMinimum());
    assertEquals("zzz", typed.getMaximum());
  }

  @Test
  public void testDateMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDate();

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDate(new DateWritable(1000));
    stats1.updateDate(new DateWritable(100));
    stats2.updateDate(new DateWritable(10));
    stats2.updateDate(new DateWritable(2000));
    stats1.merge(stats2);
    DateColumnStatistics typed = (DateColumnStatistics) stats1;
    assertEquals(new DateWritable(10).get(), typed.getMinimum());
    assertEquals(new DateWritable(2000).get(), typed.getMaximum());
    stats1.reset();
    stats1.updateDate(new DateWritable(-10));
    stats1.updateDate(new DateWritable(10000));
    stats1.merge(stats2);
    assertEquals(new DateWritable(-10).get(), typed.getMinimum());
    assertEquals(new DateWritable(10000).get(), typed.getMaximum());
  }

  @Test
  public void testTimestampMergeUTC() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateTimestamp(new Timestamp(10));
    stats1.updateTimestamp(new Timestamp(100));
    stats2.updateTimestamp(new Timestamp(1));
    stats2.updateTimestamp(new Timestamp(1000));
    stats1.merge(stats2);
    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().getTime());
    assertEquals(1000, typed.getMaximum().getTime());
    stats1.reset();
    stats1.updateTimestamp(new Timestamp(-10));
    stats1.updateTimestamp(new Timestamp(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().getTime());
    assertEquals(10000, typed.getMaximum().getTime());
    TimeZone.setDefault(original);
  }

  private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private final SimpleDateFormat format = new SimpleDateFormat(TIME_FORMAT);

  private Timestamp parseTime(String value) {
    try {
      return new Timestamp(format.parse(value).getTime());
    } catch (ParseException e) {
      throw new IllegalArgumentException("bad time parse for " + value, e);
    }
  }

  @Test
  public void testTimestampMergeLA() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateTimestamp(parseTime("2000-04-02 03:30:00"));
    stats1.updateTimestamp(parseTime("2000-04-02 01:30:00"));
    stats1.increment(2);
    stats2.updateTimestamp(parseTime("2000-10-29 01:30:00"));
    stats2.updateTimestamp(parseTime("2000-10-29 03:30:00"));
    stats2.increment(2);
    TimestampColumnStatistics typed = (TimestampColumnStatistics) stats1;
    assertEquals("2000-04-02 01:30:00.0", typed.getMinimum().toString());
    assertEquals("2000-04-02 03:30:00.0", typed.getMaximum().toString());
    stats1.merge(stats2);
    assertEquals("2000-04-02 01:30:00.0", typed.getMinimum().toString());
    assertEquals("2000-10-29 03:30:00.0", typed.getMaximum().toString());
    stats1.reset();
    stats1.updateTimestamp(parseTime("1999-04-04 00:00:00"));
    stats1.updateTimestamp(parseTime("2009-03-08 12:00:00"));
    stats1.merge(stats2);
    assertEquals("1999-04-04 00:00:00.0", typed.getMinimum().toString());
    assertEquals("2009-03-08 12:00:00.0", typed.getMaximum().toString());

    // serialize and read back in with phoenix timezone
    OrcProto.ColumnStatistics serial = stats2.serialize().build();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Phoenix"));
    ColumnStatisticsImpl stats3 = ColumnStatisticsImpl.deserialize(serial);
    assertEquals("2000-10-29 01:30:00.0",
        ((TimestampColumnStatistics) stats3).getMinimum().toString());
    assertEquals("2000-10-29 03:30:00.0",
        ((TimestampColumnStatistics) stats3).getMaximum().toString());
    TimeZone.setDefault(original);
  }

  @Test
  public void testDecimalMerge() throws Exception {
    TypeDescription schema = TypeDescription.createDecimal()
        .withPrecision(38).withScale(16);

    ColumnStatisticsImpl stats1 = ColumnStatisticsImpl.create(schema);
    ColumnStatisticsImpl stats2 = ColumnStatisticsImpl.create(schema);
    stats1.updateDecimal(new HiveDecimalWritable(10));
    stats1.updateDecimal(new HiveDecimalWritable(100));
    stats2.updateDecimal(new HiveDecimalWritable(1));
    stats2.updateDecimal(new HiveDecimalWritable(1000));
    stats1.merge(stats2);
    DecimalColumnStatistics typed = (DecimalColumnStatistics) stats1;
    assertEquals(1, typed.getMinimum().longValue());
    assertEquals(1000, typed.getMaximum().longValue());
    stats1.reset();
    stats1.updateDecimal(new HiveDecimalWritable(-10));
    stats1.updateDecimal(new HiveDecimalWritable(10000));
    stats1.merge(stats2);
    assertEquals(-10, typed.getMinimum().longValue());
    assertEquals(10000, typed.getMaximum().longValue());
  }


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
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestOrcFile." + testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }
}
