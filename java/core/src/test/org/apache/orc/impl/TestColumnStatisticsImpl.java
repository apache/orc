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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

public class TestColumnStatisticsImpl {

  @Test
  public void testUpdateDate() throws Exception {
    ColumnStatisticsImpl stat = ColumnStatisticsImpl.create(TypeDescription.createDate());
    DateWritable date = new DateWritable(16400);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 1, 16400, 16400);

    date.set(16410);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 2, 16400, 16410);

    date.set(16420);
    stat.increment();
    stat.updateDate(date);
    assertDateStatistics(stat, 3, 16400, 16420);
  }

  private void assertDateStatistics(ColumnStatisticsImpl stat, int count, int minimum, int maximum) {
    OrcProto.ColumnStatistics.Builder builder = stat.serialize();

    assertEquals(count, builder.getNumberOfValues());
    assertTrue(builder.hasDateStatistics());
    assertFalse(builder.hasStringStatistics());

    OrcProto.DateStatistics protoStat = builder.getDateStatistics();
    assertTrue(protoStat.hasMinimum());
    assertEquals(minimum, protoStat.getMinimum());
    assertTrue(protoStat.hasMaximum());
    assertEquals(maximum, protoStat.getMaximum());
  }

  @Test
  public void testOldTimestamps() throws IOException {
    Path exampleDir = new Path(System.getProperty("example.dir"));
    Path file = new Path(exampleDir, "TestOrcFile.testTimestamp.orc");
    Configuration conf = new Configuration();
    Reader reader = OrcFile.createReader(file, OrcFile.readerOptions(conf));
    TimestampColumnStatistics stats =
        (TimestampColumnStatistics) reader.getStatistics()[0];
    assertEquals("1995-01-01 00:00:00.688", stats.getMinimum().toString());
    assertEquals("2037-01-01 00:00:00.0", stats.getMaximum().toString());
  }

  @Test
  public void testStringStatisticsWithNulls() {
    OrcProto.StringStatistics stringStatistics = new ColumnStatisticsImpl
        .StringStatisticsImpl().serialize().getStringStatistics();

    assertThat(stringStatistics, equalTo(OrcProto.StringStatistics.getDefaultInstance()));
  }

  @Test
  // test that StringStatistics behave correctly with small strings
  public void testStringStatisticsUntrimmedStrings() {
    ColumnStatisticsImpl stats = new ColumnStatisticsImpl.StringStatisticsImpl();

    stats.updateString(new Text("abc"));
    stats.increment();
    stats.updateString(new Text("def"));
    stats.increment();

    OrcProto.ColumnStatistics.Builder builder = stats.serialize();

    assertThat(builder.getStringStatistics().getIsMinimumTrimmed(), is(false));
    assertThat(builder.getStringStatistics().getIsMaximumTrimmed(), is(false));
    assertThat(builder.getStringStatistics().getMinimum(), equalTo("abc"));
    assertThat(builder.getStringStatistics().getMaximum(), equalTo("def"));
  }

  @Test
  // test that when length of minimum > 1024, it is trimmed
  public void testStringStatisticsTrimLargeMinimum() {
    char[] chars = new char[1030];
    Arrays.fill(chars, 'd');
    String str = new String(chars);
    ColumnStatisticsImpl stats = new ColumnStatisticsImpl.StringStatisticsImpl();

    stats.updateString(new Text(str));
    stats.increment();

    OrcProto.ColumnStatistics.Builder builder = stats.serialize();
    assertThat(builder.getStringStatistics().getIsMinimumTrimmed(), is(true));
    assertThat(builder.getStringStatistics().getIsMaximumTrimmed(), is(true));
    assertThat(builder.getStringStatistics().getMinimum(), equalTo(StringUtils.left(str, 1024)));
    assertThat(builder.getStringStatistics().getMaximum(), equalTo(StringUtils.left(str, 1024)));
  }


  @Test
  // test that even when maximum & minimum are trimmed, they are updated correctly.
  public void testStringStatisticsUpdateOfTrimmedStrings() {
    char[] chars = new char[1030];
    Arrays.fill(chars, 'd');
    String str1 = new String(chars);
    chars = new char[1025];
    Arrays.fill(chars, 'e');
    String str2 = new String(chars);
    ColumnStatisticsImpl stats = new ColumnStatisticsImpl.StringStatisticsImpl();

    stats.updateString(new Text(str1));
    stats.increment();
    // update with a string which is greater than current maximum
    stats.updateString(new Text(str2));
    stats.increment();
    // now update minimum with a string which is less than current minimum
    stats.updateString(new Text("abc"));
    stats.increment();

    OrcProto.ColumnStatistics.Builder builder = stats.serialize();
    assertThat(builder.getStringStatistics().getIsMinimumTrimmed(), is(false));
    assertThat(builder.getStringStatistics().getIsMaximumTrimmed(), is(true));
    assertThat(builder.getStringStatistics().getMinimum(), equalTo("abc"));
    assertThat(builder.getStringStatistics().getMaximum(), equalTo(StringUtils.left(str2, 1024)));
  }
}
