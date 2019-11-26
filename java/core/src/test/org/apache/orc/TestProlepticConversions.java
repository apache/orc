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
package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestProlepticConversions {

  @Parameterized.Parameter
  public boolean writerProlepticGregorian;

  @Parameterized.Parameter(1)
  public boolean readerProlepticGregorian;

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    List<Object[]> result = new ArrayList<>();
    final boolean[] BOOLEANS = new boolean[]{false, true};
    for(Boolean writer: BOOLEANS) {
      for (Boolean reader: BOOLEANS) {
        result.add(new Object[]{writer, reader});
      }
    }
    return result;
  }

  private Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  private final Configuration conf;
  private final TimeZone UTC = TimeZone.getTimeZone("UTC");
  private final GregorianCalendar PROLEPTIC = new GregorianCalendar();
  private final GregorianCalendar HYBRID = new GregorianCalendar();
  {
    conf = new Configuration();
    PROLEPTIC.setTimeZone(UTC);
    PROLEPTIC.setGregorianChange(new Date(Long.MIN_VALUE));
    HYBRID.setTimeZone(UTC);
  }

  private FileSystem fs;
  private Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void setupPath() throws Exception {
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestProlepticConversion." +
       testCaseName.getMethodName().replaceFirst("\\[[0-9]+]", "") + ".orc");
    fs.delete(testFilePath, false);
  }

  private SimpleDateFormat createParser(String format, GregorianCalendar calendar) {
    SimpleDateFormat result = new SimpleDateFormat(format);
    result.setCalendar(calendar);
    return result;
  }

  @Test
  public void testReadWrite() throws Exception {
    TypeDescription schema = TypeDescription.fromString(
        "struct<d:date,t:timestamp>");
    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(fs)
            .setProlepticGregorian(writerProlepticGregorian))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      batch.size = 1024;
      DateColumnVector d = (DateColumnVector) batch.cols[0];
      TimestampColumnVector t = (TimestampColumnVector) batch.cols[1];
      d.changeCalendar(writerProlepticGregorian, false);
      t.changeCalendar(writerProlepticGregorian, false);
      GregorianCalendar cal = writerProlepticGregorian ? PROLEPTIC : HYBRID;
      SimpleDateFormat dateFormat = createParser("yyyy-MM-dd", cal);
      SimpleDateFormat timeFormat = createParser("yyyy-MM-dd HH:mm:ss", cal);
      for(int r=0; r < batch.size; ++r) {
        d.vector[r] = TimeUnit.MILLISECONDS.toDays(
            dateFormat.parse(String.format("%04d-01-23", r * 2 + 1)).getTime());
        Date val = timeFormat.parse(
            String.format("%04d-03-21 %02d:12:34", 2 * r + 1, r % 24));
        t.time[r] = val.getTime();
        t.nanos[r] = 0;
      }
      writer.addRowBatch(batch);
    }
    try (Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf)
            .filesystem(fs)
            .convertToProlepticGregorian(readerProlepticGregorian));
         RecordReader rows = reader.rows(reader.options())) {
      assertEquals(writerProlepticGregorian, reader.writerUsedProlepticGregorian());
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      DateColumnVector d = (DateColumnVector) batch.cols[0];
      TimestampColumnVector t = (TimestampColumnVector) batch.cols[1];
      GregorianCalendar cal = readerProlepticGregorian ? PROLEPTIC : HYBRID;
      SimpleDateFormat dateFormat = createParser("yyyy-MM-dd", cal);
      SimpleDateFormat timeFormat = createParser("yyyy-MM-dd HH:mm:ss", cal);

      // Check the file statistics
      ColumnStatistics[] colStats = reader.getStatistics();
      DateColumnStatistics dStats = (DateColumnStatistics) colStats[1];
      TimestampColumnStatistics tStats = (TimestampColumnStatistics) colStats[2];
      assertEquals("0001-01-23", dateFormat.format(dStats.getMinimum()));
      assertEquals("2047-01-23", dateFormat.format(dStats.getMaximum()));
      assertEquals("0001-03-21 00:12:34", timeFormat.format(tStats.getMinimum()));
      assertEquals("2047-03-21 15:12:34", timeFormat.format(tStats.getMaximum()));

      // Check the stripe stats
      List<StripeStatistics> stripeStats = reader.getStripeStatistics();
      assertEquals(1, stripeStats.size());
      colStats = stripeStats.get(0).getColumnStatistics();
      dStats = (DateColumnStatistics) colStats[1];
      tStats = (TimestampColumnStatistics) colStats[2];
      assertEquals("0001-01-23", dateFormat.format(dStats.getMinimum()));
      assertEquals("2047-01-23", dateFormat.format(dStats.getMaximum()));
      assertEquals("0001-03-21 00:12:34", timeFormat.format(tStats.getMinimum()));
      assertEquals("2047-03-21 15:12:34", timeFormat.format(tStats.getMaximum()));

      // Check the data
      assertTrue(rows.nextBatch(batch));
      assertEquals(1024, batch.size);
      // Ensure the column vectors are using the right calendar
      assertEquals(readerProlepticGregorian, d.isUsingProlepticCalendar());
      assertEquals(readerProlepticGregorian, t.usingProlepticCalendar());
      for(int r=0; r < batch.size; ++r) {
        String expectedD = String.format("%04d-01-23", r * 2 + 1);
        String expectedT = String.format("%04d-03-21 %02d:12:34", 2 * r + 1, r % 24);
        assertEquals("row " + r, expectedD, dateFormat.format(
            new Date(TimeUnit.DAYS.toMillis(d.vector[r]))));
        assertEquals("row " + r, expectedT, timeFormat.format(t.asScratchTimestamp(r)));
      }
    }
  }
}
