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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.DateUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.charset.StandardCharsets;
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

/**
 * This class tests all of the combinations of reading and writing the hybrid
 * and proleptic calendars.
 */
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

  /**
   * Test all of the type conversions from/to date.
   */
  @Test
  public void testSchemaEvolutionDate() throws Exception {
    TypeDescription schema = TypeDescription.fromString(
        "struct<d2s:date,d2t:date,s2d:string,t2d:timestamp>");
    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(fs)
            .setProlepticGregorian(writerProlepticGregorian)
            .useUTCTimestamp(true))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      batch.size = 1024;
      DateColumnVector d2s = (DateColumnVector) batch.cols[0];
      DateColumnVector d2t = (DateColumnVector) batch.cols[1];
      BytesColumnVector s2d = (BytesColumnVector) batch.cols[2];
      TimestampColumnVector t2d = (TimestampColumnVector) batch.cols[3];
      d2s.changeCalendar(writerProlepticGregorian, false);
      d2t.changeCalendar(writerProlepticGregorian, false);
      t2d.changeCalendar(writerProlepticGregorian, false);
      GregorianCalendar cal = writerProlepticGregorian ? PROLEPTIC : HYBRID;
      SimpleDateFormat dateFormat = createParser("yyyy-MM-dd", cal);
      SimpleDateFormat timeFormat = createParser("yyyy-MM-dd HH:mm:ss", cal);
      for(int r=0; r < batch.size; ++r) {
        String date = String.format("%04d-01-23", r * 2 + 1);
        String time = String.format("%04d-03-21 %02d:12:34", 2 * r + 1, r % 24);
        d2s.vector[r] = TimeUnit.MILLISECONDS.toDays(dateFormat.parse(date).getTime());
        d2t.vector[r] = d2s.vector[r];
        s2d.setVal(r, date.getBytes(StandardCharsets.UTF_8));
        t2d.time[r] = timeFormat.parse(time).getTime();
        t2d.nanos[r] = 0;
      }
      writer.addRowBatch(batch);
    }
    TypeDescription readerSchema = TypeDescription.fromString(
        "struct<d2s:string,d2t:timestamp,s2d:date,t2d:date>");
    try (Reader reader = OrcFile.createReader(testFilePath,
           OrcFile.readerOptions(conf)
                  .filesystem(fs)
                  .convertToProlepticGregorian(readerProlepticGregorian)
                  .useUTCTimestamp(true));
         RecordReader rows = reader.rows(reader.options()
                                               .schema(readerSchema))) {
      assertEquals(writerProlepticGregorian, reader.writerUsedProlepticGregorian());
      VectorizedRowBatch batch = readerSchema.createRowBatchV2();
      BytesColumnVector d2s = (BytesColumnVector) batch.cols[0];
      TimestampColumnVector d2t = (TimestampColumnVector) batch.cols[1];
      DateColumnVector s2d = (DateColumnVector) batch.cols[2];
      DateColumnVector t2d = (DateColumnVector) batch.cols[3];
      GregorianCalendar cal = readerProlepticGregorian ? PROLEPTIC : HYBRID;
      SimpleDateFormat dateFormat = createParser("yyyy-MM-dd", cal);
      SimpleDateFormat timeFormat = createParser("yyyy-MM-dd HH:mm:ss", cal);

      // Check the data
      assertTrue(rows.nextBatch(batch));
      assertEquals(1024, batch.size);
      // Ensure the column vectors are using the right calendar
      assertEquals(readerProlepticGregorian, d2t.usingProlepticCalendar());
      assertEquals(readerProlepticGregorian, s2d.isUsingProlepticCalendar());
      assertEquals(readerProlepticGregorian, t2d.isUsingProlepticCalendar());
      for(int r=0; r < batch.size; ++r) {
        String expectedD1 = String.format("%04d-01-23", 2 * r + 1);
        String expectedD2 = expectedD1 + " 00:00:00";
        String expectedT = String.format("%04d-03-21", 2 * r + 1);
        assertEquals("row " + r, expectedD1, d2s.toString(r));
        assertEquals("row " + r, expectedD2, timeFormat.format(d2t.asScratchTimestamp(r)));
        assertEquals("row " + r, expectedD1, DateUtils.printDate((int) s2d.vector[r],
            readerProlepticGregorian));
        assertEquals("row " + r, expectedT, dateFormat.format(
            new Date(TimeUnit.DAYS.toMillis(t2d.vector[r]))));
      }
      assertEquals(false, rows.nextBatch(batch));
    }
  }

  /**
   * Test all of the type conversions from/to timestamp, except for date,
   * which was handled above.
   */
  @Test
  public void testSchemaEvolutionTimestamp() throws Exception {
    TypeDescription schema = TypeDescription.fromString(
        "struct<t2i:timestamp,t2d:timestamp,t2D:timestamp,t2s:timestamp,"
        + "i2t:bigint,d2t:decimal(18,2),D2t:double,s2t:string>");
    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(fs)
            .setProlepticGregorian(writerProlepticGregorian)
            .useUTCTimestamp(true))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      batch.size = 1024;
      TimestampColumnVector t2i = (TimestampColumnVector) batch.cols[0];
      TimestampColumnVector t2d = (TimestampColumnVector) batch.cols[1];
      TimestampColumnVector t2D = (TimestampColumnVector) batch.cols[2];
      TimestampColumnVector t2s = (TimestampColumnVector) batch.cols[3];
      LongColumnVector i2t = (LongColumnVector) batch.cols[4];
      Decimal64ColumnVector d2t = (Decimal64ColumnVector) batch.cols[5];
      DoubleColumnVector D2t = (DoubleColumnVector) batch.cols[6];
      BytesColumnVector s2t = (BytesColumnVector) batch.cols[7];

      t2i.changeCalendar(writerProlepticGregorian, false);
      t2d.changeCalendar(writerProlepticGregorian, false);
      t2D.changeCalendar(writerProlepticGregorian, false);
      t2s.changeCalendar(writerProlepticGregorian, false);

      for(int r=0; r < batch.size; ++r) {
        String time = String.format("%04d-03-21 %02d:12:34.12", 2 * r + 1, r % 24);
        long millis = DateUtils.parseTime(time, writerProlepticGregorian, true);
        int nanos = (int) Math.floorMod(millis, 1000) * 1_000_000;
        t2i.time[r] = millis;
        t2i.nanos[r] = nanos;
        t2d.time[r] = millis;
        t2d.nanos[r] = nanos;
        t2D.time[r] = millis;
        t2D.nanos[r] = nanos;
        t2s.time[r] = millis;
        t2s.nanos[r] = nanos;
        i2t.vector[r] = millis;
        d2t.vector[r] = Math.floorDiv(millis, 10);
        D2t.vector[r] = millis / 1000.0;
        s2t.setVal(r, time.getBytes(StandardCharsets.UTF_8));
      }
      writer.addRowBatch(batch);
    }
    TypeDescription readerSchema = TypeDescription.fromString(
        "struct<i2t:timestamp,d2t:timestamp,D2t:timestamp,s2t:timestamp,"
            + "t2i:bigint,t2d:decimal(18,2),t2D:double,t2s:string>");
    try (Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf)
            .filesystem(fs)
            .convertToProlepticGregorian(readerProlepticGregorian)
            .useUTCTimestamp(true));
         RecordReader rows = reader.rows(reader.options()
                                             .schema(readerSchema))) {
      assertEquals(writerProlepticGregorian, reader.writerUsedProlepticGregorian());
      VectorizedRowBatch batch = readerSchema.createRowBatchV2();
      TimestampColumnVector i2t = (TimestampColumnVector) batch.cols[0];
      TimestampColumnVector d2t = (TimestampColumnVector) batch.cols[1];
      TimestampColumnVector D2t = (TimestampColumnVector) batch.cols[2];
      TimestampColumnVector s2t = (TimestampColumnVector) batch.cols[3];
      LongColumnVector t2i = (LongColumnVector) batch.cols[4];
      Decimal64ColumnVector t2d = (Decimal64ColumnVector) batch.cols[5];
      DoubleColumnVector t2D = (DoubleColumnVector) batch.cols[6];
      BytesColumnVector t2s = (BytesColumnVector) batch.cols[7];

      // Check the data
      assertTrue(rows.nextBatch(batch));
      assertEquals(1024, batch.size);
      // Ensure the column vectors are using the right calendar
      assertEquals(readerProlepticGregorian, i2t.usingProlepticCalendar());
      assertEquals(readerProlepticGregorian, d2t.usingProlepticCalendar());
      assertEquals(readerProlepticGregorian, D2t.usingProlepticCalendar());
      assertEquals(readerProlepticGregorian, s2t.usingProlepticCalendar());
      for(int r=0; r < batch.size; ++r) {
        String time = String.format("%04d-03-21 %02d:12:34.12", 2 * r + 1, r % 24);
        long millis = DateUtils.parseTime(time, readerProlepticGregorian, true);
        assertEquals("row " + r, time,
            DateUtils.printTime(i2t.time[r], readerProlepticGregorian, true));
        assertEquals("row " + r, time,
            DateUtils.printTime(d2t.time[r], readerProlepticGregorian, true));
        assertEquals("row " + r, time,
            DateUtils.printTime(D2t.time[r], readerProlepticGregorian, true));
        assertEquals("row " + r, time,
            DateUtils.printTime(s2t.time[r], readerProlepticGregorian, true));
        assertEquals("row " + r, Math.floorDiv(millis, 1000), t2i.vector[r]);
        assertEquals("row " + r, millis/10, t2d.vector[r]);
        assertEquals("row " + r, millis/1000.0, t2D.vector[r], 0.1);
        assertEquals("row " + r, time, t2s.toString(r));
      }
      assertEquals(false, rows.nextBatch(batch));
    }
  }
}
