/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.SerializationUtils;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@RunWith(Parameterized.class)
public class TestOrcTimezonePPD {
  private static final Logger LOG = LoggerFactory.getLogger(TestOrcTimezonePPD.class);

  Path workDir = new Path(System.getProperty("test.tmp.dir",
    "target" + File.separator + "test" + File.separator + "tmp"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  String writerTimeZone;
  String readerTimeZone;
  static TimeZone defaultTimeZone = TimeZone.getDefault();
  TimeZone utcTz = TimeZone.getTimeZone("UTC");
  DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public TestOrcTimezonePPD(String writerTZ, String readerTZ) {
    this.writerTimeZone = writerTZ;
    this.readerTimeZone = readerTZ;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> result = Arrays.asList(new Object[][]{
      {"US/Eastern", "America/Los_Angeles"},
      {"US/Eastern", "UTC"},
        /* Extreme timezones */
      {"GMT-12:00", "GMT+14:00"},
        /* No difference in DST */
      {"America/Los_Angeles", "America/Los_Angeles"}, /* same timezone both with DST */
      {"Europe/Berlin", "Europe/Berlin"}, /* same as above but europe */
      {"America/Phoenix", "Asia/Kolkata"} /* Writer no DST, Reader no DST */,
      {"Europe/Berlin", "America/Los_Angeles"} /* Writer DST, Reader DST */,
      {"Europe/Berlin", "America/Chicago"} /* Writer DST, Reader DST */,
        /* With DST difference */
      {"Europe/Berlin", "UTC"},
      {"UTC", "Europe/Berlin"} /* Writer no DST, Reader DST */,
      {"America/Los_Angeles", "Asia/Kolkata"} /* Writer DST, Reader no DST */,
      {"Europe/Berlin", "Asia/Kolkata"} /* Writer DST, Reader no DST */,
        /* Timezone offsets for the reader has changed historically */
      {"Asia/Saigon", "Pacific/Enderbury"},
      {"UTC", "Asia/Jerusalem"},
    });
    return result;
  }

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
      testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @After
  public void restoreTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  public static PredicateLeaf createPredicateLeaf(PredicateLeaf.Operator operator,
    PredicateLeaf.Type type,
    String columnName,
    Object literal,
    List<Object> literalList) {
    return new SearchArgumentImpl.PredicateLeafImpl(operator, type, columnName,
      literal, literalList);
  }

  @Test
  public void testTimestampPPDMinMax() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone.setDefault(TimeZone.getTimeZone(writerTimeZone));
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000));
    assertEquals(writerTimeZone, TimeZone.getDefault().getID());
    List<String> ts = Lists.newArrayList();
    ts.add("2007-08-01 00:00:00.0");
    ts.add("2007-08-01 04:00:00.0");
    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (String t : ts) {
      times.set(batch.size++, Timestamp.valueOf(t));
    }
    writer.addRowBatch(batch);
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(readerTimeZone, TimeZone.getDefault().getID());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    times = (TimestampColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        assertEquals(ts.get(idx++), times.asScratchTimestamp(r).toString());
      }
    }
    rows.close();
    ColumnStatistics[] colStats = reader.getStatistics();
    Timestamp gotMin = ((TimestampColumnStatistics) colStats[0]).getMinimum();
    assertEquals("2007-08-01 00:00:00.0", gotMin.toString());

    Timestamp gotMax = ((TimestampColumnStatistics) colStats[0]).getMaximum();
    assertEquals("2007-08-01 04:00:00.0", gotMax.toString());

    Assert.assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[0],
      SearchArgumentFactory.newBuilder().equals
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-01 00:00:00.0")).build().getLeaves().get(0),
      null));

    Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[0],
      SearchArgumentFactory.newBuilder().equals
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-02 00:00:00.0")).build().getLeaves().get(0),
      null));

    Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[0],
      SearchArgumentFactory.newBuilder().between
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-01 05:00:00.0"),
          Timestamp.valueOf("2007-08-01 06:00:00.0")).build().getLeaves().get(0),
      null));

    Assert.assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[0],
      SearchArgumentFactory.newBuilder().between
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-01 00:00:00.0"),
          Timestamp.valueOf("2007-08-01 03:00:00.0")).build().getLeaves().get(0),
      null));

    Assert.assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[0],
      SearchArgumentFactory.newBuilder().in
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-01 00:00:00.0"),
          Timestamp.valueOf("2007-08-01 03:00:00.0")).build().getLeaves().get(0),
      null));

    Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[0],
      SearchArgumentFactory.newBuilder().in
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-02 00:00:00.0"),
          Timestamp.valueOf("2007-08-02 03:00:00.0")).build().getLeaves().get(0),
      null));
  }

  static OrcProto.ColumnEncoding buildEncoding() {
    OrcProto.ColumnEncoding.Builder result =
        OrcProto.ColumnEncoding.newBuilder();
    result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
        .setBloomEncoding(BloomFilterIO.Encoding.UTF8_UTC.getId());
    return result.build();
  }

  @Test
  public void testTimestampPPDBloomFilter() throws Exception {
    LOG.info("Writer = " + writerTimeZone + " reader = " + readerTimeZone);
    TypeDescription schema = TypeDescription.createStruct().addField("ts", TypeDescription.createTimestamp());

    TimeZone.setDefault(TimeZone.getTimeZone(writerTimeZone));
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000).bloomFilterColumns("ts").writerVersion(OrcFile.WriterVersion.ORC_101));
    assertEquals(writerTimeZone, TimeZone.getDefault().getID());
    List<String> ts = Lists.newArrayList();
    ts.add("2007-08-01 00:00:00.0");
    ts.add("2007-08-01 04:00:00.0");
    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (String t : ts) {
      times.set(batch.size++, Timestamp.valueOf(t));
    }
    writer.addRowBatch(batch);
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(readerTimeZone, TimeZone.getDefault().getID());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    times = (TimestampColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        assertEquals(ts.get(idx++), times.asScratchTimestamp(r).toString());
      }
    }
    boolean[] sargColumns = new boolean[2];
    Arrays.fill(sargColumns, true);
    OrcIndex indices = ((RecordReaderImpl) rows).readRowIndex(0, null, sargColumns);
    rows.close();
    ColumnStatistics[] colStats = reader.getStatistics();
    Timestamp gotMin = ((TimestampColumnStatistics) colStats[1]).getMinimum();
    assertEquals("2007-08-01 00:00:00.0", gotMin.toString());

    Timestamp gotMax = ((TimestampColumnStatistics) colStats[1]).getMaximum();
    assertEquals("2007-08-01 04:00:00.0", gotMax.toString());

    OrcProto.BloomFilterIndex[] bloomFilterIndices = indices.getBloomFilterIndex();
    OrcProto.BloomFilter bloomFilter = bloomFilterIndices[1].getBloomFilter(0);
    BloomFilter bf = BloomFilterIO.deserialize(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
        buildEncoding(), reader.getWriterVersion(),
      TypeDescription.Category.TIMESTAMP, bloomFilter);
    Assert.assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[1],
      SearchArgumentFactory.newBuilder().equals
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-01 00:00:00.0")).build().getLeaves().get(0),
      bf));

    Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[1],
      SearchArgumentFactory.newBuilder().equals
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-02 00:00:00.0")).build().getLeaves().get(0),
      bf));

    Assert.assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[1],
      SearchArgumentFactory.newBuilder().in
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-01 00:00:00.0"),
          Timestamp.valueOf("2007-08-01 03:00:00.0")).build().getLeaves().get(0),
      bf));

    Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[1],
      SearchArgumentFactory.newBuilder().in
        ("c", PredicateLeaf.Type.TIMESTAMP, Timestamp.valueOf("2007-08-02 00:00:00.0"),
          Timestamp.valueOf("2007-08-02 03:00:00.0")).build().getLeaves().get(0),
      bf));
  }

  @Test
  public void testTimestampMinMaxAndBloomFilter() throws Exception {
    TypeDescription schema = TypeDescription.createStruct().addField("ts", TypeDescription.createTimestamp());

    TimeZone.setDefault(TimeZone.getTimeZone(writerTimeZone));
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000).bloomFilterColumns("ts"));
    assertEquals(writerTimeZone, TimeZone.getDefault().getID());
    List<String> ts = Lists.newArrayList();
    ts.add("2007-08-01 00:00:00.0");
    ts.add("2007-08-01 04:00:00.0");
    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (String t : ts) {
      times.set(batch.size++, Timestamp.valueOf(t));
    }
    writer.addRowBatch(batch);
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(readerTimeZone, TimeZone.getDefault().getID());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    times = (TimestampColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        assertEquals(ts.get(idx++), times.asScratchTimestamp(r).toString());
      }
    }
    boolean[] sargColumns = new boolean[2];
    Arrays.fill(sargColumns, true);
    OrcIndex indices = ((RecordReaderImpl) rows).readRowIndex(0, null, sargColumns);
    rows.close();
    ColumnStatistics[] colStats = reader.getStatistics();
    Timestamp gotMin = ((TimestampColumnStatistics) colStats[1]).getMinimum();
    assertEquals("2007-08-01 00:00:00.0", gotMin.toString());

    Timestamp gotMax = ((TimestampColumnStatistics) colStats[1]).getMaximum();
    assertEquals("2007-08-01 04:00:00.0", gotMax.toString());

    OrcProto.BloomFilterIndex[] bloomFilterIndices = indices.getBloomFilterIndex();
    OrcProto.BloomFilter bloomFilter = bloomFilterIndices[1].getBloomFilter(0);
    BloomFilter bf = BloomFilterIO.deserialize(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
        buildEncoding(), reader.getWriterVersion(),
        TypeDescription.Category.TIMESTAMP, bloomFilter);
    PredicateLeaf pred = createPredicateLeaf(
      PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
      Timestamp.valueOf("2007-08-01 00:00:00.0"), null);
    Assert.assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
      Timestamp.valueOf("2007-08-01 02:00:00.0"), null);
    Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));

    bf.addLong(SerializationUtils.convertToUtc(TimeZone.getDefault(),
        Timestamp.valueOf("2007-08-01 02:00:00.0").getTime()));
    Assert.assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));

    pred = createPredicateLeaf(PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.TIMESTAMP, "x",
      Timestamp.valueOf("2007-08-01 00:00:00.0"), null);
    Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));

    pred = createPredicateLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
      Timestamp.valueOf("2007-08-01 00:00:00.0"), null);
    Assert.assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));

    pred = createPredicateLeaf(PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.TIMESTAMP, "x", null, null);
    Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));
  }

  @Test
  public void testTimestampAllNulls() throws Exception {
    TypeDescription schema = TypeDescription.createStruct().addField("ts", TypeDescription.createTimestamp());

    TimeZone.setDefault(TimeZone.getTimeZone(writerTimeZone));
    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
        .bufferSize(10000).bloomFilterColumns("ts"));
    assertEquals(writerTimeZone, TimeZone.getDefault().getID());
    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (int i = 0; i < 3; i++) {
      times.set(batch.size++, null);
    }
    writer.addRowBatch(batch);
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(readerTimeZone, TimeZone.getDefault().getID());
    RecordReader rows = reader.rows();
    boolean[] sargColumns = new boolean[2];
    Arrays.fill(sargColumns, true);
    OrcIndex indices = ((RecordReaderImpl) rows).readRowIndex(0, null, sargColumns);
    rows.close();
    ColumnStatistics[] colStats = reader.getStatistics();
    Timestamp gotMin = ((TimestampColumnStatistics) colStats[1]).getMinimum();
    Assert.assertNull(gotMin);

    Timestamp gotMax = ((TimestampColumnStatistics) colStats[1]).getMaximum();
    Assert.assertNull(gotMax);

    OrcProto.BloomFilterIndex[] bloomFilterIndices = indices.getBloomFilterIndex();
    OrcProto.BloomFilter bloomFilter = bloomFilterIndices[1].getBloomFilter(0);
    BloomFilter bf = BloomFilterIO.deserialize(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
        buildEncoding(), reader.getWriterVersion(),
        TypeDescription.Category.TIMESTAMP, bloomFilter);
    PredicateLeaf pred = createPredicateLeaf(
      PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
      Timestamp.valueOf("2007-08-01 00:00:00.0"), null);
    Assert.assertEquals(SearchArgument.TruthValue.NULL, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));

    pred = createPredicateLeaf(PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.TIMESTAMP, "x", null, null);
    Assert.assertEquals(SearchArgument.TruthValue.YES, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));
  }
}
