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

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.fail;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.DataReader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.RecordReaderImpl.Location;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.OrcProto;

import org.apache.orc.util.BloomFilterIO;
import org.apache.orc.util.BloomFilterUtf8;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockSettings;
import org.mockito.Mockito;

public class TestRecordReaderImpl {

  @Test
  public void testFindColumn() throws Exception {
    Configuration conf = new Configuration();
    TypeDescription file = TypeDescription.fromString("struct<a:int,c:string,e:int>");
    TypeDescription reader = TypeDescription.fromString("struct<a:int,b:double,c:string,d:double,e:bigint>");
    SchemaEvolution evo = new SchemaEvolution(file, reader, new Reader.Options(conf));
    assertEquals(1, RecordReaderImpl.findColumns(evo, "a"));
    assertEquals(-1, RecordReaderImpl.findColumns(evo, "b"));
    assertEquals(2, RecordReaderImpl.findColumns(evo, "c"));
    assertEquals(-1, RecordReaderImpl.findColumns(evo, "d"));
    assertEquals(3, RecordReaderImpl.findColumns(evo, "e"));
  }

  /**
   * Create a predicate leaf. This is used by another test.
   */
  public static PredicateLeaf createPredicateLeaf(PredicateLeaf.Operator operator,
                                                  PredicateLeaf.Type type,
                                                  String columnName,
                                                  Object literal,
                                                  List<Object> literalList) {
    return new SearchArgumentImpl.PredicateLeafImpl(operator, type, columnName,
        literal, literalList);
  }

  // can add .verboseLogging() to cause Mockito to log invocations
  private final MockSettings settings = Mockito.withSettings().verboseLogging();

  static class BufferInStream
      extends InputStream implements PositionedReadable, Seekable {
    private final byte[] buffer;
    private final int length;
    private int position = 0;

    BufferInStream(byte[] bytes, int length) {
      this.buffer = bytes;
      this.length = length;
    }

    @Override
    public int read() {
      if (position < length) {
        return buffer[position++];
      }
      return -1;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
      int lengthToRead = Math.min(length, this.length - this.position);
      if (lengthToRead >= 0) {
        for(int i=0; i < lengthToRead; ++i) {
          bytes[offset + i] = buffer[position++];
        }
        return lengthToRead;
      } else {
        return -1;
      }
    }

    @Override
    public int read(long position, byte[] bytes, int offset, int length) {
      this.position = (int) position;
      return read(bytes, offset, length);
    }

    @Override
    public void readFully(long position, byte[] bytes, int offset,
                          int length) throws IOException {
      this.position = (int) position;
      while (length > 0) {
        int result = read(bytes, offset, length);
        offset += result;
        length -= result;
        if (result < 0) {
          throw new IOException("Read past end of buffer at " + offset);
        }
      }
    }

    @Override
    public void readFully(long position, byte[] bytes) throws IOException {
      readFully(position, bytes, 0, bytes.length);
    }

    @Override
    public void seek(long position) {
      this.position = (int) position;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public boolean seekToNewSource(long position) throws IOException {
      this.position = (int) position;
      return false;
    }
  }

  @Test
  public void testMaxLengthToReader() throws Exception {
    Configuration conf = new Configuration();
    OrcProto.Type rowType = OrcProto.Type.newBuilder()
        .setKind(OrcProto.Type.Kind.STRUCT).build();
    OrcProto.Footer footer = OrcProto.Footer.newBuilder()
        .setHeaderLength(0).setContentLength(0).setNumberOfRows(0)
        .setRowIndexStride(0).addTypes(rowType).build();
    OrcProto.PostScript ps = OrcProto.PostScript.newBuilder()
        .setCompression(OrcProto.CompressionKind.NONE)
        .setFooterLength(footer.getSerializedSize())
        .setMagic("ORC").addVersion(0).addVersion(11).build();
    DataOutputBuffer buffer = new DataOutputBuffer();
    footer.writeTo(buffer);
    ps.writeTo(buffer);
    buffer.write(ps.getSerializedSize());
    FileSystem fs = mock(FileSystem.class, settings);
    FSDataInputStream file =
        new FSDataInputStream(new BufferInStream(buffer.getData(),
            buffer.getLength()));
    Path p = new Path("/dir/file.orc");
    when(fs.open(p)).thenReturn(file);
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
    options.filesystem(fs);
    options.maxLength(buffer.getLength());
    when(fs.getFileStatus(p))
        .thenReturn(new FileStatus(10, false, 3, 3000, 0, p));
    Reader reader = OrcFile.createReader(p, options);
  }

  @Test
  public void testCompareToRangeInt() throws Exception {
    assertEquals(Location.BEFORE,
      RecordReaderImpl.compareToRange(19L, 20L, 40L));
    assertEquals(Location.AFTER,
      RecordReaderImpl.compareToRange(41L, 20L, 40L));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange(20L, 20L, 40L));
    assertEquals(Location.MIDDLE,
        RecordReaderImpl.compareToRange(21L, 20L, 40L));
    assertEquals(Location.MAX,
      RecordReaderImpl.compareToRange(40L, 20L, 40L));
    assertEquals(Location.BEFORE,
      RecordReaderImpl.compareToRange(0L, 1L, 1L));
    assertEquals(Location.MIN,
      RecordReaderImpl.compareToRange(1L, 1L, 1L));
    assertEquals(Location.AFTER,
      RecordReaderImpl.compareToRange(2L, 1L, 1L));
  }

  @Test
  public void testCompareToRangeString() throws Exception {
    assertEquals(Location.BEFORE,
        RecordReaderImpl.compareToRange("a", "b", "c"));
    assertEquals(Location.AFTER,
        RecordReaderImpl.compareToRange("d", "b", "c"));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange("b", "b", "c"));
    assertEquals(Location.MIDDLE,
        RecordReaderImpl.compareToRange("bb", "b", "c"));
    assertEquals(Location.MAX,
        RecordReaderImpl.compareToRange("c", "b", "c"));
    assertEquals(Location.BEFORE,
        RecordReaderImpl.compareToRange("a", "b", "b"));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange("b", "b", "b"));
    assertEquals(Location.AFTER,
        RecordReaderImpl.compareToRange("c", "b", "b"));
  }

  @Test
  public void testCompareToCharNeedConvert() throws Exception {
    assertEquals(Location.BEFORE,
      RecordReaderImpl.compareToRange("apple", "hello", "world"));
    assertEquals(Location.AFTER,
      RecordReaderImpl.compareToRange("zombie", "hello", "world"));
    assertEquals(Location.MIN,
        RecordReaderImpl.compareToRange("hello", "hello", "world"));
    assertEquals(Location.MIDDLE,
        RecordReaderImpl.compareToRange("pilot", "hello", "world"));
    assertEquals(Location.MAX,
      RecordReaderImpl.compareToRange("world", "hello", "world"));
    assertEquals(Location.BEFORE,
      RecordReaderImpl.compareToRange("apple", "hello", "hello"));
    assertEquals(Location.MIN,
      RecordReaderImpl.compareToRange("hello", "hello", "hello"));
    assertEquals(Location.AFTER,
      RecordReaderImpl.compareToRange("zombie", "hello", "hello"));
  }

  @Test
  public void testGetMin() throws Exception {
    assertEquals(10L, RecordReaderImpl.getMin(
      ColumnStatisticsImpl.deserialize(null, createIntStats(10L, 100L))));
    assertEquals(10.0d, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
      null,
      OrcProto.ColumnStatistics.newBuilder()
        .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
          .setMinimum(10.0d).setMaximum(100.0d).build()).build())));
    assertEquals(null, RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
      null,
      OrcProto.ColumnStatistics.newBuilder()
        .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
        .build())));
    assertEquals("a", RecordReaderImpl.getMin(ColumnStatisticsImpl.deserialize(
      null,
      OrcProto.ColumnStatistics.newBuilder()
        .setStringStatistics(OrcProto.StringStatistics.newBuilder()
          .setMinimum("a").setMaximum("b").build()).build())));
    assertEquals("hello", RecordReaderImpl.getMin(ColumnStatisticsImpl
      .deserialize(null, createStringStats("hello", "world"))));
    assertEquals(HiveDecimal.create("111.1"), RecordReaderImpl.getMin(ColumnStatisticsImpl
      .deserialize(null, createDecimalStats("111.1", "112.1"))));
  }

  private static OrcProto.ColumnStatistics createIntStats(Long min,
                                                          Long max) {
    OrcProto.IntegerStatistics.Builder intStats =
        OrcProto.IntegerStatistics.newBuilder();
    if (min != null) {
      intStats.setMinimum(min);
    }
    if (max != null) {
      intStats.setMaximum(max);
    }
    return OrcProto.ColumnStatistics.newBuilder()
        .setIntStatistics(intStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createBooleanStats(int n, int trueCount) {
    OrcProto.BucketStatistics.Builder boolStats = OrcProto.BucketStatistics.newBuilder();
    boolStats.addCount(trueCount);
    return OrcProto.ColumnStatistics.newBuilder().setNumberOfValues(n).setBucketStatistics(
      boolStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createIntStats(int min, int max) {
    OrcProto.IntegerStatistics.Builder intStats = OrcProto.IntegerStatistics.newBuilder();
    intStats.setMinimum(min);
    intStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setIntStatistics(intStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDoubleStats(double min, double max) {
    OrcProto.DoubleStatistics.Builder dblStats = OrcProto.DoubleStatistics.newBuilder();
    dblStats.setMinimum(min);
    dblStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDoubleStatistics(dblStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createStringStats(String min, String max,
      boolean hasNull) {
    OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.newBuilder();
    strStats.setMinimum(min);
    strStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setStringStatistics(strStats.build())
        .setHasNull(hasNull).build();
  }

  private static OrcProto.ColumnStatistics createStringStats(String min, String max) {
    OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.newBuilder();
    strStats.setMinimum(min);
    strStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setStringStatistics(strStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDateStats(int min, int max) {
    OrcProto.DateStatistics.Builder dateStats = OrcProto.DateStatistics.newBuilder();
    dateStats.setMinimum(min);
    dateStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDateStatistics(dateStats.build()).build();
  }

  private static final TimeZone utcTz = TimeZone.getTimeZone("UTC");

  private static OrcProto.ColumnStatistics createTimestampStats(String min, String max) {
    OrcProto.TimestampStatistics.Builder tsStats = OrcProto.TimestampStatistics.newBuilder();
    tsStats.setMinimumUtc(getUtcTimestamp(min));
    tsStats.setMaximumUtc(getUtcTimestamp(max));
    return OrcProto.ColumnStatistics.newBuilder().setTimestampStatistics(tsStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max) {
    OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.newBuilder();
    decStats.setMinimum(min);
    decStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDecimalStatistics(decStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max,
      boolean hasNull) {
    OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.newBuilder();
    decStats.setMinimum(min);
    decStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setDecimalStatistics(decStats.build())
        .setHasNull(hasNull).build();
  }

  @Test
  public void testGetMax() throws Exception {
    assertEquals(100L, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        null, createIntStats(10L, 100L))));
    assertEquals(100.0d, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        null,
        OrcProto.ColumnStatistics.newBuilder()
            .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                .setMinimum(10.0d).setMaximum(100.0d).build()).build())));
    assertEquals(null, RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        null,
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
            .build())));
    assertEquals("b", RecordReaderImpl.getMax(ColumnStatisticsImpl.deserialize(
        null,
        OrcProto.ColumnStatistics.newBuilder()
            .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build())));
    assertEquals("world", RecordReaderImpl.getMax(ColumnStatisticsImpl
      .deserialize(null, createStringStats("hello", "world"))));
    assertEquals(HiveDecimal.create("112.1"), RecordReaderImpl.getMax(ColumnStatisticsImpl
      .deserialize(null, createDecimalStats("111.1", "112.1"))));
  }

  static TruthValue evaluateBoolean(OrcProto.ColumnStatistics stats,
                                    PredicateLeaf predicate) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        OrcFile.WriterVersion.ORC_135, TypeDescription.Category.BOOLEAN);
  }

  static TruthValue evaluateInteger(OrcProto.ColumnStatistics stats,
                                    PredicateLeaf predicate) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        OrcFile.WriterVersion.ORC_135, TypeDescription.Category.LONG);
  }

  static TruthValue evaluateDouble(OrcProto.ColumnStatistics stats,
                                    PredicateLeaf predicate) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        OrcFile.WriterVersion.ORC_135, TypeDescription.Category.DOUBLE);
  }

  static TruthValue evaluateTimestamp(OrcProto.ColumnStatistics stats,
                                      PredicateLeaf predicate,
                                      boolean include135,
                                      boolean useUTCTimestamp) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        include135 ? OrcFile.WriterVersion.ORC_135: OrcFile.WriterVersion.ORC_101,
        TypeDescription.Category.TIMESTAMP, useUTCTimestamp);
  }

  static TruthValue evaluateTimestampBloomfilter(OrcProto.ColumnStatistics stats,
                                                 PredicateLeaf predicate,
                                                 BloomFilter bloom,
                                                 OrcFile.WriterVersion version,
                                                 boolean useUTCTimestamp) {
    OrcProto.ColumnEncoding.Builder encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    if (version.includes(OrcFile.WriterVersion.ORC_135)) {
      encoding.setBloomEncoding(BloomFilterIO.Encoding.UTF8_UTC.getId());
    }
    OrcProto.Stream.Kind kind =
        version.includes(OrcFile.WriterVersion.ORC_101) ?
            OrcProto.Stream.Kind.BLOOM_FILTER_UTF8 :
            OrcProto.Stream.Kind.BLOOM_FILTER;
    OrcProto.BloomFilter.Builder builder =
        OrcProto.BloomFilter.newBuilder();
    BloomFilterIO.serialize(builder, bloom);
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, kind,
        encoding.build(), builder.build(), version,
        TypeDescription.Category.TIMESTAMP, useUTCTimestamp);
  }

  @Test
  public void testPredEvalWithBooleanStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", true, null);
    assertEquals(TruthValue.YES_NO,
        evaluateBoolean(createBooleanStats(10, 10), pred));
    assertEquals(TruthValue.NO,
        evaluateBoolean(createBooleanStats(10, 0), pred));

    pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", true, null);
    assertEquals(TruthValue.YES_NO,
        evaluateBoolean(createBooleanStats(10, 10), pred));
    assertEquals(TruthValue.NO,
        evaluateBoolean(createBooleanStats(10, 0), pred));

    pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", false, null);
    assertEquals(TruthValue.NO,
      evaluateBoolean(createBooleanStats(10, 10), pred));
    assertEquals(TruthValue.YES_NO,
      evaluateBoolean(createBooleanStats(10, 0), pred));
  }

  @Test
  public void testPredEvalWithIntStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10, 100), pred));

    // Stats gets converted to column type. "15" is outside of "10" and "100"
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createIntStats(10, 100), pred));

    // Integer stats will not be converted date because of days/seconds/millis ambiguity
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    try {
      evaluateInteger(createIntStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Long to DATE", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    try {
      evaluateInteger(createIntStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Long to TIMESTAMP", ia.getMessage());
    }
  }

  @Test
  public void testPredEvalWithDoubleStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    // Stats gets converted to column type. "15.0" is outside of "10.0" and "100.0"
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    // Double is not converted to date type because of days/seconds/millis ambiguity
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    try {
      evaluateDouble(createDoubleStats(10.0, 100.0), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Double to DATE", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15*1000L), null);
    assertEquals(TruthValue.YES_NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150*1000L), null);
    assertEquals(TruthValue.NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));
  }

  @Test
  public void testPredEvalWithStringStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 100L, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("10", "1000"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 100.0, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("10", "1000"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "100", null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("10", "1000"), pred));

    // IllegalArgumentException is thrown when converting String to Date, hence YES_NO
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(100).get(), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 1000), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("100"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("10", "1000"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(100), null);
    try {
      evaluateInteger(createStringStats("10", "1000"), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from String to TIMESTAMP", ia.getMessage());
    }
  }

  @Test
  public void testPredEvalWithDateStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    // Date to Integer conversion is not possible.
    try {
      evaluateInteger(createDateStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Date to LONG", ia.getMessage());
    }

    // Date to Float conversion is also not possible.
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    try {
      evaluateInteger(createDateStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Date to FLOAT", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "1970-01-11", null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15.1", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "__a15__1", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "2000-01-16", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "1970-01-16", null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(150).get(), null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    // Date to Decimal conversion is also not possible.
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    try {
      evaluateInteger(createDateStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Date to DECIMAL", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15L * 24L * 60L * 60L * 1000L), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 100), pred));
  }

  @Test
  public void testPredEvalWithDecimalStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    // "15" out of range of "10.0" and "100.0"
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    // Decimal to Date not possible.
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", new DateWritable(15).get(), null);
    try {
      evaluateInteger(createDecimalStats("10.0", "100.0"), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from HiveDecimal to DATE", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15 * 1000L), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150 * 1000L), null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));
  }

  @Test
  public void testPredEvalWithTimestampStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP,
        "x", Timestamp.valueOf("2017-01-01 00:00:00"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00",
            "2018-01-01 00:00:00"), pred, true, false));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2018-01-01 00:00:00"),
            pred, true, false));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2018-01-01 00:00:00"),
            pred, true, false));

    // pre orc-135 should always be yes_no_null.
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x",  Timestamp.valueOf("2017-01-01 00:00:00"), null);
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2017-01-01 00:00:00"),
            pred, false, false));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", Timestamp.valueOf("2017-01-01 00:00:00").toString(), null);
    assertEquals(TruthValue.YES_NO,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2018-01-01 00:00:00"),
            pred, true, false));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", Date.valueOf("2016-01-01"), null);
    assertEquals(TruthValue.NO,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2017-01-01 00:00:00"),
            pred, true, false));
    assertEquals(TruthValue.YES_NO,
        evaluateTimestamp(createTimestampStats("2015-01-01 00:00:00", "2016-01-01 00:00:00"),
            pred, true, false));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(createTimestampStats("2015-01-01 00:00:00", "2016-01-01 00:00:00"),
            pred, true, false));
  }

  @Test
  public void testEquals() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(15L, 30L), pred)) ;
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(0L, 10L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(15L, 15L), pred));
  }

  @Test
  public void testNullSafeEquals() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG,
            "x", 15L, null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.NO,
        evaluateInteger(createIntStats(0L, 10L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(15L, 15L), pred));
  }

  @Test
  public void testLessThan() throws Exception {
    PredicateLeaf lessThan = createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.LONG,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(20L, 30L), lessThan));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(15L, 30L), lessThan));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 30L), lessThan));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 15L), lessThan));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(0L, 10L), lessThan));
  }

  @Test
  public void testLessThanEquals() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.LONG,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(0L, 10L), pred));
  }

  @Test
  public void testIn() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.LONG,
            "x", null, args);
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(20L, 20L), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(30L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(12L, 18L), pred));
  }

  @Test
  public void testBetween() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.LONG,
            "x", null, args);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(0L, 5L), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(30L, 40L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(5L, 15L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(15L, 25L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(5L, 25L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(10L, 20L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(12L, 18L), pred));

    // check with empty predicate list
    args.clear();
    pred = createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.LONG,
            "x", null, args);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(0L, 5L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(30L, 40L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(5L, 15L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10L, 20L), pred));
  }

  @Test
  public void testIsNull() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.LONG,
            "x", null, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(20L, 30L), pred));
  }


  @Test
  public void testEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testNullSafeEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.NO,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testLessThanWithNullInStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.NO_NULL, // min, same stats
        evaluateInteger(createStringStats("c", "c", true), pred));
  }

  @Test
  public void testLessThanEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testInWithNullInStats() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("c");
    args.add("f");
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
            "x", null, args);
    assertEquals(TruthValue.NO_NULL, // before & after
        evaluateInteger(createStringStats("d", "e", true), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("e", "f", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testBetweenWithNullInStats() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("c");
    args.add("f");
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.STRING,
            "x", null, args);
    assertEquals(TruthValue.YES_NULL, // before & after
        evaluateInteger(createStringStats("d", "e", true), pred));
    assertEquals(TruthValue.YES_NULL, // before & max
        evaluateInteger(createStringStats("e", "f", true), pred));
    assertEquals(TruthValue.NO_NULL, // before & before
        evaluateInteger(createStringStats("h", "g", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // before & min
        evaluateInteger(createStringStats("f", "g", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // before & middle
        evaluateInteger(createStringStats("e", "g", true), pred));

    assertEquals(TruthValue.YES_NULL, // min & after
        evaluateInteger(createStringStats("c", "e", true), pred));
    assertEquals(TruthValue.YES_NULL, // min & max
        evaluateInteger(createStringStats("c", "f", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // min & middle
        evaluateInteger(createStringStats("c", "g", true), pred));

    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("a", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL, // min & after, same stats
        evaluateInteger(createStringStats("c", "c", true), pred));
  }

  @Test
  public void testTimestampStatsOldFiles() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
      (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP,
        "x", Timestamp.valueOf("2000-01-01 00:00:00"), null);
    OrcProto.ColumnStatistics cs = createTimestampStats("2000-01-01 00:00:00", "2001-01-01 00:00:00");
    assertEquals(TruthValue.YES_NO_NULL,
      evaluateTimestampBloomfilter(cs, pred, new BloomFilterUtf8(10000, 0.01), OrcFile.WriterVersion.ORC_101, false));
    BloomFilterUtf8 bf = new BloomFilterUtf8(10, 0.05);
    bf.addLong(getUtcTimestamp("2000-06-01 00:00:00"));
    assertEquals(TruthValue.NO_NULL,
      evaluateTimestampBloomfilter(cs, pred, bf, OrcFile.WriterVersion.ORC_135, false));
    assertEquals(TruthValue.YES_NO_NULL,
      evaluateTimestampBloomfilter(cs, pred, bf, OrcFile.WriterVersion.ORC_101, false));
  }

  @Test
  public void testTimestampUTC() throws Exception {
    DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    f.setTimeZone(TimeZone.getTimeZone("UTC"));
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP,
            "x", new Timestamp(f.parse("2015-01-01 00:00:00").getTime()), null);
    PredicateLeaf pred2 = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP,
            "x", new Timestamp(f.parse("2014-12-31 23:59:59").getTime()), null);
    PredicateLeaf pred3 = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP,
            "x", new Timestamp(f.parse("2016-01-01 00:00:01").getTime()), null);
    OrcProto.ColumnStatistics cs = createTimestampStats("2015-01-01 00:00:00", "2016-01-01 00:00:00");

    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(cs, pred, true, true));
    assertEquals(TruthValue.NO_NULL,
        evaluateTimestamp(cs, pred2, true, true));
    assertEquals(TruthValue.NO_NULL,
        evaluateTimestamp(cs, pred3, true, true));

    assertEquals(TruthValue.NO_NULL,
        evaluateTimestampBloomfilter(cs, pred, new BloomFilterUtf8(10000, 0.01), OrcFile.WriterVersion.ORC_135, true));
    assertEquals(TruthValue.NO_NULL,
        evaluateTimestampBloomfilter(cs, pred2, new BloomFilterUtf8(10000, 0.01), OrcFile.WriterVersion.ORC_135, true));

    BloomFilterUtf8 bf = new BloomFilterUtf8(10, 0.05);
    bf.addLong(getUtcTimestamp("2015-06-01 00:00:00"));
    assertEquals(TruthValue.NO_NULL,
        evaluateTimestampBloomfilter(cs, pred, bf, OrcFile.WriterVersion.ORC_135, true));

    bf.addLong(getUtcTimestamp("2015-01-01 00:00:00"));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestampBloomfilter(cs, pred, bf, OrcFile.WriterVersion.ORC_135, true));
  }

  private static long getUtcTimestamp(String ts)  {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormat.setTimeZone(utcTz);
    try {
      return dateFormat.parse(ts).getTime();
    } catch (ParseException e) {
      throw new IllegalArgumentException("Can't parse " + ts, e);
    }
  }

  @Test
  public void testIsNullWithNullInStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.STRING,
            "x", null, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("c", "d", true), pred));
    assertEquals(TruthValue.NO,
        evaluateInteger(createStringStats("c", "d", false), pred));
  }

  @Test
  public void testOverlap() throws Exception {
    assertTrue(!RecordReaderUtils.overlap(0, 10, -10, -1));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 0));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 1));
    assertTrue(RecordReaderUtils.overlap(0, 10, 2, 8));
    assertTrue(RecordReaderUtils.overlap(0, 10, 5, 10));
    assertTrue(RecordReaderUtils.overlap(0, 10, 10, 11));
    assertTrue(RecordReaderUtils.overlap(0, 10, 0, 10));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 11));
    assertTrue(!RecordReaderUtils.overlap(0, 10, 11, 12));
  }

  private static DiskRangeList diskRanges(Integer... points) {
    DiskRangeList head = null, tail = null;
    for(int i = 0; i < points.length; i += 2) {
      DiskRangeList range = new DiskRangeList(points[i], points[i+1]);
      if (tail == null) {
        head = tail = range;
      } else {
        tail = tail.insertAfter(range);
      }
    }
    return head;
  }

  @Test
  public void testGetIndexPosition() throws Exception {
    assertEquals(0, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.PRESENT, true, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(0, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.INT,
            OrcProto.Stream.Kind.DATA, true, false));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DICTIONARY, OrcProto.Type.Kind.STRING,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(6, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.LENGTH, true, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.BINARY,
            OrcProto.Stream.Kind.LENGTH, false, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(6, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, true, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, false, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, true, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, false, true));
    assertEquals(7, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, true, true));
    assertEquals(5, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, OrcProto.Type.Kind.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, false, true));
  }

  @Test
  public void testPartialPlan() throws Exception {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(99000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{true, true, false, false, true, false};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: int, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
                .addSubtypes(1).addSubtypes(2).addFieldNames("x")
                .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(0, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(0, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 51000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));

    // if we read no rows, don't read any bytes
    rowGroups = new boolean[]{false, false, false, false, false, false};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertNull(result);

    // all rows, but only columns 0 and 2.
    rowGroups = null;
    columns = new boolean[]{true, false, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, null, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100000, 200000)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, null, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100000, 200000)));

    rowGroups = new boolean[]{false, true, false, false, false, false};
    indexes[2] = indexes[1];
    indexes[1] = null;
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100100, 102000,
        112000, 122000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100100, 102000,
        112000, 122000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP)));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    indexes[1] = indexes[2];
    columns = new boolean[]{true, true, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000, 100500, 102000,
        152000, 200000)));
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000, 100500, 102000,
        152000, 200000)));
  }


  @Test
  public void testPartialPlanCompressed() throws Exception {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(99000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{true, true, false, false, true, false};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: int, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
        .addSubtypes(1).addSubtypes(2).addFieldNames("x")
        .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, true, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(0, 100000)));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, true, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(500, 1000, 51000, 100000)));
  }

  @Test
  public void testPartialPlanString() throws Exception {
    DiskRangeList result;

    // set the streams
    List<OrcProto.Stream> streams = new ArrayList<OrcProto.Stream>();
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(1).setLength(1000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(1).setLength(94000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.LENGTH)
        .setColumn(1).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DICTIONARY_DATA)
        .setColumn(1).setLength(3000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.PRESENT)
        .setColumn(2).setLength(2000).build());
    streams.add(OrcProto.Stream.newBuilder()
        .setKind(OrcProto.Stream.Kind.DATA)
        .setColumn(2).setLength(98000).build());

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{false, true, false, false, true, true};

    // set the index
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[columns.length];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(0).addPositions(-1).addPositions(-1)
            .addPositions(0)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(100).addPositions(-1).addPositions(-1)
            .addPositions(10000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(200).addPositions(-1).addPositions(-1)
            .addPositions(20000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(300).addPositions(-1).addPositions(-1)
            .addPositions(30000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(400).addPositions(-1).addPositions(-1)
            .addPositions(40000)
            .build())
        .addEntry(OrcProto.RowIndexEntry.newBuilder()
            .addPositions(500).addPositions(-1).addPositions(-1)
            .addPositions(50000)
            .build())
        .build();

    // set encodings
    List<OrcProto.ColumnEncoding> encodings =
        new ArrayList<OrcProto.ColumnEncoding>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    // set types struct{x: string, y: int}
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT)
        .addSubtypes(1).addSubtypes(2).addFieldNames("x")
        .addFieldNames("y").build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build());

    // filter by rows and groups
    result = RecordReaderImpl.planReadPartialDataStreams(streams, indexes,
        columns, rowGroups, false, encodings, types, 32768, true);
    assertThat(result, is(diskRanges(100, 1000,
        11000, 21000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        41000, 100000)));
  }

  @Test
  public void testIntNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createIntStats(10, 100));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testIntEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createIntStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testIntInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(15L);
    args.add(19L);
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.LONG,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createIntStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(19);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(15.0);
    args.add(19.0);
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.FLOAT,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(19.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("str_15");
    args.add("str_19");
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_19");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.DATE, "x",
        new DateWritable(15).get(), null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new DateWritable(i)).getDays());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDateStats(10, 100));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(15)).getDays());
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DATE, "x",
        new DateWritable(15).get(), null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new DateWritable(i)).getDays());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDateStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(15)).getDays());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(new DateWritable(15).get());
    args.add(new DateWritable(19).get());
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DATE,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong((new DateWritable(i)).getDays());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDateStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(19)).getDays());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong((new DateWritable(15)).getDays());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDecimalEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DECIMAL, "x",
        new HiveDecimalWritable("15"),
        null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDecimalStats("10", "200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDecimalInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(new HiveDecimalWritable("15"));
    args.add(new HiveDecimalWritable("19"));
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDecimalStats("10", "200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(19).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testNullsInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(new HiveDecimalWritable("15"));
    args.add(null);
    args.add(new HiveDecimalWritable("19"));
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDecimalStats("10", "200", false));
    // hasNull is false, so bloom filter should return NO
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    cs = ColumnStatisticsImpl.deserialize(null, createDecimalStats("10", "200", true));
    // hasNull is true, so bloom filter should return YES_NO_NULL
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(19).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testClose() throws Exception {
    DataReader mockedDataReader = mock(DataReader.class);
    DataReader cloned = mock(DataReader.class);
    when(mockedDataReader.clone()).thenReturn(cloned);
    closeMockedRecordReader(mockedDataReader);

    verify(cloned, atLeastOnce()).close();
  }

  @Test
  public void testCloseWithException() throws Exception {
    DataReader mockedDataReader = mock(DataReader.class);
    DataReader cloned = mock(DataReader.class);
    when(mockedDataReader.clone()).thenReturn(cloned);
    doThrow(IOException.class).when(cloned).close();

    try {
      closeMockedRecordReader(mockedDataReader);
      fail("Exception should have been thrown when Record Reader was closed");
    } catch (IOException expected) {

    }

    verify(cloned, atLeastOnce()).close();
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  private void closeMockedRecordReader(DataReader mockedDataReader) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "empty.orc");
    FileSystem.get(conf).delete(path, true);
    Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf)
        .setSchema(TypeDescription.createLong()));
    writer.close();
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));

    RecordReader recordReader = reader.rows(reader.options()
        .dataReader(mockedDataReader));

    recordReader.close();
  }

  @Test
  public void TestOldBloomFilters() throws Exception {
    OrcProto.StripeFooter footer =
        OrcProto.StripeFooter.newBuilder()
            .addStreams(OrcProto.Stream.newBuilder()
               .setColumn(1).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(1).setKind(OrcProto.Stream.Kind.BLOOM_FILTER).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(2).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(2).setKind(OrcProto.Stream.Kind.BLOOM_FILTER).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(3).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(3).setKind(OrcProto.Stream.Kind.BLOOM_FILTER).setLength(1000).build())
        .build();
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:decimal(10,2),z:string>");
    OrcProto.Stream.Kind[] bloomFilterKinds = new OrcProto.Stream.Kind[4];

    // normal read
    DiskRangeList ranges = RecordReaderUtils.planIndexReading(schema, footer,
        false, new boolean[]{true, true, false, true},
        new boolean[]{false, true, false, true},
        OrcFile.WriterVersion.HIVE_4243,
        bloomFilterKinds);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[3]);
    assertEquals("range start: 0 end: 2000", ranges.toString());
    assertEquals("range start: 4000 end: 6000", ranges.next.toString());
    assertEquals(null, ranges.next.next);

    // ignore non-utf8 bloom filter
    Arrays.fill(bloomFilterKinds, null);
    ranges = RecordReaderUtils.planIndexReading(schema, footer,
        true, new boolean[]{true, true, false, true},
        new boolean[]{false, true, false, true},
        OrcFile.WriterVersion.HIVE_4243,
        bloomFilterKinds);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(null, bloomFilterKinds[3]);
    assertEquals("range start: 0 end: 2000", ranges.toString());
    assertEquals("range start: 4000 end: 5000", ranges.next.toString());
    assertEquals(null, ranges.next.next);

    // check that we are handling the post hive-12055 strings correctly
    Arrays.fill(bloomFilterKinds, null);
    ranges = RecordReaderUtils.planIndexReading(schema, footer,
        true, null, new boolean[]{false, true, true, true},
        OrcFile.WriterVersion.HIVE_12055, bloomFilterKinds);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(null, bloomFilterKinds[2]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[3]);
    assertEquals("range start: 0 end: 3000", ranges.toString());
    assertEquals("range start: 4000 end: 6000", ranges.next.toString());
    assertEquals(null, ranges.next.next);

    // ignore non-utf8 bloom filter on decimal
    Arrays.fill(bloomFilterKinds, null);
    ranges = RecordReaderUtils.planIndexReading(schema, footer,
        true, null,
        new boolean[]{false, false, true, false},
        OrcFile.WriterVersion.HIVE_4243,
        bloomFilterKinds);
    assertEquals(null, bloomFilterKinds[2]);
    assertEquals("range start: 0 end: 1000", ranges.toString());
    assertEquals("range start: 2000 end: 3000", ranges.next.toString());
    assertEquals("range start: 4000 end: 5000", ranges.next.next.toString());
    assertEquals(null, ranges.next.next.next);
  }

  @Test
  public void TestCompatibleBloomFilters() throws Exception {
    OrcProto.StripeFooter footer =
        OrcProto.StripeFooter.newBuilder()
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(1).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(1).setKind(OrcProto.Stream.Kind.BLOOM_FILTER).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(2).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(2).setKind(OrcProto.Stream.Kind.BLOOM_FILTER).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(2).setKind(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(3).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(3).setKind(OrcProto.Stream.Kind.BLOOM_FILTER).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(3).setKind(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).setLength(1000).build())
            .build();
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:decimal(10,2),z:string>");
    OrcProto.Stream.Kind[] bloomFilterKinds = new OrcProto.Stream.Kind[4];

    // normal read
    DiskRangeList ranges = RecordReaderUtils.planIndexReading(schema, footer,
        false, new boolean[]{true, true, false, true},
        new boolean[]{false, true, false, true},
        OrcFile.WriterVersion.HIVE_4243,
        bloomFilterKinds);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, bloomFilterKinds[3]);
    assertEquals("range start: 0 end: 2000", ranges.toString());
    assertEquals("range start: 5000 end: 6000", ranges.next.toString());
    assertEquals("range start: 7000 end: 8000", ranges.next.next.toString());
    assertEquals(null, ranges.next.next.next);

    //
    Arrays.fill(bloomFilterKinds, null);
    ranges = RecordReaderUtils.planIndexReading(schema, footer,
        true, null,
        new boolean[]{false, true, true, false},
        OrcFile.WriterVersion.HIVE_4243,
        bloomFilterKinds);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, bloomFilterKinds[2]);
    assertEquals("range start: 0 end: 3000", ranges.toString());
    assertEquals("range start: 4000 end: 6000", ranges.next.toString());
    assertEquals(null, ranges.next.next);
  }

  @Test
  public void TestNewBloomFilters() throws Exception {
    OrcProto.StripeFooter footer =
        OrcProto.StripeFooter.newBuilder()
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(1).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(1).setKind(OrcProto.Stream.Kind.BLOOM_FILTER).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(2).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(2).setKind(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(3).setKind(OrcProto.Stream.Kind.ROW_INDEX).setLength(1000).build())
            .addStreams(OrcProto.Stream.newBuilder()
                .setColumn(3).setKind(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).setLength(1000).build())
            .build();
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:decimal(10,2),z:string>");
    OrcProto.Stream.Kind[] bloomFilterKinds = new OrcProto.Stream.Kind[4];

    // normal read
    DiskRangeList ranges = RecordReaderUtils.planIndexReading(schema, footer,
        false, new boolean[]{true, true, false, true},
        new boolean[]{false, true, false, true},
        OrcFile.WriterVersion.HIVE_4243,
        bloomFilterKinds);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, bloomFilterKinds[3]);
    assertEquals("range start: 0 end: 2000", ranges.toString());
    assertEquals("range start: 4000 end: 6000", ranges.next.toString());
    assertEquals(null, ranges.next.next);

    //
    Arrays.fill(bloomFilterKinds, null);
    ranges = RecordReaderUtils.planIndexReading(schema, footer,
        true, null,
        new boolean[]{false, true, true, false},
        OrcFile.WriterVersion.HIVE_4243,
        bloomFilterKinds);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, bloomFilterKinds[2]);
    assertEquals("range start: 0 end: 5000", ranges.toString());
    assertEquals(null, ranges.next);
  }

  static OrcProto.RowIndexEntry createIndexEntry(Long min, Long max) {
    return OrcProto.RowIndexEntry.newBuilder()
             .setStatistics(createIntStats(min, max)).build();
  }

  @Test
  public void testPickRowGroups() throws Exception {
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
    SchemaEvolution evolution = new SchemaEvolution(schema, schema,
        new Reader.Options(conf));
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .equals("x", PredicateLeaf.Type.LONG, 100L)
            .equals("y", PredicateLeaf.Type.LONG, 10L)
            .end().build();
    RecordReaderImpl.SargApplier applier =
        new RecordReaderImpl.SargApplier(sarg, 1000, evolution,
            OrcFile.WriterVersion.ORC_135, false);
    OrcProto.StripeInformation stripe =
        OrcProto.StripeInformation.newBuilder().setNumberOfRows(4000).build();
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[3];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(createIndexEntry(0L, 10L))
        .addEntry(createIndexEntry(100L, 200L))
        .addEntry(createIndexEntry(300L, 500L))
        .addEntry(createIndexEntry(100L, 100L))
        .build();
    indexes[2] = OrcProto.RowIndex.newBuilder()
        .addEntry(createIndexEntry(0L, 9L))
        .addEntry(createIndexEntry(11L, 20L))
        .addEntry(createIndexEntry(10L, 10L))
        .addEntry(createIndexEntry(0L, 100L))
        .build();
    List<OrcProto.ColumnEncoding> encodings = new ArrayList<>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    boolean[] rows = applier.pickRowGroups(new ReaderImpl.StripeInformationImpl(stripe),
        indexes, null, encodings, null, false);
    assertEquals(4, rows.length);
    assertEquals(false, rows[0]);
    assertEquals(false, rows[1]);
    assertEquals(false, rows[2]);
    assertEquals(true, rows[3]);
    assertEquals(0, applier.getExceptionCount()[0]);
    assertEquals(0, applier.getExceptionCount()[1]);
  }

  @Test
  public void testPickRowGroupsError() throws Exception {
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
    SchemaEvolution evolution = new SchemaEvolution(schema, schema,
        new Reader.Options(conf));
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
              .equals("x", PredicateLeaf.Type.DATE, Date.valueOf("2017-01-02"))
              .equals("y", PredicateLeaf.Type.LONG, 10L)
            .end().build();
    RecordReaderImpl.SargApplier applier =
        new RecordReaderImpl.SargApplier(sarg, 1000, evolution,
            OrcFile.WriterVersion.ORC_135, false);
    OrcProto.StripeInformation stripe =
        OrcProto.StripeInformation.newBuilder().setNumberOfRows(3000).build();
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[3];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(createIndexEntry(0L, 10L))
        .addEntry(createIndexEntry(10L, 20L))
        .addEntry(createIndexEntry(20L, 30L))
        .build();
    indexes[2] = OrcProto.RowIndex.newBuilder()
        .addEntry(createIndexEntry(0L, 9L))
        .addEntry(createIndexEntry(10L, 20L))
        .addEntry(createIndexEntry(0L, 30L))
        .build();
    List<OrcProto.ColumnEncoding> encodings = new ArrayList<>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    boolean[] rows = applier.pickRowGroups(new ReaderImpl.StripeInformationImpl(stripe),
        indexes, null, encodings, null, false);
    assertEquals(3, rows.length);
    assertEquals(false, rows[0]);
    assertEquals(true, rows[1]);
    assertEquals(true, rows[2]);
    assertEquals(1, applier.getExceptionCount()[0]);
    assertEquals(0, applier.getExceptionCount()[1]);
  }

  @Test
  public void testSkipDataReaderOpen() throws Exception {
    IOException ioe = new IOException("Don't open when there is no stripe");

    DataReader mockedDataReader = mock(DataReader.class);
    doThrow(ioe).when(mockedDataReader).open();
    when(mockedDataReader.clone()).thenReturn(mockedDataReader);
    doNothing().when(mockedDataReader).close();

    Configuration conf = new Configuration();
    Path path = new Path(workDir, "empty.orc");
    FileSystem.get(conf).delete(path, true);
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf).setSchema(TypeDescription.createLong());
    Writer writer = OrcFile.createWriter(path, options);
    writer.close();

    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    Reader.Options readerOptions = reader.options().dataReader(mockedDataReader);
    RecordReader recordReader = reader.rows(readerOptions);
    recordReader.close();
  }

  @Test
  public void testCloseAtConstructorException() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "oneRow.orc");
    FileSystem.get(conf).delete(path, true);

    TypeDescription schema = TypeDescription.createLong();
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf).setSchema(schema);
    Writer writer = OrcFile.createWriter(path, options);
    VectorizedRowBatch writeBatch = schema.createRowBatch();
    int row = writeBatch.size++;
    ((LongColumnVector) writeBatch.cols[0]).vector[row] = 0;
    writer.addRowBatch(writeBatch);
    writer.close();

    DataReader mockedDataReader = mock(DataReader.class);
    when(mockedDataReader.clone()).thenReturn(mockedDataReader);
    doThrow(new IOException()).when(mockedDataReader).readStripeFooter(any());

    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    Reader.Options readerOptions = reader.options().dataReader(mockedDataReader);
    boolean isCalled = false;
    try {
      reader.rows(readerOptions);
    } catch (IOException ie) {
      isCalled = true;
    }
    assertTrue(isCalled);
    verify(mockedDataReader, times(1)).close();
  }
}
