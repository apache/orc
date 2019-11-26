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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.SerializationUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class TimestampTreeWriter extends TreeWriterBase {
  public static final int MILLIS_PER_SECOND = 1000;
  public static final String BASE_TIMESTAMP_STRING = "2015-01-01 00:00:00";

  private final IntegerWriter seconds;
  private final IntegerWriter nanos;
  private final boolean isDirectV2;
  private boolean useUTCTimestamp;
  private final TimeZone localTimezone;
  private final long baseEpochSecsLocalTz;
  private final long baseEpochSecsUTC;
  private final boolean useProleptic;

  public TimestampTreeWriter(int columnId,
                             TypeDescription schema,
                             WriterContext writer,
                             boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    this.isDirectV2 = isNewWriteFormat(writer);
    this.seconds = createIntegerWriter(writer.createStream(id,
        OrcProto.Stream.Kind.DATA), true, isDirectV2, writer);
    this.nanos = createIntegerWriter(writer.createStream(id,
        OrcProto.Stream.Kind.SECONDARY), false, isDirectV2, writer);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
    this.useUTCTimestamp = writer.getUseUTCTimestamp();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    this.localTimezone = TimeZone.getDefault();
    dateFormat.setTimeZone(this.localTimezone);
    try {
      this.baseEpochSecsLocalTz = dateFormat
          .parse(TimestampTreeWriter.BASE_TIMESTAMP_STRING).getTime() /
          TimestampTreeWriter.MILLIS_PER_SECOND;
    } catch (ParseException e) {
      throw new IOException("Unable to create base timestamp tree writer", e);
    }
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    try {
      this.baseEpochSecsUTC = dateFormat
          .parse(TimestampTreeWriter.BASE_TIMESTAMP_STRING).getTime() /
          TimestampTreeWriter.MILLIS_PER_SECOND;
    } catch (ParseException e) {
      throw new IOException("Unable to create base timestamp tree writer", e);
    }
    useProleptic = writer.getProlepticGregorian();
  }

  @Override
  OrcProto.ColumnEncoding.Builder getEncoding() {
    OrcProto.ColumnEncoding.Builder result = super.getEncoding();
    if (isDirectV2) {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
    } else {
      result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    }
    return result;
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    TimestampColumnVector vec = (TimestampColumnVector) vector;
    vec.changeCalendar(useProleptic, true);
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        // ignore the bottom three digits from the vec.time field
        final long secs = vec.time[0] / MILLIS_PER_SECOND;
        final int newNanos = vec.nanos[0];
        // set the millis based on the top three digits of the nanos
        long millis = secs * MILLIS_PER_SECOND + newNanos / 1_000_000;
        if (millis < 0 && newNanos > 999_999) {
          millis -= MILLIS_PER_SECOND;
        }
        long utc = vec.isUTC() ?
            millis : SerializationUtils.convertToUtc(localTimezone, millis);
        indexStatistics.updateTimestamp(utc);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            bloomFilter.addLong(millis);
          }
          bloomFilterUtf8.addLong(utc);
        }
        final long nano = formatNanos(vec.nanos[0]);
        for (int i = 0; i < length; ++i) {
          seconds.write(secs - (useUTCTimestamp ? baseEpochSecsUTC : baseEpochSecsLocalTz));
          nanos.write(nano);
        }
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          // ignore the bottom three digits from the vec.time field
          final long secs = vec.time[i + offset] / MILLIS_PER_SECOND;
          final int newNanos = vec.nanos[i + offset];
          // set the millis based on the top three digits of the nanos
          long millis = secs * MILLIS_PER_SECOND + newNanos / 1_000_000;
          if (millis < 0 && newNanos > 999_999) {
            millis -= MILLIS_PER_SECOND;
          }
          long utc = vec.isUTC() ?
              millis : SerializationUtils.convertToUtc(localTimezone, millis);
          if (useUTCTimestamp) {
            seconds.write(secs - baseEpochSecsUTC);
          } else {
            seconds.write(secs - baseEpochSecsLocalTz);
          }
          nanos.write(formatNanos(newNanos));
          indexStatistics.updateTimestamp(utc);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addLong(millis);
            }
            bloomFilterUtf8.addLong(utc);
          }
        }
      }
    }
  }

  @Override
  public void writeStripe(OrcProto.StripeFooter.Builder builder,
                          OrcProto.StripeStatistics.Builder stats,
                          int requiredIndexEntries) throws IOException {
    super.writeStripe(builder, stats, requiredIndexEntries);
    if (rowIndexPosition != null) {
      recordPosition(rowIndexPosition);
    }
  }

  private static long formatNanos(int nanos) {
    if (nanos == 0) {
      return 0;
    } else if (nanos % 100 != 0) {
      return ((long) nanos) << 3;
    } else {
      nanos /= 100;
      int trailingZeros = 1;
      while (nanos % 10 == 0 && trailingZeros < 7) {
        nanos /= 10;
        trailingZeros += 1;
      }
      return ((long) nanos) << 3 | trailingZeros;
    }
  }

  @Override
  void recordPosition(PositionRecorder recorder) throws IOException {
    super.recordPosition(recorder);
    seconds.getPosition(recorder);
    nanos.getPosition(recorder);
  }

  @Override
  public long estimateMemory() {
    return super.estimateMemory() + seconds.estimateMemory() +
        nanos.estimateMemory();
  }

  @Override
  public long getRawDataSize() {
    return fileStatistics.getNumberOfValues() *
        JavaDataModel.get().lengthOfTimestamp();
  }

  @Override
  public void flushStreams() throws IOException {
    super.flushStreams();
    seconds.flush();
    nanos.flush();
  }
}
