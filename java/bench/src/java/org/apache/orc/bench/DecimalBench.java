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

package org.apache.orc.bench;

import com.google.gson.JsonStreamParser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrackingLocalFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.parquet.read.DataWritableReadSupport;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.bench.convert.BatchReader;
import org.apache.orc.bench.convert.GenerateVariants;
import org.apache.orc.bench.convert.csv.CsvReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations=2, time=30, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations=10, time=30, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
public class DecimalBench {

  private static final String ROOT_ENVIRONMENT_NAME = "bench.root.dir";
  private static final Path root;
  static {
    String value = System.getProperty(ROOT_ENVIRONMENT_NAME);
    root = value == null ? null : new Path(value);
  }

  /**
   * Abstract out whether we are writing short or long decimals
   */
  interface Loader {
    /**
     * Load the data from the values array into the ColumnVector.
     * @param vector the output
     * @param values the intput
     * @param offset the first input value
     * @param length the number of values to copy
     */
    void loadData(ColumnVector vector, long[] values, int offset, int length);
  }

  static class Decimal64Loader implements Loader {
    final int scale;
    final int precision;

    Decimal64Loader(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void loadData(ColumnVector vector, long[] values, int offset, int length) {
      Decimal64ColumnVector v = (Decimal64ColumnVector) vector;
      v.ensureSize(length, false);
      v.noNulls = true;
      for(int p=0; p < length; ++p) {
        v.vector[p] = values[p + offset];
      }
      v.precision = (short) precision;
      v.scale = (short) scale;
    }
  }

  static class DecimalLoader implements Loader {
    final int scale;
    final int precision;

    DecimalLoader(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void loadData(ColumnVector vector, long[] values, int offset, int length) {
      DecimalColumnVector v = (DecimalColumnVector) vector;
      v.noNulls = true;
      for(int p=0; p < length; ++p) {
        v.vector[p].setFromLongAndScale(values[offset + p], scale);
      }
      v.precision = (short) precision;
      v.scale = (short) scale;
    }
  }

  @State(Scope.Thread)
  public static class OutputState {

    // try both short and long decimals
    @Param({"8", "19"})
    public int precision;

    long[] total_amount = new long[1024 * 1024];
    Configuration conf = new Configuration();
    FileSystem fs = new NullFileSystem();
    TypeDescription schema;
    VectorizedRowBatch batch;
    Loader loader;

    @Setup
    public void setup() throws IOException {
      schema = TypeDescription.createDecimal()
          .withScale(2)
          .withPrecision(precision);
      loader = precision <= 18 ?
            new Decimal64Loader(precision, 2) :
            new DecimalLoader(precision, 2);
      readCsvData(total_amount, root, "total_amount", conf);
      batch = schema.createRowBatchV2();
    }
  }

  @Benchmark
  public void write(OutputState state) throws Exception {
    Writer writer = OrcFile.createWriter(new Path("null"),
        OrcFile.writerOptions(state.conf)
            .fileSystem(state.fs)
            .setSchema(state.schema)
            .compress(CompressionKind.NONE));
    int r = 0;
    int batchSize = state.batch.getMaxSize();
    while (r < state.total_amount.length) {
      state.batch.size = batchSize;
      state.loader.loadData(state.batch.cols[0], state.total_amount, r, batchSize);
      writer.addRowBatch(state.batch);
      r += batchSize;
    }
    writer.close();
  }

  static void readCsvData(long[] data,
                          Path root,
                          String column,
                          Configuration conf) throws IOException {
    TypeDescription schema = Utilities.loadSchema("taxi.schema");
    int row = 0;
    int batchPosn = 0;
    BatchReader reader =
        new GenerateVariants.RecursiveReader(new Path(root, "sources/taxi"), "csv",
        schema, conf, org.apache.orc.bench.CompressionKind.ZLIB);
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 0;
    TypeDescription columnSchema = schema.findSubtype(column);
    DecimalColumnVector cv = (DecimalColumnVector) batch.cols[columnSchema.getId() - 1];
    int scale = columnSchema.getScale();
    while (row < data.length) {
      if (batchPosn >= batch.size) {
        if (!reader.nextBatch(batch)) {
          throw new IllegalArgumentException("Not enough data");
        }
        batchPosn = 0;
      }
      data[row++] = cv.vector[batchPosn++].serialize64(scale);
    }
  }

  @State(Scope.Thread)
  public static class InputState {

    // try both DecimalColumnVector and Decimal64ColumnVector
    @Param({"ORIGINAL", "USE_DECIMAL64"})
    public TypeDescription.RowBatchVersion version;

    Configuration conf = new Configuration();
    FileSystem fs;
    TypeDescription schema;
    VectorizedRowBatch batch;
    Path path;
    boolean[] include;
    Reader reader;
    OrcFile.ReaderOptions options;

    @Setup
    public void setup() throws IOException {
      fs = FileSystem.getLocal(conf).getRaw();
      path = new Path(root, "generated/taxi/orc.none");
      schema = Utilities.loadSchema("taxi.schema");
      batch = schema.createRowBatch(version, 1024);
      // only include the columns with decimal values
      include = new boolean[schema.getMaximumId() + 1];
      for(TypeDescription child: schema.getChildren()) {
        if (child.getCategory() == TypeDescription.Category.DECIMAL) {
          include[child.getId()] = true;
        }
      }
      reader = OrcFile.createReader(path,
          OrcFile.readerOptions(conf).filesystem(fs));
      // just read the decimal columns from the first stripe
      reader.options().include(include).range(0, 1000);
    }
  }

  @Benchmark
  public void read(Blackhole blackhole, InputState state) throws Exception {
    RecordReader rows = state.reader.rows();
    while (rows.nextBatch(state.batch)) {
      blackhole.consume(state.batch);
    }
    rows.close();
  }

  public static void main(String[] args) throws Exception {
    new Runner(new OptionsBuilder()
        .include(DecimalBench.class.getSimpleName())
        .jvmArgs("-server", "-Xms256m", "-Xmx2g",
            "-D" + ROOT_ENVIRONMENT_NAME + "=" + args[0]).build()
    ).run();
  }
}
