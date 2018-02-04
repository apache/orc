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

package org.apache.orc.bench.floating;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.impl.writer.DoubleTreeWriter;
import org.apache.orc.impl.writer.DoubleTreeWriterFlip;
import org.apache.orc.impl.writer.DoubleTreeWriterFpcV1;
import org.apache.orc.impl.writer.DoubleTreeWriterFpcV2;
import org.apache.orc.impl.writer.DoubleTreeWriterPlainV2;
import org.apache.orc.impl.writer.DoubleTreeWriterSplit;
import org.apache.orc.impl.writer.TreeWriter;
import org.apache.orc.impl.writer.WriterContext;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations=1, time=1)
@Measurement(iterations=1, time=1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DoubleWriteBenchmark {
  private static final String DATA_DIRECTORY_ENV = "data.directory";

  // the compression buffer size
  private static final int BUFFER_SIZE = 256 * 1024;

  // the number of doubles to write in each test
  private static final int NUM_DOUBLES = 1024 * 1024;

  private static final int BATCH_SIZE = 1024;

  public enum DataSet {
    LIST_PRICE("tpcds_list_price.csv.gz"),
    DISCOUNT_AMT("tpcds_discount_amt.csv.gz"),
    IOT_METER("iot_meter.csv.gz"),
    NYC_TAXI_DROP_LAT("nyc-taxi-drop-lat.csv.gz"),
    NYC_TAXI_DROP_LONG("nyc-taxi-drop-long.csv.gz"),
    HIGGS("HIGGS-4.csv.gz"),
    HEPMASS("hepmass-8.csv.gz"),
    PHONE("phone-accelerometer-4.csv.gz");

    private final String file;

    DataSet(String file) {
      this.file = file;
    }

    public String getFile() {
      return file;
    }
  }

  private static List<Double> read(String dataDirectory,
                                   DataSet data) throws IOException {
    InputStream input = new GZIPInputStream(
        new FileInputStream(dataDirectory + "/" + data.file));
    BufferedReader reader = new BufferedReader(new InputStreamReader(input,
        StandardCharsets.UTF_8));
    List<Double> result = new ArrayList<>();
    while (reader.ready()) {
      String value = reader.readLine();
      try {
        result.add(Double.parseDouble(value));
      } catch (NumberFormatException | NullPointerException e) {
        throw new IOException("Bad number '" + value + "'", e);
      }
    }
    reader.close();
    return result;
  }

  public static double[] readInput(String dataDirectory,
                                   DataSet data,
                                   int size) throws IOException {
    double[] result = new double[size];
    List<Double> input = read(dataDirectory, data);
    int next = 0;
    for(int i=0; i < result.length; ++i) {
      result[i] = input.get(next);
      next = (next + 1) % input.size();
    }
    return result;
  }

  public enum Algorithm {
    PLAIN("plain"),
    PLAIN_V2("plain"),
    FPC_V1("fpcV1"),
    FPC_V2("fpcV2"),
    FLIP("flip"),
    SPLIT("split");

    Algorithm(String format) {
      this.format = format;
    }
    private final String format;

    public String getFormat() {
      return format;
    }
  }

  public static TreeWriter createWriter(Algorithm algorithm,
                                        TypeDescription schema,
                                        WriterContext context) throws IOException {
    switch (algorithm) {
      case PLAIN:
        return new DoubleTreeWriter(0, schema, context, false);
      case PLAIN_V2:
        return new DoubleTreeWriterPlainV2(0, schema, context, false);
      case FPC_V1:
        return new DoubleTreeWriterFpcV1(0, schema, context, false);
      case FPC_V2:
        return new DoubleTreeWriterFpcV2(0, schema, context, false);
      case FLIP:
        return new DoubleTreeWriterFlip(0, schema, context, false);
      case SPLIT:
        return new DoubleTreeWriterSplit(0, schema, context, false);
      default:
        throw new IllegalArgumentException("Unknown algorithm " + algorithm);
    }
  }

  @State(Scope.Benchmark)
  public static class InputData {

    private static final String DATA_DIRECTORY =
        System.getProperty(DATA_DIRECTORY_ENV);

    @Param({"LIST_PRICE",
        "DISCOUNT_AMT",
        "IOT_METER",
        "NYC_TAXI_DROP_LAT",
        "NYC_TAXI_DROP_LONG",
        "HIGGS",
        "HEPMASS",
        "PHONE"})
    DataSet dataSet;

    double[] doubles;

    public InputData() {
      // PASS
    }

    @Setup
    public void setup() throws IOException {
      doubles = readInput(DATA_DIRECTORY, dataSet, NUM_DOUBLES);
    }
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class OutputData {

    @Param({"NONE", "ZLIB", "LZO"})
    CompressionKind compression;

    @Param({"PLAIN", "PLAIN_V2" , "FPC_V1", "FPC_V2", "FLIP", "SPLIT"})
    Algorithm algorithm;

    long bytesWrote;
    long recordCount;
    OutputCollector collect;
    TreeWriter writer;
    DoubleColumnVector vector;

    public OutputData() {
    }

    public long bytesPer1000() {
      return (bytesWrote * 1000) / recordCount;
    }

    @Setup
    public void setup() throws IOException {
      collect = new OutputCollector(compression, new Configuration());
      bytesWrote = 0;
      recordCount = 0;
      TypeDescription schema = TypeDescription.fromString("double");
      vector = new DoubleColumnVector(BATCH_SIZE);
      writer = createWriter(algorithm, schema, collect);
    }
  }

  @Benchmark
  public void runTest(InputData input,
                      OutputData output) throws IOException {
    output.collect.clear();
    OrcProto.StripeFooter.Builder footer = OrcProto.StripeFooter.newBuilder();
    OrcProto.StripeStatistics.Builder stats = OrcProto.StripeStatistics.newBuilder();
    int posn = 0;
    while (posn < input.doubles.length){
      int length = Math.min(BATCH_SIZE, input.doubles.length - posn);
      System.arraycopy(input.doubles, posn, output.vector.vector, 0, length);
      output.writer.writeBatch(output.vector, 0, length);
      posn += length;
    }
    output.writer.writeStripe(footer, stats, 0);
    output.bytesWrote += output.collect.getSize();
    output.recordCount += input.doubles.length;
  }

  /**
   * Make a testing class that will let us benchmark TreeWriters.
   */
  private static class OutputCollector implements WriterContext {
    private long size = 0;

    private final CompressionKind compression;
    private final Configuration configuration;

    OutputCollector(CompressionKind compression, Configuration configuration) {
      this.compression = compression;
      this.configuration = configuration;
    }

    public long getSize() {
      return size;
    }

    public void clear() {
      size = 0;
    }

    @Override
    public OutStream createStream(int column,
                                  OrcProto.Stream.Kind kind) throws IOException {
      return new OutStream("test", BUFFER_SIZE,
          WriterImpl.createCodec(compression),
          new PhysicalWriter.OutputReceiver() {
        @Override
        public void output(ByteBuffer buffer) throws IOException {
          size += buffer.remaining();
        }

        @Override
        public void suppress() {
          // PASS
        }
      });
    }

    @Override
    public int getRowIndexStride() {
      return 0;
    }

    @Override
    public boolean buildIndex() {
      return false;
    }

    @Override
    public boolean isCompressed() {
      return compression != CompressionKind.NONE;
    }

    @Override
    public OrcFile.EncodingStrategy getEncodingStrategy() {
      return OrcFile.EncodingStrategy.SPEED;
    }

    @Override
    public boolean[] getBloomFilterColumns() {
      return new boolean[1];
    }

    @Override
    public double getBloomFilterFPP() {
      return 0;
    }

    @Override
    public Configuration getConfiguration() {
      return configuration;
    }

    @Override
    public OrcFile.Version getVersion() {
      return OrcFile.Version.UNSTABLE_PRE_2_0;
    }

    @Override
    public OrcFile.BloomFilterVersion getBloomFilterVersion() {
      return OrcFile.BloomFilterVersion.UTF8;
    }

    @Override
    public void writeIndex(StreamName name, OrcProto.RowIndex.Builder index) {
      // pass
    }

    @Override
    public void writeBloomFilter(StreamName name,
                                 OrcProto.BloomFilterIndex.Builder bloom) {
      // pass
    }
  }

  static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = new Options()
        .addOption("h", "help", false, "Provide help")
        .addOption("D", "define", true, "Change configuration settings")
        .addOption("d", "data", true, "Define data directory root");
    CommandLine result = new DefaultParser().parse(options, args, true);
    if (result.hasOption("help")) {
      new HelpFormatter().printHelp("double-write", options);
      System.exit(1);
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    CommandLine cli = parseCommandLine(args);
    String data = "bench/src/test/resources/";
    if (cli.hasOption("data")) {
      data = cli.getOptionValue("data");
    }
    new Runner(new OptionsBuilder()
        .include(DoubleWriteBenchmark.class.getSimpleName())
        .jvmArgs("-server", "-Xms256m", "-Xmx2g",
        "-D" + DATA_DIRECTORY_ENV + "=" + data).build()
    ).run();
  }
}
