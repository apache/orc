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
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.DoubleTreeReaderFlip;
import org.apache.orc.impl.DoubleTreeReaderFpcV1;
import org.apache.orc.impl.DoubleTreeReaderFpcV2;
import org.apache.orc.impl.DoubleTreeReaderSplit;
import org.apache.orc.impl.DoubleTreeReaderV2;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.SchemaEvolution;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.TreeReaderFactory;
import org.apache.orc.impl.WriterImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations=1, time=1)
@Measurement(iterations=1, time=1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DoubleReadBenchmark {
  private static final String DATA_DIRECTORY_ENV = "data.directory";
  static final String DEFAULT_DATA_DIR = "bench/target/double-mini-orc/";
  private static String dataDirectory = DEFAULT_DATA_DIR;
  static {
    if (System.getProperty(DATA_DIRECTORY_ENV) != null) {
      dataDirectory = System.getProperty(DATA_DIRECTORY_ENV);
    }
  }

  private static final int BATCH_SIZE = 1024;

  public static TreeReaderFactory.TreeReader createReader(int column,
                                                          TreeReaderFactory.Context context,
                                                          DoubleWriteBenchmark.Algorithm algorithm
                                                          ) throws IOException {
    switch (algorithm) {
      case PLAIN:
        return new TreeReaderFactory.DoubleTreeReader(column);
      case PLAIN_V2:
        return new DoubleTreeReaderV2(column, null, context);
      case FPC_V1:
        return new DoubleTreeReaderFpcV1(column, null, context);
      case FPC_V2:
        return new DoubleTreeReaderFpcV2(column, null, context);
      case FLIP:
        return new DoubleTreeReaderFlip(column, null, context);
      case SPLIT:
        return new DoubleTreeReaderSplit(column, null, context);
      default:
        throw new IllegalArgumentException("Unknown algorithm " + algorithm);
    }
  }

  @State(Scope.Benchmark)
  public static class InputData {

    @Param({"LIST_PRICE",
        "DISCOUNT_AMT",
        "IOT_METER",
        "NYC_TAXI_DROP_LAT",
        "NYC_TAXI_DROP_LONG",
        "HIGGS",
        "HEPMASS",
        "PHONE"})
    DoubleWriteBenchmark.DataSet dataSet;

    @Param({"NONE", "ZLIB", "LZO"})
    CompressionKind compression;

    @Param({"PLAIN", "PLAIN_V2", "FPC_V1", "FPC_V2", "FLIP", "SPLIT"})
    DoubleWriteBenchmark.Algorithm algorithm;

    OrcProto.StripeFooter footer;
    Map<StreamName, byte[]> streams = new HashMap<>();
    CompressionCodec codec;

    public InputData() {
      // PASS
    }

    @Setup
    public void setup() throws IOException {
      String path = DoubleReadSetup.makeFilename(dataDirectory, algorithm,
          dataSet, compression);
      DataInputStream inStream = new DataInputStream(new FileInputStream(path));
      int footerSize = inStream.readInt();
      byte[] footerBuffer = new byte[footerSize];
      inStream.readFully(footerBuffer);
      footer = OrcProto.StripeFooter.parseFrom(footerBuffer);
      streams.clear();
      for(OrcProto.Stream stream: footer.getStreamsList()) {
        StreamName name = new StreamName(stream.getColumn(), stream.getKind());
        CompressionCodec codec = WriterImpl.createCodec(compression);
        long len = stream.getLength();
        byte[] buffer = new byte[(int) len];
        inStream.readFully(buffer);
        streams.put(name, buffer);
      }
      if (inStream.available() > 0) {
        throw new IllegalStateException("Left over bytes in " + path);
      }
      inStream.close();
      codec = WriterImpl.createCodec(compression);
    }
  }

  @State(Scope.Thread)
  public static class OutputData {


    DoubleColumnVector vector;
    ReaderContext context;

    public OutputData() {
    }

    @Setup
    public void setup() throws IOException {
      vector = new DoubleColumnVector(BATCH_SIZE);
    }
  }

  @Benchmark
  public void runTest(InputData input,
                      OutputData output,
                      Blackhole hole) throws IOException {
    TreeReaderFactory.TreeReader reader =
        createReader(0, output.context, input.algorithm);
    Map<StreamName, InStream> streams = new HashMap<>(input.streams.size());
    for(StreamName name: input.streams.keySet()) {
      List<DiskRange> ranges = new ArrayList<>();
      byte[] buffer = input.streams.get(name);
      ranges.add(new BufferChunk(ByteBuffer.wrap(buffer), 0));
      InStream data = InStream.create("data", ranges, buffer.length,
          input.codec, DoubleReadSetup.BUFFER_SIZE);
      streams.put(name, data);
    }
    reader.startStripe(streams, input.footer);
    int count = 0;
    while (count < DoubleReadSetup.NUM_DOUBLES) {
      output.vector.reset();
      int length = Math.min(BATCH_SIZE, DoubleReadSetup.NUM_DOUBLES - count);
      reader.nextVector(output.vector, null, BATCH_SIZE);
      count += length;
      hole.consume(output.vector);
    }
  }

  static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = new Options()
        .addOption("h", "help", false, "Provide help")
        .addOption("D", "define", true, "Change configuration settings")
        .addOption("d", "data", true, "Define data directory root");
    CommandLine result = new DefaultParser().parse(options, args, true);
    if (result.hasOption("help")) {
      new HelpFormatter().printHelp("double-read", options);
      System.exit(1);
    }
    return result;
  }

  public static class ReaderContext implements TreeReaderFactory.Context {
    private final SchemaEvolution evolution;

    public ReaderContext(TypeDescription schema) {
      Reader.Options options = new Reader.Options();
      evolution = new SchemaEvolution(schema, null, options);
    }

    @Override
    public SchemaEvolution getSchemaEvolution() {
      return evolution;
    }

    @Override
    public boolean isSkipCorrupt() {
      return false;
    }

    @Override
    public String getWriterTimezone() {
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLine cli = parseCommandLine(args);
    if (cli.hasOption("data")) {
      dataDirectory = cli.getOptionValue("data");
    }
    new Runner(new OptionsBuilder()
        .include(DoubleReadBenchmark.class.getSimpleName())
        .jvmArgs("-server", "-Xms256m", "-Xmx2g",
        "-D" + DATA_DIRECTORY_ENV + "=" + dataDirectory).build()
    ).run();
  }
}
