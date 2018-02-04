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
import org.apache.orc.impl.writer.TreeWriter;
import org.apache.orc.impl.writer.WriterContext;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class generates mini-ORC files for the read benchmark.
 */
public class DoubleReadSetup {

  // the compression buffer size
  public static final int BUFFER_SIZE = 256 * 1024;

  // the number of doubles to write in each test
  public static final int NUM_DOUBLES = 1024 * 1024;

  private static final int BATCH_SIZE = 1024;

  public static final CompressionKind[] compressionKinds = {CompressionKind.NONE,
      CompressionKind.LZO, CompressionKind.ZLIB};

  public static String makeFilename(String outputDirectory,
                                    DoubleWriteBenchmark.Algorithm algorithm,
                                    DoubleWriteBenchmark.DataSet data,
                                    CompressionKind compression) {
    String baseName = data.name().toLowerCase();
    int posn = baseName.indexOf('.');
    if (posn > 0) {
      baseName = baseName.substring(0, posn);
    }
    return outputDirectory + "/" + baseName + "_" + algorithm.getFormat() + "_" +
        compression.name().toLowerCase() + ".morc";
  }

  public static void writeMiniOrc(double[] input,
                                  DoubleWriteBenchmark.Algorithm algorithm,
                                  DoubleWriteBenchmark.DataSet data,
                                  CompressionKind compression,
                                  String outputDirectory,
                                  Configuration conf) throws IOException {
    OutputCollector output = new OutputCollector(compression, conf);
    TreeWriter writer = DoubleWriteBenchmark.createWriter(algorithm,
        TypeDescription.fromString("double"), output);
    OrcProto.StripeFooter.Builder footer = OrcProto.StripeFooter.newBuilder();
    OrcProto.StripeStatistics.Builder stats = OrcProto.StripeStatistics.newBuilder();
    DoubleColumnVector vector = new DoubleColumnVector(BATCH_SIZE);
    int posn = 0;
    while (posn < input.length){
      int length = Math.min(BATCH_SIZE, input.length - posn);
      System.arraycopy(input, posn, vector.vector, 0, length);
      writer.writeBatch(vector, 0, length);
      posn += length;
    }
    writer.writeStripe(footer, stats, 0);
    String outputFilename = makeFilename(outputDirectory, algorithm, data,
        compression);
    System.out.println("Writing " + outputFilename);
    output.writeMiniOrc(outputFilename);
  }

  /**
   * Make a testing class that will let us benchmark TreeWriters.
   */
  private static class OutputCollector implements WriterContext {
    private Map<StreamName,List<ByteBuffer>> output = new TreeMap<>();
    private final CompressionKind compression;
    private final Configuration configuration;

    OutputCollector(CompressionKind compression, Configuration configuration) {
      this.compression = compression;
      this.configuration = configuration;
    }

    public void writeMiniOrc(String filename) throws IOException {
      OrcProto.StripeFooter.Builder footer = OrcProto.StripeFooter.newBuilder();
      footer.addColumns(OrcProto.ColumnEncoding.newBuilder()
          .setKind(OrcProto.ColumnEncoding.Kind.DIRECT));
      List<ByteBuffer> data = new ArrayList<>();
      for (Map.Entry<StreamName,List<ByteBuffer>> entry: output.entrySet()) {
        StreamName name = entry.getKey();
        int sum = 0;
        for(ByteBuffer chunk: entry.getValue()) {
          sum += chunk.remaining();
        }
        footer.addStreams(OrcProto.Stream.newBuilder()
            .setKind(name.getKind())
            .setColumn(name.getColumn())
            .setLength(sum));
        data.addAll(entry.getValue());
      }
      OrcProto.StripeFooter msg = footer.build();
      DataOutputStream stream =
          new DataOutputStream(new FileOutputStream(filename));
      stream.writeInt(msg.getSerializedSize());
      msg.writeTo(stream);
      for(ByteBuffer chunk: data) {
        stream.write(chunk.array(), chunk.arrayOffset() + chunk.position(),
            chunk.remaining());
      }
      stream.close();
    }

    @Override
    public OutStream createStream(int column,
                                  OrcProto.Stream.Kind kind) throws IOException {
      final StreamName name = new StreamName(column, kind);
      output.put(name, new ArrayList<ByteBuffer>());
      return new OutStream("test", BUFFER_SIZE,
          WriterImpl.createCodec(compression),
          new PhysicalWriter.OutputReceiver() {
        @Override
        public void output(ByteBuffer buffer) throws IOException {
          output.get(name).add(buffer);
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
        .addOption("d", "data", true, "Define data directory root")
        .addOption("o", "output", true, "Define the output directory");
    CommandLine result = new DefaultParser().parse(options, args, true);
    if (result.hasOption("help")) {
      new HelpFormatter().printHelp("double-read-setup", options);
      System.exit(1);
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    CommandLine cli = parseCommandLine(args);
    String data = "bench/src/test/resources/";
    String output = DoubleReadBenchmark.DEFAULT_DATA_DIR;
    if (cli.hasOption("data")) {
      data = cli.getOptionValue("data");
    }
    if (cli.hasOption("output")) {
      output = cli.getOptionValue("output");
    }

    // create the output directory
    new File(output).mkdirs();
    Configuration conf = new Configuration();

    // iterate through the datasets, algorithms, and compression
    for(DoubleWriteBenchmark.DataSet set: DoubleWriteBenchmark.DataSet.values()) {
      double[] input = DoubleWriteBenchmark.readInput(data, set, NUM_DOUBLES);
      for(DoubleWriteBenchmark.Algorithm algorithm: DoubleWriteBenchmark.Algorithm.values()) {
        if (algorithm != DoubleWriteBenchmark.Algorithm.PLAIN) {
          for (CompressionKind compression : compressionKinds) {
            writeMiniOrc(input, algorithm, set, compression, output, conf);
          }
        }
      }
    }
  }
}
