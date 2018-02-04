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

package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.PhysicalWriter;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.floating.DoubleReadBenchmark;
import org.apache.orc.bench.floating.DoubleWriteBenchmark;
import org.apache.orc.impl.writer.TreeWriter;
import org.apache.orc.impl.writer.WriterContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestDoubleWriteRead {
  private static final int TEST_SIZE = 1024 * 1024;
  private static final int BUFFER_SIZE = 256 * 1024;
  private static final int VECTOR_SIZE = 1024;
  private static final Configuration config = new Configuration();

  private final DoubleWriteBenchmark.Algorithm algorithm;
  private final DoubleWriteBenchmark.DataSet dataSet;
  private final CompressionKind compression;

  public TestDoubleWriteRead(DoubleWriteBenchmark.Algorithm algorithm,
                             DoubleWriteBenchmark.DataSet dataSet,
                             CompressionKind compression) {
    this.algorithm = algorithm;
    this.dataSet = dataSet;
    this.compression = compression;
  }

  @Test
  public void runTest() throws IOException {
    double[] inputData = readData(dataSet.getFile(), TEST_SIZE);
    TypeDescription schema = TypeDescription.fromString("double");
    OutputCollector output = new OutputCollector(compression, config);
    TreeWriter writer = DoubleWriteBenchmark.createWriter(algorithm, schema, output);
    DoubleColumnVector vector = new DoubleColumnVector(VECTOR_SIZE);
    int offset = 0;
    while (offset < inputData.length) {
      int len = Math.min(inputData.length - offset, VECTOR_SIZE);
      System.arraycopy(inputData, offset, vector.vector, 0, len);
      writer.writeBatch(vector, 0, len);
      offset += len;
    }
    OrcProto.StripeFooter.Builder footer = OrcProto.StripeFooter.newBuilder();
    footer.addColumns(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT));
    OrcProto.StripeStatistics.Builder stats = OrcProto.StripeStatistics.newBuilder();
    writer.writeStripe(footer, stats, 0);

    ReaderContext context = new ReaderContext(schema);
    TreeReaderFactory.TreeReader reader =
        DoubleReadBenchmark.createReader(0, context, algorithm);
    reader.startStripe(output.createStreams(), footer.build());
    offset = 0;
    while (offset < inputData.length) {
      int len = Math.min(inputData.length - offset, VECTOR_SIZE);
      reader.nextVector(vector, null, len);
      for(int r=0; r < len; ++r) {
        assertEquals(dataSet + " value " + (r + offset), inputData[r+offset],
            vector.vector[r], 1e-15);
      }
      offset += len;
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> setParameters() {
    Collection<Object[]> params = new ArrayList<>();
    for(DoubleWriteBenchmark.Algorithm algorithm: DoubleWriteBenchmark.Algorithm.values()) {
      for(DoubleWriteBenchmark.DataSet data: DoubleWriteBenchmark.DataSet.values()) {
        for (CompressionKind compression: new CompressionKind[]{
            CompressionKind.NONE, CompressionKind.ZLIB, CompressionKind.LZO}) {
          params.add(new Object[]{algorithm, data, compression});
        }
      }
    }
    return params;
  }

  /**
   * Make an in-memory mini-orc reader/writer.
   */
  private static class OutputCollector implements WriterContext {
    private Map<StreamName,List<ByteBuffer>> output = new TreeMap<>();
    private final CompressionKind compression;
    private final Configuration configuration;

    OutputCollector(CompressionKind compression, Configuration configuration) {
      this.compression = compression;
      this.configuration = configuration;
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

    public Map<StreamName, InStream> createStreams() throws IOException {
      Map<StreamName, InStream> result = new TreeMap<>();
      for(Map.Entry<StreamName, List<ByteBuffer>> entry: output.entrySet()) {
        List<DiskRange> ranges = new ArrayList<>();
        int size = 0;
        for(ByteBuffer buffer: entry.getValue()) {
          size += buffer.remaining();
        }
        ByteBuffer merged = ByteBuffer.allocate(size);
        for(ByteBuffer buffer: entry.getValue()) {
          merged.put(buffer);
        }
        merged.flip();
        ranges.add(new BufferChunk(merged, 0));
        InStream data = InStream.create("data", ranges, size,
            WriterImpl.createCodec(compression), BUFFER_SIZE);
        result.put(entry.getKey(), data);
      }
      return result;
    }
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

  public static double[] readData(String name, int size) throws IOException {
    double[] result = new double[size];
    List<Double> input = read(name);
    int next = 0;
    for(int i=0; i < result.length; ++i) {
      result[i] = input.get(next);
      next = (next + 1) % input.size();
    }
    return result;
  }

  private static List<Double> read(String fileName) throws IOException {
    InputStream input = new GZIPInputStream(
        TestDoubleWriteRead.class.getClassLoader().getResourceAsStream(fileName));
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
}
