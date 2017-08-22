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

package org.apache.orc.bench;

import org.apache.orc.PhysicalWriter;
import org.apache.orc.impl.DoubleWriter;
import org.apache.orc.impl.DoubleWriterV1;
import org.apache.orc.impl.DoubleWriterV2;
import org.apache.orc.impl.DynamicByteArray;
import org.apache.orc.impl.OutStream;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations=1, time=1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations=1, time=1, timeUnit = TimeUnit.SECONDS)
@org.openjdk.jmh.annotations.State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
public class DoubleEncodingBenchmark {
  private static final int SIZE = 1024 * 1024;

  private static final List<Double> iotMeters = new ArrayList<>();
  private static final List<Double> listPrices = new ArrayList<>();
  private static final List<Double> discountAmts = new ArrayList<>();

  @Param({"LIST_PRICE", "DISCOUNT_AMT", "IOT_METER"})
  public String dataSet;

  @AuxCounters
  @org.openjdk.jmh.annotations.State(Scope.Thread)
  public static class State {
    long bytesWrote;
    float compressionRatio;
    final double[] doubles = new double[SIZE];

    public State() {
      try {
        read("iot_meter.csv", iotMeters);
        read("tpcds_list_price.csv", listPrices);
        read("tpcds_discount_amt.csv", discountAmts);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Setup(Level.Iteration)
    public void clean() {
      bytesWrote = 0;
    }

    @TearDown(Level.Iteration)
    public void print() {
      compressionRatio = 100 * (1 - bytesWrote / (SIZE * 8.0f));
      System.out.println();
      System.out.format("Compression ratio: %f%%%n", compressionRatio);
    }

    private void read(String fileName, List<Double> list) throws IOException {
      final BufferedReader reader = new BufferedReader(
          new InputStreamReader(DoubleEncodingBenchmark.class.getClassLoader().
              getResourceAsStream(fileName), StandardCharsets.UTF_8.name()));
      while (reader.ready()) {
        try {
          list.add(Double.parseDouble(reader.readLine()));
        } catch (NumberFormatException | NullPointerException e) {
          list.add(0.0d);
        }
      }
      reader.close();
    }
  }

  @Benchmark
  public void doubleWriterV1(State counters) throws IOException {
    final OutputCollector collect = new OutputCollector();
    final OutStream outStream = new OutStream("test",
        SIZE * (Long.SIZE / Byte.SIZE), null, collect);
    doubleWriter(counters, new DoubleWriterV1(outStream));
    counters.bytesWrote = collect.buffer.size();
  }

  @Benchmark
  public void doubleWriterV2(State counters) throws IOException {
    final OutputCollector collect = new OutputCollector();
    final OutStream outStream = new OutStream("test",
        SIZE * (Long.SIZE / Byte.SIZE), null, collect);
    doubleWriter(counters, new DoubleWriterV2(outStream));
    counters.bytesWrote = collect.buffer.size();
  }

  public void doubleWriter(State counters, DoubleWriter writer) throws
      IOException {
    switch (dataSet) {
      case "IOT_METER":
        for (int i = 0; i < SIZE; i++) {
          counters.doubles[i] = iotMeters.get(i % iotMeters.size());
        }
        break;
      case "LIST_PRICE":
        for (int i = 0; i < SIZE; i++) {
          counters.doubles[i] = listPrices.get(i % listPrices.size());
        }
        break;
      case "DISCOUNT_AMT":
        for (int i = 0; i < SIZE; i++) {
          counters.doubles[i] = discountAmts.get(i % discountAmts.size());
        }
        break;
      default:
    }

    for (int i = 0; i < SIZE; i++) {
      writer.write(counters.doubles[i]);
    }
    writer.flush();
  }

  private static class OutputCollector implements
      PhysicalWriter.OutputReceiver {
    private final DynamicByteArray buffer = new DynamicByteArray();

    @Override
    public void output(ByteBuffer buffer) throws IOException {
      this.buffer.add(buffer.array(), buffer.arrayOffset(), buffer.limit());
    }

    @Override
    public void suppress() {
      // PASS
    }
  }

  public static void main(String[] args) throws Exception {
    new Runner(new OptionsBuilder()
        .include(DoubleEncodingBenchmark.class.getSimpleName()).build()
    ).run();
  }
}
