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
package org.apache.orc.bench.hive.rowfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.Utilities;
import org.apache.orc.OrcFilterContext;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class DecimalRowFilterBenchmark extends org.openjdk.jmh.Main {

  private static final Path root = new Path(System.getProperty("user.dir"));

  @State(Scope.Thread)
  public static class InputState {

    // try both DecimalColumnVector and Decimal64
    @Param({"ORIGINAL", "USE_DECIMAL64"})
    public TypeDescription.RowBatchVersion version;

    @Param({"DECIMAL"})
    public TypeDescription.Category benchType;

    @Param({"0.01", "0.1", "0.2", "0.4", "0.6", "0.8", "1."})
    public String filterPerc;

    @Param({"2"})
    public int filterColsNum;

    Configuration conf = new Configuration();
    FileSystem fs;
    TypeDescription schema;
    VectorizedRowBatch batch;
    Path path;
    boolean[] include;
    Reader reader;
    Reader.Options readerOptions;
    String filter_column = "vendor_id";

    @Setup
    public void setup() throws IOException {
      fs = FileSystem.getLocal(conf).getRaw();
      path = new Path(root, "data/generated/taxi/orc.none");
      schema = Utilities.loadSchema("taxi.schema");
      batch = schema.createRowBatch(version, 1024);
      include = new boolean[schema.getMaximumId() + 1];
      for(TypeDescription child: schema.getChildren()) {
        if (schema.getFieldNames().get(child.getId()-1).compareTo(filter_column) == 0) {
          System.out.println("Apply Filter on column: " + schema.getFieldNames().get(child.getId()-1));
          include[child.getId()] = true;
        } else if (child.getCategory() == benchType) {
          System.out.println("Skip column(s): " + schema.getFieldNames().get(child.getId()-1));
          include[child.getId()] = true;
          if (--filterColsNum == 0) break;
        }
      }
      if (filterColsNum != 0) {
        System.err.println("Dataset does not contain type: "+ benchType);
        System.exit(-1);
      }
      generateRandomSet(Double.parseDouble(filterPerc));
      reader = OrcFile.createReader(path,
          OrcFile.readerOptions(conf).filesystem(fs));
      // just read the Decimal columns
      readerOptions = reader.options().include(include);
    }

    static boolean[] filterValues = null;
    public static boolean[] generateRandomSet(double percentage) throws IllegalArgumentException {
      if (percentage > 1.0) {
        throw new IllegalArgumentException("Filter percentage must be < 1.0 but was "+ percentage);
      }
      filterValues = new boolean[1024];
      int count = 0;
      while (count < (1024 * percentage)) {
        Random randomGenerator = new Random();
        int randVal = randomGenerator.nextInt(1024);
        if (filterValues[randVal] == false) {
          filterValues[randVal] = true;
          count++;
        }
      }
      return filterValues;
    }

    public static void customIntRowFilter(OrcFilterContext batch) {
      int newSize = 0;
      for (int row = 0; row < batch.getSelectedSize(); ++row) {
        if (filterValues[row]) {
          batch.getSelected()[newSize++] = row;
        }
      }
      batch.setSelectedInUse(true);
      batch.setSelectedSize(newSize);
    }
  }

  @Benchmark
  public void readOrcRowFilter(Blackhole blackhole, InputState state) throws Exception {
    RecordReader rows =
        state.reader.rows(state.readerOptions
            .setRowFilter(new String[]{state.filter_column}, InputState::customIntRowFilter));
    while (rows.nextBatch(state.batch)) {
      blackhole.consume(state.batch);
    }
    rows.close();
  }

  @Benchmark
  public void readOrcNoFilter(Blackhole blackhole, InputState state) throws Exception {
    RecordReader rows = state.reader.rows(state.readerOptions);
    while (rows.nextBatch(state.batch)) {
      blackhole.consume(state.batch);
    }
    rows.close();
  }

  /*
   * Run this test:
   *  java -cp hive/target/orc-benchmarks-hive-*-uber.jar org.apache.orc.bench.hive.rowfilter.DecimalRowFilterBenchmark
   */
  public static void main(String[] args) throws RunnerException {
    new Runner(new OptionsBuilder()
        .include(DecimalRowFilterBenchmark.class.getSimpleName())
        .forks(1)
        .build()).run();
  }
}