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

package org.apache.orc.bench.hive;

import com.google.protobuf.ByteString;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CustomStatistics;
import org.apache.orc.CustomStatisticsBuilder;
import org.apache.orc.CustomStatisticsRegister;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class OptimizeFilterBenchmark extends org.openjdk.jmh.Main {

  Path workDir = new Path(System.getProperty("java.io.tmpdir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  TypeDescription schema;
  Reader reader;
  String[] columns = new String[] {"x","y", "z"};

  @Param({"2", "3"})
  public int proportion;

  @Param({"10", "100", "1000"})
  public int quota;

  @Setup
  public void initOrcFile() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("testWriterImpl.orc");
    fs.create(testFilePath, true);
    TDigestBuilder tDigestBuilder = new TDigestBuilder();
    CustomStatisticsRegister.getInstance().register(tDigestBuilder);
    conf.set(OrcConf.CUSTOM_STATISTICS_COLUMNS.getAttribute(),
        "x:" + tDigestBuilder.name() + ",y:" + tDigestBuilder.name()
            + ",z:" + tDigestBuilder.name());

    schema = TypeDescription.fromString("struct<x:int,y:int,z:int>");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).overwrite(true));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector x = (LongColumnVector) batch.cols[0];
    LongColumnVector y = (LongColumnVector) batch.cols[1];
    LongColumnVector z = (LongColumnVector) batch.cols[2];
    Random random = new Random();
    for(int r=0; r < 10000; ++r) {
      int row = batch.size++;
      x.vector[row] = random.nextInt(quota);
      y.vector[row] = random.nextInt(quota * proportion);
      z.vector[row] = random.nextInt(quota * proportion * proportion);
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
    reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
  }

  @Benchmark
  public void useTDigest() throws IOException {
    ColumnStatistics[] statistics = reader.getStatistics();
    TDigestCustomStatistics[] customStatistics;
    ColumnStatistics xStatistic = statistics[1];
    ColumnStatistics yStatistic = statistics[2];
    ColumnStatistics zStatistic = statistics[3];

    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder().startAnd();

    customStatistics = new TDigestCustomStatistics[] {
        ((TDigestCustomStatistics) xStatistic.getCustomStatistics()).bindId(1),
        ((TDigestCustomStatistics) yStatistic.getCustomStatistics()).bindId(2),
        ((TDigestCustomStatistics) zStatistic.getCustomStatistics()).bindId(3)
    };
    Arrays.sort(customStatistics, Comparator.comparingDouble(o -> o.tDigest.cdf(0)));
    for (TDigestCustomStatistics statistic : customStatistics) {
      builder = builder.equals(columns[statistic.id - 1], PredicateLeaf.Type.LONG, 0L);
    }

    SearchArgument searchArgument = builder.end().build();
    Reader.Options options = reader.options().schema(schema)
        .searchArgument(searchArgument, columns);
    VectorizedRowBatch batch = schema.createRowBatch();
    RecordReader rowIterator = reader.rows(options);
    while (rowIterator.nextBatch(batch)) {}
    rowIterator.close();
  }

  @Benchmark
  public void noUseTDigest() throws IOException {
    SearchArgument.Builder builder = SearchArgumentFactory.newBuilder().startAnd();

    for (String column : columns) {
      builder = builder.equals(column, PredicateLeaf.Type.LONG, 0L);
    }

    SearchArgument searchArgument = builder.end().build();
    Reader.Options options = reader.options().schema(schema)
        .searchArgument(searchArgument, columns);
    VectorizedRowBatch batch = schema.createRowBatch();
    RecordReader rowIterator = reader.rows(options);
    while (rowIterator.nextBatch(batch)) {}
    rowIterator.close();
  }


  public static class TDigestBuilder implements CustomStatisticsBuilder {

    @Override
    public String name() {
      return "TDigestForInteger";
    }

    @Override
    public CustomStatistics[] build() {
      return new CustomStatistics[] {
          new TDigestCustomStatistics(false),
          new TDigestCustomStatistics(false),
          new TDigestCustomStatistics(true),
      };
    }

    @Override
    public CustomStatistics build(ByteString statisticsContent) {
      return new TDigestCustomStatistics(
          AVLTreeDigest.fromBytes(statisticsContent.asReadOnlyByteBuffer()));
    }
  }

  public static class TDigestCustomStatistics implements CustomStatistics {

    private TDigest tDigest;

    private boolean needPersistence;

    private int id;

    public TDigestCustomStatistics(TDigest tDigest) {
      this.tDigest = tDigest;
    }

    public TDigestCustomStatistics(boolean needPersistence) {
      this.tDigest = TDigest.createAvlTreeDigest(100);
      this.needPersistence = needPersistence;
    }

    @Override
    public String name() {
      return "TDigestForInteger";
    }

    @Override
    public void updateInteger(long value, int repetitions) {
      tDigest.add(value, repetitions);
    }

    @Override
    public ByteString content() {
      ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.byteSize());
      tDigest.asBytes(byteBuffer);
      byteBuffer.flip();
      return ByteString.copyFrom(byteBuffer);
    }

    @Override
    public boolean needPersistence() {
      return needPersistence;
    }

    @Override
    public void merge(CustomStatistics customStatistics) {
      if (customStatistics instanceof TDigestCustomStatistics) {
        tDigest.add(((TDigestCustomStatistics) customStatistics).tDigest);
      }
    }

    @Override
    public void reset() {
      tDigest = TDigest.createAvlTreeDigest(100);
    }

    public TDigest getTDigest() {
      return tDigest;
    }

    public TDigestCustomStatistics bindId(int id) {
      this.id = id;
      return this;
    }
  }

  /*
   * Run this test:
   *  java -cp hive/target/orc-benchmarks-hive-*-uber.jar org.apache.orc.bench.hive.OptimizeFilterBenchmark
   */
  public static void main(String[] args) throws RunnerException {
    new Runner(new OptionsBuilder()
        .include(OptimizeFilterBenchmark.class.getSimpleName())
        .forks(1)
        .build()).run();
  }

}
