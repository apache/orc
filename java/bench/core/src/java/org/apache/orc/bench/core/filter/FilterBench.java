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

package org.apache.orc.bench.core.filter;

import com.google.auto.service.AutoService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcFile;
import org.apache.orc.bench.core.OrcBenchmark;
import org.apache.orc.filter.OrcFilterContext;
import org.apache.orc.filter.FilterFactory;
import org.apache.orc.filter.impl.BatchFilter;
import org.apache.orc.filter.impl.Filters;
import org.apache.orc.filter.impl.LongEquals;
import org.apache.orc.filter.impl.LongIn;
import org.junit.Assert;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.apache.orc.bench.core.BenchmarkOptions.FORK;
import static org.apache.orc.bench.core.BenchmarkOptions.GC;
import static org.apache.orc.bench.core.BenchmarkOptions.HELP;
import static org.apache.orc.bench.core.BenchmarkOptions.ITERATIONS;
import static org.apache.orc.bench.core.BenchmarkOptions.MAX_MEMORY;
import static org.apache.orc.bench.core.BenchmarkOptions.MIN_MEMORY;
import static org.apache.orc.bench.core.BenchmarkOptions.TIME;
import static org.apache.orc.bench.core.BenchmarkOptions.WARMUP_ITERATIONS;

@AutoService(OrcBenchmark.class)
public class FilterBench implements OrcBenchmark {
  @Override
  public String getName() {
    return "filter";
  }

  @Override
  public String getDescription() {
    return "Perform filter bench";
  }

  @Override
  public void run(String[] args) throws Exception {
    new Runner(parseOptions(args)).run();
  }

  private static CommandLine parseCommandLine(String[] args) {
    org.apache.commons.cli.Options options = new org.apache.commons.cli.Options()
      .addOption("h", HELP, false, "Provide help")
      .addOption("i", ITERATIONS, true, "Number of iterations")
      .addOption("I", WARMUP_ITERATIONS, true, "Number of warmup iterations")
      .addOption("f", FORK, true, "How many forks to use")
      .addOption("t", TIME, true, "How long each iteration is in seconds")
      .addOption("m", MIN_MEMORY, true, "The minimum size of each JVM")
      .addOption("M", MAX_MEMORY, true, "The maximum size of each JVM")
      .addOption("g", GC, false, "Should GC be profiled");
    CommandLine result;
    try {
      result = new DefaultParser().parse(options, args, true);
    } catch (ParseException pe) {
      System.err.println("Argument exception - " + pe.getMessage());
      result = null;
    }
    if (result == null || result.hasOption(HELP) || result.getArgs().length == 0) {
      new HelpFormatter().printHelp("java -jar <jar> <command> <options> <sub_cmd>\n"
                                    + "sub_cmd:\nsimple\ncomplex\ngetvalue\n",
                                    options);
      System.err.println();
      System.exit(1);
    }
    return result;
  }

  public static Options parseOptions(String[] args) throws IOException {
    CommandLine options = parseCommandLine(args);
    String cmd = options.getArgs()[0];
    Class<?> cls;
    switch (cmd) {
      case "simple":
        cls = SimpleFilter.class;
        break;
      case "complex":
        cls = ComplexFilter.class;
        break;
      case "getvalue":
        cls = GetValue.class;
        break;
      case "access":
        cls = Access.class;
        break;
      case "equals":
        cls = Equal.class;
        break;
      case "in":
        cls = In.class;
        break;
      default:
        throw new UnsupportedOperationException(String.format("Command %s is not supported", cmd));
    }
    OptionsBuilder builder = new OptionsBuilder();
    builder.include(cls.getSimpleName());
    if (options.hasOption(GC)) {
      builder.addProfiler("hs_gc");
    }
    if (options.hasOption(ITERATIONS)) {
      builder.measurementIterations(Integer.parseInt(options.getOptionValue(ITERATIONS)));
    }
    if (options.hasOption(WARMUP_ITERATIONS)) {
      builder.warmupIterations(Integer.parseInt(options.getOptionValue(
        WARMUP_ITERATIONS)));
    }
    if (options.hasOption(FORK)) {
      builder.forks(Integer.parseInt(options.getOptionValue(
        FORK)));
    }
    if (options.hasOption(TIME)) {
      TimeValue iterationTime = TimeValue.seconds(Long.parseLong(
        options.getOptionValue(TIME)));
      builder.measurementTime(iterationTime);
      builder.warmupTime(iterationTime);
    }

    String minMemory = options.getOptionValue(MIN_MEMORY, "256m");
    String maxMemory = options.getOptionValue(MAX_MEMORY, "2g");
    builder.jvmArgs("-server",
                    "-Xms" + minMemory, "-Xmx" + maxMemory);
    return builder.build();
  }

  private static Consumer<OrcFilterContext> createFilter(SearchArgument sArg,
                                                      String fType,
                                                      boolean normalize) {
    switch (fType) {
      case "row":
        return RowFilterFactory.createRowFilter(sArg, normalize);
      case "vector":
        return FilterFactory.createVectorFilter(sArg,
                                                FilterBenchUtil.schema,
                                                OrcFile.Version.CURRENT,
                                                normalize);
      default:
        throw new IllegalArgumentException();
    }
  }

  @OutputTimeUnit(value = TimeUnit.MICROSECONDS)
  @Warmup(iterations = 20, time = 1)
  @BenchmarkMode(value = Mode.AverageTime)
  @Fork(value = 1)
  @State(value = Scope.Benchmark)
  @Measurement(iterations = 20, time = 1)
  public static class SimpleFilter {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleFilter.class);
    private OrcFilterContext fc;
    private int[] expSel;

    @Param( {"4", "8", "16", "32", "256"})
    private int fInSize;

    @Param( {"row", "vector"})
    private String fType;

    private Consumer<OrcFilterContext> f;

    @Setup
    public void setup() {
      Random rnd = new Random(1024);
      VectorizedRowBatch b = FilterBenchUtil.createBatch(rnd);

      fc = new OrcFilterContext(FilterBenchUtil.schema).setBatch(b);
      Map.Entry<SearchArgument, int[]> r = FilterBenchUtil.createSArg(rnd, b, fInSize);
      SearchArgument sArg = r.getKey();
      expSel = r.getValue();
      f = createFilter(sArg, fType, false);
      LOG.info("Created {}", f);
    }

    @Benchmark
    public void filter() {
      // Reset the selection
      FilterBenchUtil.unFilterBatch(fc);
      f.accept(fc);
    }

    @TearDown
    public void tearDown() {
      FilterBenchUtil.validate(fc, expSel);
      LOG.info("Selected {} rows", fc.getSelectedSize());
    }
  }

  @OutputTimeUnit(value = TimeUnit.MICROSECONDS)
  @Warmup(iterations = 20, time = 1)
  @BenchmarkMode(value = Mode.AverageTime)
  @Fork(value = 1)
  @State(value = Scope.Benchmark)
  @Measurement(iterations = 20, time = 1)
  public static class ComplexFilter {
    private static final Logger LOG = LoggerFactory.getLogger(ComplexFilter.class);

    private OrcFilterContext fc;
    private int[] expSel;

    private final int inSize = 32;

    @Param( {"2", "4", "8"})
    private int fSize;

    @Param( {"true", "false"})
    private boolean normalize;

    @Param( {"row", "vector"})
    private String fType;

    private Consumer<OrcFilterContext> f;
    private final Configuration conf = new Configuration();

    @Setup
    public void setup() {
      VectorizedRowBatch b = FilterBenchUtil.createBatch(new Random(1024));

      fc = new OrcFilterContext(FilterBenchUtil.schema).setBatch(b);
      conf.setBoolean("storage.sarg.normalize", normalize);
      Map.Entry<SearchArgument, int[]> r = FilterBenchUtil.createComplexSArg(new Random(1024),
                                                                             b,
                                                                             inSize,
                                                                             fSize);

      SearchArgument sArg = r.getKey();
      LOG.info("SearchArgument has {} leaves and {} expressions",
               sArg.getLeaves().size(),
               sArg.getExpression().getChildren().size());
      expSel = r.getValue();
      f = createFilter(sArg, fType, normalize);
      LOG.info("Created {}", f);
    }

    @Benchmark
    public void filter() {
      // Reset the selection
      FilterBenchUtil.unFilterBatch(fc);
      f.accept(fc);
    }

    @TearDown
    public void tearDown() {
      FilterBenchUtil.validate(fc, expSel);
      LOG.info("Selected {} rows", fc.getSelectedSize());
    }

  }

  @OutputTimeUnit(value = TimeUnit.NANOSECONDS)
  @Warmup(iterations = 2, time = 1)
  @BenchmarkMode(value = Mode.AverageTime)
  @Fork(value = 1)
  @State(value = Scope.Benchmark)
  @Measurement(iterations = 2, time = 1)
  public static class GetValue {
    private static final Logger LOG = LoggerFactory.getLogger(GetValue.class);
    private VectorizedRowBatch b;
    private long expSum = 0;
    private long sum = 0;

    @Setup
    public void setup() {
      b = FilterBenchUtil.createBatch(new Random(1024));
      LongColumnVector lv = (LongColumnVector) b.cols[0];
      for (int i = 0; i < b.size; i++) {
        expSum += lv.vector[i];
      }
      LOG.info("BatchSize={}, repeating={}, nonulls={}, expSum={}",
               b.size,
               lv.isRepeating,
               lv.noNulls,
               expSum);
    }

    @Benchmark
    public void getWithFunction() {
      sum = ComputeSum.sumWithFunction(b);
    }

    @Benchmark
    public void getWithMethod() {
      sum = ComputeSum.sumWithMethod(b);
    }

    @Benchmark
    public void direct() {
      sum = ComputeSum.sumDirect(b);
    }

    @TearDown
    public void tearDown() {
      Assert.assertEquals(expSum, sum);
    }
  }

  @OutputTimeUnit(value = TimeUnit.NANOSECONDS)
  @Warmup(iterations = 2, time = 1)
  @BenchmarkMode(value = Mode.AverageTime)
  @Fork(value = 1)
  @State(value = Scope.Benchmark)
  @Measurement(iterations = 2, time = 1)
  public static class Access {
    private static final Logger LOG = LoggerFactory.getLogger(Access.class);
    private long[] v;
    private long expSum = 0;
    private long sum = 0;

    @Setup
    public void setup() {
      v = ((LongColumnVector) FilterBenchUtil.createBatch(new Random(1024)).cols[0]).vector;
      for (long i : v) {
        expSum += i;
      }
      LOG.info("BatchSize={}, expSum={}",
               v.length,
               expSum);
    }

    @Benchmark
    public void methodAccessElements() {
      sum = 0;
      for (int i = 0; i < v.length; i++) {
        sum += getLong(v, i);
      }
    }

    @Benchmark
    public void functionAccessElements() {
      sum = 0;
      BiFunction<long[], Integer, Long> f = Access::getLong;
      for (int i = 0; i < v.length; i++) {
        sum += f.apply(v, i);
      }
    }

    private static long getLong(long[] es, int idx) {
      return es[idx];
    }

    @Benchmark
    public void indexAccessElements() {
      sum = 0;
      for (long l : v) {
        sum += l;
      }
    }

    @Benchmark
    public void iterateElements() {
      sum = 0;
      for (long i : v) {
        sum += i;
      }
    }

    @TearDown
    public void tearDown() {
      Assert.assertEquals(expSum, sum);
    }
  }

  @OutputTimeUnit(value = TimeUnit.MICROSECONDS)
  @Warmup(iterations = 2, time = 1)
  @BenchmarkMode(value = Mode.AverageTime)
  @Fork(value = 1)
  @State(value = Scope.Benchmark)
  @Measurement(iterations = 2, time = 1)
  public static class Equal {
    private static final Logger LOG = LoggerFactory.getLogger(Equal.class);
    private OrcFilterContext fc;
    private int[] expSel;
    private long value;
    private BatchFilter gFilter;
    private BatchFilter dFilter;
    int size;

    @Setup
    public void setup() {
      Random rnd = new Random(1024);
      fc = new OrcFilterContext(FilterBenchUtil.schema).setBatch(FilterBenchUtil.createBatch(rnd));
      size = fc.getSelectedSize();
      expSel = new int[size];
      LongColumnVector lv = (LongColumnVector) fc.findColumnVector("f1");
      value = lv.vector[rnd.nextInt(size)];
      int currSize = 0;
      for (int i = 0; i < size; i++) {
        if (value == lv.vector[i]) {
          expSel[currSize++] = i;
        }
      }
      LOG.info("BatchSize={}, repeating={}, nonulls={}, expCount={}",
               size,
               lv.isRepeating,
               lv.noNulls,
               currSize);
      gFilter = new BatchFilter(new LongEquals("f1", value), null);
      dFilter = new BatchFilter(new Filters.EqualsLongDirect("f1", value), null);
    }

    private void resetSel() {
      fc.setSelectedSize(size);
      fc.setSelectedInUse(false);
    }

    @Benchmark
    public void directCheck() {
      resetSel();
      int currSize = 0;
      LongColumnVector lv = (LongColumnVector) fc.findColumnVector("f1");
      long[] vector = lv.vector;
      int[] sel = fc.getSelected();
      if (lv.isRepeating && (lv.noNulls || !lv.isNull[0])) {
        if (value == vector[0]) {
          for (int i = 0; i < size; i++) {
            sel[currSize++] = i;
          }
        }
      } else if (lv.noNulls) {
        for (int i = 0; i < size; i++) {
          if (value == vector[i]) {
            sel[currSize++] = i;
          }
        }
      } else {
        for (int i = size - 1; i > -1; --i) {
          if (!lv.isNull[i] && value == vector[i]) {
            sel[currSize++] = i;
          }
        }
      }
      fc.setSelectedInUse(true);
      fc.setSelectedSize(currSize);
    }

    @Benchmark
    public void directFilter() {
      resetSel();
      dFilter.accept(fc);
    }

    @Benchmark
    public void genericFilter() {
      resetSel();
      gFilter.accept(fc);
    }

    @TearDown
    public void tearDown() {
      Assert.assertArrayEquals(expSel, fc.getSelected());
      Assert.assertTrue(fc.isSelectedInUse());
    }
  }

  @OutputTimeUnit(value = TimeUnit.MICROSECONDS)
  @Warmup(iterations = 20, time = 1)
  @BenchmarkMode(value = Mode.AverageTime)
  @Fork(value = 1)
  @State(value = Scope.Benchmark)
  @Measurement(iterations = 20, time = 1)
  public static class In {
    private static final Logger LOG = LoggerFactory.getLogger(Equal.class);
    private OrcFilterContext fc;
    private int[] expSel;
    private BatchFilter arrayFilter;
    private BatchFilter setFilter;
    private int size;

    @Param( {"8", "16", "64", "128", "1024"})
    int inSize;

    @Setup
    public void setup() {
      Random rnd = new Random(1024);
      fc = new OrcFilterContext(FilterBenchUtil.schema).setBatch(FilterBenchUtil.createBatch(rnd));
      size = fc.getSelectedSize();
      expSel = new int[size];
      LongColumnVector lv = (LongColumnVector) fc.findColumnVector("f1");
      Set<Long> sValues = new HashSet<>(inSize);
      for (int i = 0; i < inSize; i++) {
        if (i % 2 == 0) {
          sValues.add(rnd.nextLong());
        } else {
          sValues.add(lv.vector[rnd.nextInt(size)]);
        }
      }

      int currSize = 0;
      for (int i = 0; i < size; i++) {
        if (sValues.contains(lv.vector[i])) {
          expSel[currSize++] = i;
        }
      }
      LOG.info("BatchSize={}, repeating={}, nonulls={}, expCount={}",
               size,
               lv.isRepeating,
               lv.noNulls,
               currSize);
      arrayFilter = new BatchFilter(new LongIn("f1", Arrays.asList(sValues.toArray())), null);
      setFilter = new BatchFilter(new Filters.InLongSet("f1", Arrays.asList(sValues.toArray())),
                                  null);
    }

    private void resetSel() {
      fc.setSelectedSize(size);
      fc.setSelectedInUse(false);
    }

    @Benchmark
    public void testArrayFilter() {
      resetSel();
      arrayFilter.accept(fc);
    }

    @Benchmark
    public void testSetFilter() {
      resetSel();
      setFilter.accept(fc);
    }

    @TearDown
    public void teardown() {
      Assert.assertArrayEquals(expSel, fc.getSelected());
      Assert.assertTrue(fc.isSelectedInUse());
    }
  }
}
