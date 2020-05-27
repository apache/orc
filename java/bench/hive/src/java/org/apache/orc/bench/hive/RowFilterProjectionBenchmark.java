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

import com.google.auto.service.AutoService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrackingLocalFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.OrcBenchmark;
import org.apache.orc.bench.core.ReadCounters;
import org.apache.orc.bench.core.Utilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@AutoService(OrcBenchmark.class)
public class RowFilterProjectionBenchmark implements OrcBenchmark {

  private static final Path root = Utilities.getBenchmarkRoot();

  @Param({ "github", "sales", "taxi"})
  public String dataset;

//  @Param({"none", "snappy", "gz"})
  @Param({"none"})
  public String compression;

  @Param({"0.01", "0.1", "0.2", "0.4", "0.6", "0.8", "1."})
  public String filter_percentage;

  @Param({"all", "2", "5", "10", "20"})
  public String projected_columns;

  @Override
  public String getName() {
    return "row-filter";
  }

  @Override
  public String getDescription() {
    return "Benchmark column projection with row-level filtering";
  }

  @Override
  public void run(String[] args) throws Exception {
    new Runner(Utilities.parseOptions(args, getClass())).run();
  }

  static Set<Integer> filterValues = null;
  public static void generateRandomSet(double percentage) throws IllegalArgumentException {
    if (percentage > 1.0) {
      throw new IllegalArgumentException("Filter percentage must be < 1.0 but was "+ percentage);
    }
    filterValues = new HashSet<>();
    while (filterValues.size() < (1024 * percentage)) {
      Random randomGenerator = new Random();
      filterValues.add(randomGenerator.nextInt(1024));
    }
  }

  public static void customIntRowFilter(VectorizedRowBatch batch) {
    int newSize = 0;
    for (int row = 0; row < batch.size; ++row) {
      // Pass ony Valid key
      if (filterValues.contains(row)) {
        batch.selected[newSize++] = row;
      }
    }
    batch.selectedInUse = true;
    batch.size = newSize;
  }

  @Benchmark
  public void orcRowFilter(ReadCounters counters) throws Exception {
    Configuration conf = new Configuration();
    TrackingLocalFileSystem fs = new TrackingLocalFileSystem();
    fs.initialize(new URI("file:///"), conf);
    FileSystem.Statistics statistics = fs.getLocalStatistics();
    statistics.reset();
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf).filesystem(fs);
    Path path = Utilities.getVariant(root, dataset, "orc", compression);
    Reader reader = OrcFile.createReader(path, options);
    TypeDescription schema = reader.getSchema();
    // select an ID column to apply filter on
    String filter_column;
    if ("taxi".equals(dataset)) {
      filter_column = "vendor_id";
    } else if ("sales".equals(dataset)) {
      filter_column = "sales_id";
    } else if ("github".equals(dataset)) {
      filter_column = "id";
    } else {
      throw new IllegalArgumentException("Unknown data set " + dataset);
    }
    boolean[] include = new boolean[schema.getMaximumId() + 1];
    int columns_len = schema.getMaximumId();
    if (projected_columns.compareTo("all") != 0) {
      columns_len = Integer.parseInt(projected_columns);
    }
    // select the remaining columns to project
    List<TypeDescription> children = schema.getChildren();
    boolean foundFilterCol = false;
    for (int c = children.get(0).getId(); c < schema.getMaximumId() + 1; ++c) {
      if (c < schema.getFieldNames().size() && schema.getFieldNames().get(c-1).compareTo(filter_column) == 0) {
        foundFilterCol = true;
        include[c] = true;
      }
      else {
        if (columns_len > 0) {
          include[c] = true;
          columns_len--;
        }
      }
      if (foundFilterCol && (columns_len == 0)) break;
    }
    generateRandomSet(Double.parseDouble(filter_percentage));
    RecordReader rows =
        reader.rows(reader.options()
          .include(include)
          .setRowFilter(new String[]{filter_column}, RowFilterProjectionBenchmark::customIntRowFilter));

    VectorizedRowBatch batch = schema.createRowBatch();
    while (rows.nextBatch(batch)) {
      counters.addRecords(batch.size);
    }
    rows.close();
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
    counters.addInvocation();
  }

  @Benchmark
  public void orcNoFilter(ReadCounters counters) throws Exception {
    Configuration conf = new Configuration();
    TrackingLocalFileSystem fs = new TrackingLocalFileSystem();
    fs.initialize(new URI("file:///"), conf);
    FileSystem.Statistics statistics = fs.getLocalStatistics();
    statistics.reset();
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf).filesystem(fs);
    Path path = Utilities.getVariant(root, dataset, "orc", compression);
    Reader reader = OrcFile.createReader(path, options);
    TypeDescription schema = reader.getSchema();
    // select an ID column to apply filter on
    String filter_column;
    if ("taxi".equals(dataset)) {
      filter_column = "vendor_id";
    } else if ("sales".equals(dataset)) {
      filter_column = "sales_id";
    } else if ("github".equals(dataset)) {
      filter_column = "id";
    } else {
      throw new IllegalArgumentException("Unknown data set " + dataset);
    }
    boolean[] include = new boolean[schema.getMaximumId() + 1];
    int columns_len = schema.getMaximumId();
    if (projected_columns.compareTo("all") != 0) {
      columns_len = Integer.parseInt(projected_columns);
    }
    // select the remaining columns to project
    List<TypeDescription> children = schema.getChildren();
    boolean foundFilterCol = false;
    for (int c = children.get(0).getId(); c < schema.getMaximumId() + 1; ++c) {
      if (c < schema.getFieldNames().size() && schema.getFieldNames().get(c-1).compareTo(filter_column) == 0) {
        foundFilterCol = true;
        include[c] = true;
      }
      else {
        if (columns_len > 0) {
          include[c] = true;
          columns_len--;
        }
      }
      if (foundFilterCol && (columns_len == 0)) break;
    }
    RecordReader rows = reader.rows(reader.options().include(include));

    VectorizedRowBatch batch = schema.createRowBatch();
    while (rows.nextBatch(batch)) {
      counters.addRecords(batch.size);
    }
    rows.close();
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
    counters.addInvocation();
  }

  @Benchmark
  public void filterCorrectness(ReadCounters counters) throws Exception {
    Configuration conf = new Configuration();
    TrackingLocalFileSystem fs = new TrackingLocalFileSystem();
    fs.initialize(new URI("file:///"), conf);
    FileSystem.Statistics statistics = fs.getLocalStatistics();
    statistics.reset();
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf).filesystem(fs);
    Path path = Utilities.getVariant(root, dataset, "orc", compression);
    Reader reader = OrcFile.createReader(path, options);

    TrackingLocalFileSystem cleanFs = new TrackingLocalFileSystem();
    cleanFs.initialize(new URI("file:///"), conf);
    OrcFile.ReaderOptions cleanOptions = OrcFile.readerOptions(conf).filesystem(cleanFs);
    Reader cleanReader = OrcFile.createReader(path, cleanOptions);

    TypeDescription schema = reader.getSchema();
    // select an ID column to apply filter on
    String filter_column;
    if ("taxi".equals(dataset)) {
      filter_column = "vendor_id";
    } else if ("sales".equals(dataset)) {
      filter_column = "sales_id";
    } else if ("github".equals(dataset)) {
      filter_column = "id";
    } else {
      throw new IllegalArgumentException("Unknown data set " + dataset);
    }
    boolean[] include = new boolean[schema.getMaximumId() + 1];

    // select the remaining columns to project
    List<TypeDescription> children = schema.getChildren();
    for (int c = children.get(0).getId(); c < schema.getMaximumId() + 1; ++c) {
        include[c] = true;
    }

    generateRandomSet(Double.parseDouble(filter_percentage));
    RecordReader rows =
        reader.rows(reader.options()
            .include(include)
            .setRowFilter(new String[]{filter_column}, RowFilterProjectionBenchmark::customIntRowFilter));

    RecordReader cleanRows = cleanReader.rows(reader.options().include(include));

    VectorizedRowBatch batch = schema.createRowBatch();
    VectorizedRowBatch cleanBatch = schema.createRowBatch();

    while (rows.nextBatch(batch) && cleanRows.nextBatch(cleanBatch)) {
      counters.addRecords(batch.size);
      for (int col = 0; col < batch.numCols; col++) {
        for (int rowId : filterValues) {
          StringBuilder cleanSB = new StringBuilder();
          Exception cleanException = null;
          StringBuilder filteredSB = new StringBuilder();
          Exception filterException = null;
          try {
            cleanBatch.cols[col].stringifyValue(cleanSB, rowId);
          } catch (NullPointerException ex) {
            cleanException = ex;
          }
          try {
            batch.cols[col].stringifyValue(filteredSB, rowId);
          } catch (NullPointerException ex) {
            filterException = ex;
          }
          if ((filterException == null && cleanException != null) || (cleanException == null && filterException != null)
              || (cleanSB.toString().compareTo(filteredSB.toString()) != 0)) {
            System.err.println("Possible Issue in Col: " + col + " RowId: " + rowId + "\n");
            System.err.println("Filtered: \t" + filteredSB + "\t" + filterException);
            System.err.println("Clean: \t" + cleanSB + "\t" + cleanException);
            break;
          }
        }
      }
    }
    rows.close();
    cleanRows.close();
    counters.addBytes(statistics.getReadOps(), statistics.getBytesRead());
    counters.addInvocation();
  }
}