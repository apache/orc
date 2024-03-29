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

package org.apache.orc.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;

import java.util.ArrayList;
import java.util.List;

/**
 * Check whether the specified column of multiple ORC files can filter the specified value.
 */
public class CheckTool {

  private static final String CHECK_TYPE_PREDICATE = "predicate";
  private static final String CHECK_TYPE_STAT = "stat";
  private static final String CHECK_TYPE_BLOOM_FILTER = "bloom-filter";

  public static void main(Configuration conf, String[] args) throws Exception {
    Options opts = createOptions();
    CommandLine cli = new DefaultParser().parse(opts, args);
    HelpFormatter formatter = new HelpFormatter();
    if (cli.hasOption('h')) {
      formatter.printHelp("check", opts);
      return;
    }

    String type = cli.getOptionValue("type");
    if (type == null ||
        (!type.equals(CHECK_TYPE_PREDICATE) &&
            !type.equals(CHECK_TYPE_STAT) &&
            !type.equals(CHECK_TYPE_BLOOM_FILTER))) {
      System.err.printf("type %s not support %n", type);
      formatter.printHelp("check", opts);
      return;
    }
    String column = cli.getOptionValue("column");
    if (column == null || column.isEmpty()) {
      System.err.println("column is null");
      formatter.printHelp("check", opts);
      return;
    }
    String[] values = cli.getOptionValues("values");
    if (values == null || values.length == 0) {
      System.err.println("values is null");
      formatter.printHelp("check", opts);
      return;
    }
    boolean ignoreExtension = cli.hasOption("ignoreExtension");

    List<Path> inputFiles = new ArrayList<>();
    String[] files = cli.getArgs();
    for (String root : files) {
      Path rootPath = new Path(root);
      FileSystem fs = rootPath.getFileSystem(conf);
      for (RemoteIterator<LocatedFileStatus> itr = fs.listFiles(rootPath, true); itr.hasNext(); ) {
        LocatedFileStatus status = itr.next();
        if (status.isFile() && (ignoreExtension || status.getPath().getName().endsWith(".orc"))) {
          inputFiles.add(status.getPath());
        }
      }
    }
    if (inputFiles.isEmpty()) {
      System.err.println("No files found.");
      System.exit(1);
    }

    for (Path inputFile : inputFiles) {
      System.out.println("input file: " + inputFile);
      FileSystem fs = inputFile.getFileSystem(conf);
      try (Reader reader = OrcFile.createReader(inputFile,
          OrcFile.readerOptions(conf).filesystem(fs))) {
        RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
        TypeDescription schema = reader.getSchema();
        boolean[] includedColumns = OrcUtils.includeColumns(column, schema);
        int colIndex = -1;
        for (int i = 0; i < includedColumns.length; i++) {
          if (includedColumns[i]) {
            colIndex = i;
            break;
          }
        }
        if (colIndex == -1) {
          System.err.printf("column: %s not found in file: %s%n", column, inputFile);
          continue;
        }
        int stripeIndex = -1;
        for (StripeInformation stripe : reader.getStripes()) {
          ++stripeIndex;

          OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);

          OrcProto.ColumnEncoding columnEncoding = footer.getColumns(colIndex);
          TypeDescription subtype = reader.getSchema().findSubtype(colIndex);
          TypeDescription.Category columnCategory = subtype.getCategory();
          OrcIndex indices = rows.readRowIndex(stripeIndex, null, includedColumns);
          if (type.equals(CHECK_TYPE_BLOOM_FILTER)) {
            checkBloomFilter(inputFile, reader, indices, stripeIndex,
                colIndex, column, columnEncoding, columnCategory, values);
          } else {
            checkStatOrPredicate(inputFile, reader, indices, stripeIndex,
                colIndex, column, columnEncoding, subtype, columnCategory, values, type);
          }
        }
      }
    }
  }

  private static void checkStatOrPredicate(Path inputFile,
      Reader reader,
      OrcIndex indices,
      int stripeIndex,
      int colIndex,
      String column,
      OrcProto.ColumnEncoding columnEncoding,
      TypeDescription subtype,
      TypeDescription.Category columnCategory,
      String[] values,
      String type) {
    OrcProto.RowIndex rowGroupIndex = indices.getRowGroupIndex()[colIndex];
    int entryCount = rowGroupIndex.getEntryCount();
    boolean hasBloomFilter = true;
    OrcProto.BloomFilterIndex[] bloomFilterIndices = indices.getBloomFilterIndex();
    OrcProto.BloomFilterIndex bloomFilterIndex = bloomFilterIndices[colIndex];
    if (bloomFilterIndex == null || bloomFilterIndex.getBloomFilterList().isEmpty()) {
      hasBloomFilter = false;
    }
    for (int i = 0; i < entryCount; i++) {
      OrcProto.ColumnStatistics statistics = rowGroupIndex.getEntry(i).getStatistics();
      ColumnStatistics cs = ColumnStatisticsImpl.deserialize(subtype,
          statistics,
          reader.writerUsedProlepticGregorian(),
          reader.getConvertToProlepticGregorian());

      BloomFilter bloomFilter = null;
      if (type.equals(CHECK_TYPE_PREDICATE) && hasBloomFilter) {
        bloomFilter = BloomFilterIO.deserialize(
            indices.getBloomFilterKinds()[colIndex], columnEncoding,
            reader.getWriterVersion(), columnCategory, bloomFilterIndex.getBloomFilter(i));
      }

      for (String value : values) {
        PredicateLeaf predicateLeaf = createPredicateLeaf(PredicateLeaf.Operator.EQUALS,
            getPredicateLeafType(columnCategory), column, convert(columnCategory, value));
        SearchArgument.TruthValue truthValue = RecordReaderImpl.evaluatePredicate(
            cs, predicateLeaf, bloomFilter);
        System.out.printf("stripe: %d, rowIndex: %d, value: %s, test value: %s%n",
            stripeIndex, i, value, truthValue);
      }
    }
  }

  private static void checkBloomFilter(Path inputFile,
      Reader reader,
      OrcIndex indices,
      int stripeIndex,
      int colIndex,
      String column,
      OrcProto.ColumnEncoding columnEncoding,
      TypeDescription.Category columnCategory,
      String[] values) {
    OrcProto.BloomFilterIndex[] bloomFilterIndices = indices.getBloomFilterIndex();
    OrcProto.BloomFilterIndex bloomFilterIndex = bloomFilterIndices[colIndex];
    if (bloomFilterIndex == null || bloomFilterIndex.getBloomFilterList().isEmpty()) {
      System.err.printf("The bloom filter index for column: %s is not found in file: %s%n",
          column, inputFile);
      return;
    }
    List<OrcProto.BloomFilter> bloomFilterList = bloomFilterIndex.getBloomFilterList();
    for (int i = 0; i < bloomFilterList.size(); i++) {
      OrcProto.BloomFilter bf = bloomFilterList.get(i);
      org.apache.orc.util.BloomFilter bloomFilter = BloomFilterIO.deserialize(
          indices.getBloomFilterKinds()[colIndex], columnEncoding,
          reader.getWriterVersion(), columnCategory, bf);
      for (String value : values) {
        boolean testResult = test(bloomFilter, columnCategory, value);
        if (testResult) {
          System.out.printf("stripe: %d, rowIndex: %d, value: %s, bloom filter: maybe exist%n",
              stripeIndex, i, value);
        } else {
          System.out.printf("stripe: %d, rowIndex: %d, value: %s, bloom filter: not exist%n",
              stripeIndex, i, value);
        }
      }
    }
  }

  private static boolean test(BloomFilter bloomFilter,
      TypeDescription.Category columnCategory, String value) {
    switch (columnCategory){
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case DATE:
      case TIMESTAMP:
        return bloomFilter.testLong(Long.parseLong(value));
      case FLOAT:
      case DOUBLE:
        return bloomFilter.testDouble(Double.parseDouble(value));
      case STRING:
      case CHAR:
      case VARCHAR:
      case DECIMAL:
        return bloomFilter.testString(value);
      default:
        throw new IllegalStateException("Not supported type:" + columnCategory);
    }
  }

  private static Object convert(
      TypeDescription.Category columnCategory, String value) {
    switch (columnCategory) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case DATE:
      case TIMESTAMP:
        return Long.parseLong(value);
      case FLOAT:
      case DOUBLE:
        return Double.parseDouble(value);
      case STRING:
      case CHAR:
      case VARCHAR:
      case DECIMAL:
        return value;
      default:
        throw new IllegalStateException("Not supported type:" + columnCategory);
    }
  }

  private static PredicateLeaf.Type getPredicateLeafType(TypeDescription.Category columnCategory) {
    switch (columnCategory){
      case BOOLEAN:
        return PredicateLeaf.Type.BOOLEAN;
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return PredicateLeaf.Type.LONG;
      case DATE:
        return PredicateLeaf.Type.DATE;
      case TIMESTAMP:
        return  PredicateLeaf.Type.TIMESTAMP;
      case FLOAT:
      case DOUBLE:
        return  PredicateLeaf.Type.FLOAT;
      case STRING:
      case CHAR:
      case VARCHAR:
      case DECIMAL:
        return PredicateLeaf.Type.STRING;
      default:
        throw new IllegalStateException("Not supported type:" + columnCategory);
    }
  }

  private static PredicateLeaf createPredicateLeaf(PredicateLeaf.Operator operator,
      PredicateLeaf.Type type,
      String columnName,
      Object literal) {
    return new SearchArgumentImpl.PredicateLeafImpl(operator, type, columnName,
        literal, null);
  }

  private static Options createOptions() {
    Options result = new Options();

    result.addOption(Option.builder("t")
        .longOpt("type")
        .desc(String.format("check type = {%s, %s, %s}",
            CHECK_TYPE_PREDICATE, CHECK_TYPE_STAT, CHECK_TYPE_BLOOM_FILTER))
        .hasArg()
        .build());

    result.addOption(Option.builder("col")
        .longOpt("column")
        .desc("column name")
        .hasArg()
        .build());

    result.addOption(Option.builder("v")
        .longOpt("values")
        .desc("test values")
        .hasArgs()
        .build());

    result.addOption(Option.builder("h")
        .longOpt("help")
        .desc("print help message")
        .build());
    return result;
  }
}
