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
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcUtils;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.util.BloomFilterIO;

import java.util.ArrayList;
import java.util.List;

/**
 * Check whether the specified column of multiple ORC files can filter the specified value.
 */
public class BloomFilter {

  public static void main(Configuration conf, String[] args) throws Exception {
    Options opts = createOptions();
    CommandLine cli = new DefaultParser().parse(opts, args);
    HelpFormatter formatter = new HelpFormatter();
    if (cli.hasOption('h')) {
      formatter.printHelp("bloom-filter", opts);
      return;
    }

    String column = cli.getOptionValue("column");
    if (column == null || column.isEmpty()) {
      System.err.println("column is null");
      formatter.printHelp("bloom-filter", opts);
      return;
    }
    String[] values = cli.getOptionValues("values");
    if (values == null || values.length == 0) {
      System.err.println("values is null");
      formatter.printHelp("bloom-filter", opts);
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
      try(Reader reader = OrcFile.createReader(inputFile,
          OrcFile.readerOptions(conf).filesystem(fs))) {
        RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
        TypeDescription schema = reader.getSchema();
        boolean[] bloomFilterColumns = OrcUtils.includeColumns(column, schema);
        int colIndex = -1;
        for (int i = 0; i < bloomFilterColumns.length; i++) {
          if (bloomFilterColumns[i]) {
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
          TypeDescription.Category columnCategory =
              reader.getSchema().findSubtype(colIndex).getCategory();
          OrcIndex indices = rows.readRowIndex(stripeIndex, null, bloomFilterColumns);
          OrcProto.BloomFilterIndex[] bloomFilterIndices = indices.getBloomFilterIndex();
          OrcProto.BloomFilterIndex bloomFilterIndex = bloomFilterIndices[colIndex];
          if (bloomFilterIndex == null || bloomFilterIndex.getBloomFilterList().isEmpty()) {
            System.err.printf("The bloom filter index for column: %s is not found in file: %s%n",
                column, inputFile);
            continue;
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
                System.out.printf("stripe: %d, rowIndex: %d, value: %s maybe exist%n",
                    stripeIndex, i, value);
              } else {
                System.out.printf("stripe: %d, rowIndex: %d, value: %s not exist%n",
                    stripeIndex, i, value);
              }
            }
          }
        }
      }
    }
  }

  private static boolean test(org.apache.orc.util.BloomFilter bloomFilter,
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

  private static Options createOptions() {
    Options result = new Options();
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
        .desc("Print help message")
        .build());
    return result;
  }
}
