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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Merge multiple ORC files that all have the same schema into a single ORC file.
 */
public class MergeFiles {

  public static void main(Configuration conf, String[] args) throws Exception {
    Options opts = createOptions();
    CommandLine cli = new DefaultParser().parse(opts, args);
    HelpFormatter formatter = new HelpFormatter();
    if (cli.hasOption('h')) {
      formatter.printHelp("merge", opts);
      return;
    }
    String outputFilename = cli.getOptionValue("output");
    if (outputFilename == null || outputFilename.isEmpty()) {
      System.err.println("output filename is null");
      formatter.printHelp("merge", opts);
      return;
    }
    boolean ignoreExtension = cli.hasOption("ignoreExtension");

    List<Path> inputFiles = new ArrayList<>();
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf);

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

    List<Path> mergedFiles = OrcFile.mergeFiles(
        new Path(outputFilename), writerOptions, inputFiles);

    List<Path> unSuccessMergedFiles = new ArrayList<>();
    if (mergedFiles.size() != inputFiles.size()) {
      Set<Path> mergedFilesSet = new HashSet<>(mergedFiles);
      for (Path inputFile : inputFiles) {
        if (!mergedFilesSet.contains(inputFile)) {
          unSuccessMergedFiles.add(inputFile);
        }
      }
    }

    if (!unSuccessMergedFiles.isEmpty()) {
      System.err.println("List of files that could not be merged:");
      unSuccessMergedFiles.forEach(path -> System.err.println(path.toString()));
    }

    System.out.printf("Output path: %s, Input files size: %d, Merge files size: %d%n",
        outputFilename, inputFiles.size(), mergedFiles.size());
    if (!unSuccessMergedFiles.isEmpty()) {
      System.exit(1);
    }
  }

  private static Options createOptions() {
    Options result = new Options();
    result.addOption(Option.builder("o")
        .longOpt("output")
        .desc("Output filename")
        .hasArg()
        .build());

    result.addOption(Option.builder("i")
        .longOpt("ignoreExtension")
        .desc("Ignore ORC file extension")
        .build());

    result.addOption(Option.builder("h")
        .longOpt("help")
        .desc("Print help message")
        .build());
    return result;
  }
}
