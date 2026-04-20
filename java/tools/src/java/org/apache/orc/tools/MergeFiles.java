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
 * Merge multiple ORC files that all have the same schema into one or more ORC files.
 * When {@code --maxSize} is specified, the tool splits output into multiple part files
 * under the given output directory, each not exceeding the specified size threshold.
 */
public class MergeFiles {

  static final String PART_FILE_FORMAT = "part-%05d.orc";

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

    long maxSizeBytes = 0;
    if (cli.hasOption("maxSize")) {
      try {
        maxSizeBytes = Long.parseLong(cli.getOptionValue("maxSize"));
      } catch (NumberFormatException e) {
        System.err.println("--maxSize requires a numeric value in bytes.");
        System.exit(1);
      }
      if (maxSizeBytes <= 0) {
        System.err.println("--maxSize must be a positive number of bytes.");
        System.exit(1);
      }
    }

    List<LocatedFileStatus> inputStatuses = new ArrayList<>();
    String[] files = cli.getArgs();
    for (String root : files) {
      Path rootPath = new Path(root);
      FileSystem fs = rootPath.getFileSystem(conf);
      for (RemoteIterator<LocatedFileStatus> itr = fs.listFiles(rootPath, true); itr.hasNext(); ) {
        LocatedFileStatus status = itr.next();
        if (status.isFile() && (ignoreExtension || status.getPath().getName().endsWith(".orc"))) {
          inputStatuses.add(status);
        }
      }
    }
    if (inputStatuses.isEmpty()) {
      System.err.println("No files found.");
      System.exit(1);
    }

    List<Path> inputFiles = new ArrayList<>(inputStatuses.size());
    for (LocatedFileStatus s : inputStatuses) {
      inputFiles.add(s.getPath());
    }

    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf);

    if (maxSizeBytes > 0) {
      mergeIntoMultipleFiles(conf, writerOptions, inputStatuses, inputFiles,
          new Path(outputFilename), maxSizeBytes);
    } else {
      mergeIntoSingleFile(writerOptions, inputFiles, new Path(outputFilename), outputFilename);
    }
  }

  /**
   * Original single-output behavior (no --maxSize).
   */
  private static void mergeIntoSingleFile(OrcFile.WriterOptions writerOptions,
                                           List<Path> inputFiles,
                                           Path outputPath,
                                           String outputFilename) throws Exception {
    List<Path> mergedFiles = OrcFile.mergeFiles(outputPath, writerOptions, inputFiles);

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

  /**
   * Multi-output behavior when --maxSize is set.
   * Input files are grouped by cumulative raw file size; each group is merged into
   * a separate part file (part-00000.orc, part-00001.orc, ...) under outputDir.
   * A single file whose size already exceeds maxSizeBytes is placed in its own part.
   */
  private static void mergeIntoMultipleFiles(Configuration conf,
                                              OrcFile.WriterOptions writerOptions,
                                              List<LocatedFileStatus> inputStatuses,
                                              List<Path> inputFiles,
                                              Path outputDir,
                                              long maxSizeBytes) throws Exception {
    FileSystem outFs = outputDir.getFileSystem(conf);
    if (outFs.exists(outputDir)) {
      if (!outFs.getFileStatus(outputDir).isDirectory()) {
        throw new IllegalArgumentException(
            "Output path already exists and is not a directory: " + outputDir);
      }
      if (outFs.listStatus(outputDir).length > 0) {
        throw new IllegalArgumentException(
            "Output directory must be empty for multi-file merge: " + outputDir);
      }
    } else if (!outFs.mkdirs(outputDir)) {
      throw new IllegalStateException("Failed to create output directory: " + outputDir);
    }

    // Group input files into batches where each batch's total size <= maxSizeBytes.
    List<List<Path>> batches = new ArrayList<>();
    List<Path> currentBatch = new ArrayList<>();
    long currentBatchSize = 0;

    for (LocatedFileStatus status : inputStatuses) {
      long fileSize = status.getLen();
      if (!currentBatch.isEmpty() && currentBatchSize + fileSize > maxSizeBytes) {
        batches.add(currentBatch);
        currentBatch = new ArrayList<>();
        currentBatchSize = 0;
      }
      currentBatch.add(status.getPath());
      currentBatchSize += fileSize;
    }
    if (!currentBatch.isEmpty()) {
      batches.add(currentBatch);
    }

    int totalMerged = 0;
    List<Path> allUnmerged = new ArrayList<>();

    for (int i = 0; i < batches.size(); i++) {
      List<Path> batch = batches.get(i);
      Path partOutput = new Path(outputDir, String.format(PART_FILE_FORMAT, i));
      List<Path> merged = OrcFile.mergeFiles(partOutput, writerOptions.clone(), batch);
      totalMerged += merged.size();

      if (merged.size() != batch.size()) {
        Set<Path> mergedSet = new HashSet<>(merged);
        for (Path p : batch) {
          if (!mergedSet.contains(p)) {
            allUnmerged.add(p);
          }
        }
      }
    }

    if (!allUnmerged.isEmpty()) {
      System.err.println("List of files that could not be merged:");
      allUnmerged.forEach(path -> System.err.println(path.toString()));
    }

    System.out.printf(
        "Output path: %s, Input files size: %d, Merge files size: %d, Output files: %d%n",
        outputDir, inputFiles.size(), totalMerged, batches.size());
    if (!allUnmerged.isEmpty()) {
      System.exit(1);
    }
  }

  private static Options createOptions() {
    Options result = new Options();
    result.addOption(Option.builder("o")
        .longOpt("output")
        .desc("Output filename (single-file mode) or output directory (multi-file mode)")
        .hasArg()
        .build());

    result.addOption(Option.builder("i")
        .longOpt("ignoreExtension")
        .desc("Ignore ORC file extension")
        .build());

    result.addOption(Option.builder("m")
        .longOpt("maxSize")
        .desc("Maximum cumulative input file size in bytes per output ORC file. When set, "
            + "--output is treated as an output directory and merged files are written as "
            + "part-00000.orc, part-00001.orc, etc. Input files are grouped at file "
            + "boundaries so an individual file larger than this threshold will still be "
            + "placed in its own part.")
        .hasArg()
        .build());

    result.addOption(Option.builder("h")
        .longOpt("help")
        .desc("Print help message")
        .build());
    return result;
  }
}
