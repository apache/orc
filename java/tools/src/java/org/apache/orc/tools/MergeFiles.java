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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.orc.OrcFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
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
      System.err.println("--output path is required.");
      formatter.printHelp("merge", opts);
      return;
    }
    boolean ignoreExtension = cli.hasOption("ignoreExtension");
    boolean preserveStructure = cli.hasOption("preserveStructure");
    boolean overwrite = cli.hasOption("overwrite");

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

    String[] files = cli.getArgs();
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf);
    Path outputPath = new Path(outputFilename);

    // Multi-file modes rewrite the output directory in place, so its subtree must
    // not overlap with any input root; otherwise --overwrite could delete files
    // that were already enumerated as inputs.
    if (preserveStructure || maxSizeBytes > 0) {
      assertOutputNotOverlappingInputs(conf, outputPath, files);
    }

    if (preserveStructure) {
      if (files.length != 1) {
        System.err.println(
            "--preserveStructure requires exactly one input directory.");
        System.exit(1);
      }
      mergePreserveStructure(conf, writerOptions, new Path(files[0]),
          outputPath, maxSizeBytes, ignoreExtension, overwrite);
      return;
    }

    List<LocatedFileStatus> inputStatuses = new ArrayList<>();
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

    inputStatuses.sort(Comparator.comparing(FileStatus::getPath));

    if (maxSizeBytes > 0) {
      mergeIntoMultipleFiles(conf, writerOptions, inputStatuses,
          outputPath, maxSizeBytes, overwrite);
    } else {
      List<Path> inputFiles = new ArrayList<>(inputStatuses.size());
      for (LocatedFileStatus s : inputStatuses) {
        inputFiles.add(s.getPath());
      }
      mergeIntoSingleFile(writerOptions, inputFiles, outputPath, outputFilename);
    }
  }

  /**
   * Reject any configuration where {@code outputPath} equals, lies under, or
   * contains any of the given input roots (after qualification). Used by
   * multi-file output modes where the output directory is rewritten in place.
   */
  private static void assertOutputNotOverlappingInputs(Configuration conf,
                                                       Path outputPath,
                                                       String[] inputRoots) throws IOException {
    FileSystem outFs = outputPath.getFileSystem(conf);
    Path qualifiedOutput = outFs.makeQualified(outputPath);
    String outStr = qualifiedOutput.toString();
    String outPrefix = outStr.endsWith("/") ? outStr : outStr + "/";
    for (String root : inputRoots) {
      Path rootPath = new Path(root);
      FileSystem inFs = rootPath.getFileSystem(conf);
      Path qualifiedInput = inFs.makeQualified(rootPath);
      String inStr = qualifiedInput.toString();
      String inPrefix = inStr.endsWith("/") ? inStr : inStr + "/";
      if (outStr.equals(inStr) || outStr.startsWith(inPrefix) || inStr.startsWith(outPrefix)) {
        throw new IllegalArgumentException(
            "Output path must not overlap with any input path: "
                + "output=" + outputPath + ", input=" + root);
      }
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
                                              Path outputDir,
                                              long maxSizeBytes,
                                              boolean overwrite) throws Exception {
    DirMergeResult r = mergeBatchedIntoDir(conf, writerOptions, inputStatuses,
        outputDir, maxSizeBytes, overwrite);

    if (!r.unmergedFiles.isEmpty()) {
      System.err.println("List of files that could not be merged:");
      r.unmergedFiles.forEach(path -> System.err.println(path.toString()));
    }

    System.out.printf(
        "Output path: %s, Input files size: %d, Merge files size: %d, Output files: %d%n",
        outputDir, inputStatuses.size(), r.mergedFileCount, r.partFileCount);
    if (!r.unmergedFiles.isEmpty()) {
      System.exit(1);
    }
  }

  /**
   * Preserve the relative directory structure of {@code inputRoot} under
   * {@code outputRoot}: every directory containing ORC files (a "leaf") is merged
   * independently, and its relative path is mirrored beneath {@code outputRoot}.
   *
   * <p>Leaves are located by a depth-first walk of {@code inputRoot}. A directory
   * that contains both ORC files and subdirectories is considered ambiguous and
   * raises an error. Hidden entries (names starting with {@code '_'} or
   * {@code '.'}) are always skipped, matching Hive/Spark conventions for markers
   * like {@code _SUCCESS}, {@code _committed_*} or {@code _temporary}.
   *
   * <p>If {@code --maxSize} was not supplied ({@code maxSizeBytes == 0}), each
   * leaf is merged into a single {@code part-00000.orc} file; otherwise
   * ({@code maxSizeBytes > 0}, already validated in {@link #main}) the files
   * under a leaf are split into size-bounded part files exactly as in the flat
   * multi-file mode.
   */
  private static void mergePreserveStructure(Configuration conf,
                                             OrcFile.WriterOptions writerOptions,
                                             Path inputRoot,
                                             Path outputRoot,
                                             long maxSizeBytes,
                                             boolean ignoreExtension,
                                             boolean overwrite) throws Exception {
    FileSystem inFs = inputRoot.getFileSystem(conf);
    if (!inFs.exists(inputRoot) || !inFs.getFileStatus(inputRoot).isDirectory()) {
      throw new IllegalArgumentException(
          "Input path must be an existing directory with --preserveStructure: " + inputRoot);
    }

    Path qualifiedInputRoot = inFs.makeQualified(inputRoot);
    List<Path> leaves = new ArrayList<>();
    collectLeafDirs(inFs, qualifiedInputRoot, ignoreExtension, leaves);

    if (leaves.isEmpty()) {
      System.err.println("No leaf directories containing ORC files found under: " + inputRoot);
      System.exit(1);
    }

    FileSystem outFs = outputRoot.getFileSystem(conf);
    prepareOutputDir(outFs, outputRoot, overwrite, "--preserveStructure");

    long effectiveMax = maxSizeBytes > 0 ? maxSizeBytes : Long.MAX_VALUE;
    int totalInputFiles = 0;
    int totalMerged = 0;
    int totalPartFiles = 0;
    List<Path> allUnmerged = new ArrayList<>();

    for (Path leaf : leaves) {
      String relative = relativize(qualifiedInputRoot, inFs.makeQualified(leaf));
      Path outputLeaf = relative.isEmpty() ? outputRoot : new Path(outputRoot, relative);

      List<LocatedFileStatus> leafStatuses =
          listLeafFiles(inFs, leaf, ignoreExtension);
      if (leafStatuses.isEmpty()) {
        continue;
      }

      // Each leaf's output directory lives under a freshly-prepared outputRoot,
      // so we don't need (and shouldn't force) overwrite semantics here.
      DirMergeResult r = mergeBatchedIntoDir(
          conf, writerOptions, leafStatuses, outputLeaf, effectiveMax, false);

      totalInputFiles += leafStatuses.size();
      totalMerged += r.mergedFileCount;
      totalPartFiles += r.partFileCount;
      allUnmerged.addAll(r.unmergedFiles);

      System.out.printf(
          "Leaf: %s -> %s, Input files: %d, Merge files: %d, Output files: %d%n",
          leaf, outputLeaf, leafStatuses.size(), r.mergedFileCount, r.partFileCount);
    }

    if (!allUnmerged.isEmpty()) {
      System.err.println("List of files that could not be merged:");
      allUnmerged.forEach(path -> System.err.println(path.toString()));
    }

    System.out.printf(
        "Output root: %s, Leaves: %d, Total input files: %d, "
            + "Total merge files: %d, Total output files: %d%n",
        outputRoot, leaves.size(), totalInputFiles, totalMerged, totalPartFiles);
    if (!allUnmerged.isEmpty()) {
      System.exit(1);
    }
  }

  /**
   * Core per-directory merge: groups {@code inputStatuses} into size-bounded batches
   * and writes each batch as {@code part-NNNNN.orc} under {@code outputDir}. This
   * helper contains no I/O side effects on stdout/stderr or process exit; callers
   * aggregate/report results themselves.
   */
  private static DirMergeResult mergeBatchedIntoDir(Configuration conf,
                                                    OrcFile.WriterOptions writerOptions,
                                                    List<LocatedFileStatus> inputStatuses,
                                                    Path outputDir,
                                                    long maxSizeBytes,
                                                    boolean overwrite) throws Exception {
    FileSystem outFs = outputDir.getFileSystem(conf);
    prepareOutputDir(outFs, outputDir, overwrite, "multi-file merge");

    List<List<Path>> batches = new ArrayList<>();
    List<Path> currentBatch = new ArrayList<>();
    long currentBatchSize = 0;

    for (LocatedFileStatus status : inputStatuses) {
      long fileSize = status.getLen();
      if (!currentBatch.isEmpty() && currentBatchSize > maxSizeBytes - fileSize) {
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
    int partFilesWritten = 0;
    List<Path> allUnmerged = new ArrayList<>();

    // Advance the part index only when a file is actually written, so
    // part-NNNNN.orc stays contiguous and the reported count matches disk.
    for (List<Path> batch : batches) {
      Path partOutput = new Path(outputDir, String.format(PART_FILE_FORMAT, partFilesWritten));
      List<Path> merged = OrcFile.mergeFiles(partOutput, writerOptions.clone(), batch);

      if (merged.isEmpty()) {
        // Drop any 0-stripe placeholder left behind and skip this slot.
        if (outFs.exists(partOutput)) {
          outFs.delete(partOutput, false);
        }
        allUnmerged.addAll(batch);
        continue;
      }

      totalMerged += merged.size();
      partFilesWritten++;

      if (merged.size() != batch.size()) {
        Set<Path> mergedSet = new HashSet<>(merged);
        for (Path p : batch) {
          if (!mergedSet.contains(p)) {
            allUnmerged.add(p);
          }
        }
      }
    }

    return new DirMergeResult(partFilesWritten, totalMerged, allUnmerged);
  }

  /**
   * Ensure {@code outputDir} is an empty directory ready to receive part files.
   * <ul>
   *   <li>If it does not exist, it is created.</li>
   *   <li>If it exists and is empty, it is used as-is.</li>
   *   <li>If it exists, is a directory and non-empty:
   *     <ul>
   *       <li>when {@code overwrite} is {@code true}: a warning is printed and the
   *           directory is deleted recursively, then recreated.</li>
   *       <li>when {@code overwrite} is {@code false}: an
   *           {@link IllegalArgumentException} is thrown so callers do not
   *           accidentally destroy existing data.</li>
   *     </ul>
   *   </li>
   *   <li>If it exists but is not a directory, an exception is thrown.</li>
   * </ul>
   */
  private static void prepareOutputDir(FileSystem outFs,
                                       Path outputDir,
                                       boolean overwrite,
                                       String modeLabel) throws IOException {
    if (outFs.exists(outputDir)) {
      if (!outFs.getFileStatus(outputDir).isDirectory()) {
        throw new IllegalArgumentException(
            "Output path already exists and is not a directory: " + outputDir);
      }
      FileStatus[] existing = outFs.listStatus(outputDir);
      if (existing.length > 0) {
        if (!overwrite) {
          throw new IllegalArgumentException(
              "Output directory is not empty for " + modeLabel + ": " + outputDir
                  + " (use --overwrite to delete existing contents)");
        }
        System.err.println("Overwriting existing non-empty output directory: " + outputDir
            + " (" + existing.length + " entries will be deleted)");
        if (!outFs.delete(outputDir, true)) {
          throw new IllegalStateException(
              "Failed to clean existing output directory: " + outputDir);
        }
        if (!outFs.mkdirs(outputDir)) {
          throw new IllegalStateException(
              "Failed to recreate output directory: " + outputDir);
        }
      }
    } else if (!outFs.mkdirs(outputDir)) {
      throw new IllegalStateException("Failed to create output directory: " + outputDir);
    }
  }

  /**
   * Recursively walk {@code dir}; append every directory that directly contains at
   * least one ORC file to {@code out}. A directory containing both ORC files and
   * subdirectories is treated as an error. Hidden entries (names starting with
   * {@code '_'} or {@code '.'}) are always skipped to match Hive/Spark conventions
   * around {@code _SUCCESS}, {@code _temporary}, etc.
   */
  private static void collectLeafDirs(FileSystem fs,
                                      Path dir,
                                      boolean ignoreExtension,
                                      List<Path> out) throws IOException {
    FileStatus[] children = fs.listStatus(dir);
    List<FileStatus> orcFiles = new ArrayList<>();
    List<FileStatus> subDirs = new ArrayList<>();
    for (FileStatus c : children) {
      String name = c.getPath().getName();
      if (isHidden(name)) {
        continue;
      }
      if (c.isDirectory()) {
        subDirs.add(c);
      } else if (isOrcCandidate(c, ignoreExtension)) {
        orcFiles.add(c);
      }
    }
    if (!orcFiles.isEmpty() && !subDirs.isEmpty()) {
      throw new IllegalArgumentException(
          "Directory contains both ORC files and subdirectories which is ambiguous"
              + " under --preserveStructure: " + dir);
    }
    if (!orcFiles.isEmpty()) {
      out.add(dir);
      return;
    }
    subDirs.sort(Comparator.comparing(FileStatus::getPath));
    for (FileStatus sub : subDirs) {
      collectLeafDirs(fs, sub.getPath(), ignoreExtension, out);
    }
  }

  /**
   * List direct ORC-file children of a leaf directory (non-recursive), skipping
   * hidden entries (names starting with {@code '_'} or {@code '.'}).
   */
  private static List<LocatedFileStatus> listLeafFiles(FileSystem fs,
                                                       Path leaf,
                                                       boolean ignoreExtension)
      throws IOException {
    List<LocatedFileStatus> out = new ArrayList<>();
    for (RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(leaf); it.hasNext(); ) {
      LocatedFileStatus s = it.next();
      String name = s.getPath().getName();
      if (isHidden(name)) {
        continue;
      }
      if (isOrcCandidate(s, ignoreExtension)) {
        out.add(s);
      }
    }
    out.sort(Comparator.comparing(FileStatus::getPath));
    return out;
  }

  private static boolean isHidden(String name) {
    return !name.isEmpty() && (name.charAt(0) == '_' || name.charAt(0) == '.');
  }

  private static boolean isOrcCandidate(FileStatus s, boolean ignoreExtension) {
    return s.isFile() && (ignoreExtension || s.getPath().getName().endsWith(".orc"));
  }

  /**
   * Return the portion of {@code child}'s path that lies beneath {@code root}. Both
   * arguments should already be qualified (same scheme/authority). Returns an empty
   * string when {@code child} equals {@code root}.
   */
  private static String relativize(Path root, Path child) {
    String rootStr = Path.getPathWithoutSchemeAndAuthority(root).toString();
    String childStr = Path.getPathWithoutSchemeAndAuthority(child).toString();
    if (childStr.equals(rootStr)) {
      return "";
    }
    String prefix = rootStr.endsWith("/") ? rootStr : rootStr + "/";
    if (!childStr.startsWith(prefix)) {
      throw new IllegalStateException(
          "Child path is not under root: child=" + child + ", root=" + root);
    }
    return childStr.substring(prefix.length());
  }

  private static final class DirMergeResult {
    final int partFileCount;
    final int mergedFileCount;
    final List<Path> unmergedFiles;

    DirMergeResult(int partFileCount, int mergedFileCount, List<Path> unmergedFiles) {
      this.partFileCount = partFileCount;
      this.mergedFileCount = mergedFileCount;
      this.unmergedFiles = unmergedFiles;
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
        .desc("Maximum cumulative input file size in bytes per output ORC file. Must be a "
            + "positive integer; a value of 0, a negative value, or a non-numeric value "
            + "causes the tool to exit with an error. When set, --output is treated as an "
            + "output directory and merged files are written as part-00000.orc, "
            + "part-00001.orc, etc. Input files are grouped at file boundaries so an "
            + "individual file larger than this threshold will still be placed in its own "
            + "part.")
        .hasArg()
        .build());

    result.addOption(Option.builder("p")
        .longOpt("preserveStructure")
        .desc("Mirror the input directory structure under --output: each directory that "
            + "directly contains ORC files (a 'leaf' directory, e.g. a Hive partition "
            + "such as d=2025-04-25/h=01) is merged independently and written to the "
            + "corresponding relative path under --output. Works with any nesting depth. "
            + "Requires exactly one input directory. Hidden files/directories (names "
            + "starting with '_' or '.', such as _SUCCESS or _temporary) are always "
            + "skipped to match Hive/Spark conventions. A directory that contains both "
            + "ORC files and subdirectories is rejected.")
        .build());

    result.addOption(Option.builder()
        .longOpt("overwrite")
        .desc("Applies to multi-file output modes (--maxSize and --preserveStructure). "
            + "If the output directory already exists and is non-empty, delete its "
            + "contents before writing merged part files. Without this flag, the tool "
            + "aborts with an error when the output directory is non-empty so that "
            + "existing data is not silently destroyed. Only a long form is provided "
            + "to avoid any risk of confusing --overwrite with --output (-o).")
        .build());

    result.addOption(Option.builder("h")
        .longOpt("help")
        .desc("Print help message")
        .build());
    return result;
  }
}
