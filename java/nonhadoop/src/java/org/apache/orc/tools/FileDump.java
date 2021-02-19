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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.orc.impl.HadoopShimsFactory;
import org.apache.orc.impl.ReaderImplCore;
import org.apache.orc.shims.Configuration;
import org.apache.orc.shims.FileIO;
import org.apache.orc.shims.FileStatus;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.core.OrcFile;
import org.apache.orc.core.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.apache.orc.impl.OrcAcidUtilsCore;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.orc.impl.RecordReaderImpl;

/**
 * A tool for printing out the file structure of ORC files.
 */
public final class FileDump {
  public static final String UNKNOWN = "UNKNOWN";
  public static final String SEPARATOR = StringUtils.repeat("_", 120) + "\n";
  public static final int DEFAULT_BLOCK_SIZE = 256 * 1024 * 1024;
  public static final String DEFAULT_BACKUP_PATH = System.getProperty("java.io.tmpdir");

  // not used
  private FileDump() {
  }

  public static void main(Configuration conf, String[] args) throws Exception {
    List<Integer> rowIndexCols = new ArrayList<>(0);
    Options opts = createOptions();
    CommandLine cli = new GnuParser().parse(opts, args);

    if (cli.hasOption('h')) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("orcfiledump", opts);
      return;
    }

    boolean dumpData = cli.hasOption('d');
    String backupPath = DEFAULT_BACKUP_PATH;
    if (cli.hasOption("backup-path")) {
      backupPath = cli.getOptionValue("backup-path");
    }

    if (cli.hasOption("r")) {
      String val = cli.getOptionValue("r");
      if (val != null && val.trim().equals("*")) {
        rowIndexCols = null; // All the columns
      } else {
        String[] colStrs = cli.getOptionValue("r").split(",");
        rowIndexCols = new ArrayList<>(colStrs.length);
        for (String colStr : colStrs) {
          rowIndexCols.add(Integer.parseInt(colStr));
        }
      }
    }

    boolean printTimeZone = cli.hasOption('t');
    boolean jsonFormat = cli.hasOption('j');
    String[] files = cli.getArgs();
    if (files.length == 0) {
      System.err.println("Error : ORC files are not specified");
      return;
    }

    // if the specified path is directory, iterate through all files and print the file dump
    List<String> filesInPath = new ArrayList<>();
    for(String filename: files) {
      filesInPath.addAll(getAllFilesInPath(Paths.get(removeProtocol(filename))));
    }

    if (dumpData) {
      PrintData.main(conf, filesInPath.toArray(new String[filesInPath.size()]));
    } else {
      if (jsonFormat) {
        boolean prettyPrint = cli.hasOption('p');
        JsonFileDump.printJsonMetaData(filesInPath, conf, rowIndexCols, prettyPrint, printTimeZone);
      } else {
        printMetaData(filesInPath, conf, rowIndexCols, printTimeZone, false, backupPath);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HadoopShimsFactory.get().createConfiguration(null, null);
    main(conf, args);
  }

  /**
   * This method returns an ORC reader object if the specified file is readable. If the specified
   * file has side file (_flush_length) file, then max footer offset will be read from the side
   * file and orc reader will be created from that offset. Since both data file and side file
   * use hflush() for flushing the data, there could be some inconsistencies and both files could be
   * out-of-sync. Following are the cases under which null will be returned
   *
   * 1) If the file specified by path or its side file is still open for writes
   * 2) If *_flush_length file does not return any footer offset
   * 3) If *_flush_length returns a valid footer offset but the data file is not readable at that
   *    position (incomplete data file)
   * 4) If *_flush_length file length is not a multiple of 8, then reader will be created from
   *    previous valid footer. If there is no such footer (file length > 0 and < 8), then null will
   *    be returned
   *
   * Also, if this method detects any file corruption (mismatch between data file and side file)
   * then it will add the corresponding file to the specified input list for corrupted files.
   *
   * In all other cases, where the file is readable this method will return a reader object.
   *
   * @param path - file to get reader for
   * @param conf - configuration object
   * @param corruptFiles - fills this list with all possible corrupted files
   * @return - reader for the specified file or null
   * @throws IOException
   */
  static Reader getReader(String path, Configuration conf,
      List<String> corruptFiles) throws IOException {
    FileIO fs = HadoopShimsFactory.get().createFileIO(path, conf);
    FileStatus dataStatus = fs.getFileStatus(path);
    long dataFileLen = dataStatus.getLength();
    System.err.println("Processing data file " + path + " [length: " + dataFileLen + "]");
    String sideFile = OrcAcidUtilsCore.getSideFile(path);
    final boolean sideFileExists = fs.exists(sideFile);
    FileStatus sideStatus = sideFileExists ? fs.getFileStatus(sideFile) : null;

    Reader reader = null;
    if (sideFileExists) {
      final long maxLen = OrcAcidUtilsCore.getLastFlushLength(fs, path);
      final long sideFileLen = sideStatus.getLength();
      System.err.println("Found flush length file " + sideFile
          + " [length: " + sideFileLen + ", maxFooterOffset: " + maxLen + "]");
      // no offsets read from side file
      if (maxLen == -1) {

        // if data file is larger than last flush length, then additional data could be recovered
        if (dataFileLen > maxLen) {
          System.err.println("Data file has more data than max footer offset:" + maxLen +
              ". Adding data file to recovery list.");
          if (corruptFiles != null) {
            corruptFiles.add(path);
          }
        }
        return null;
      }

      try {
        reader = OrcFile.createReader(path,
            OrcFile.readerOptions(conf).maxLength(maxLen));

        // if data file is larger than last flush length, then additional data could be recovered
        if (dataFileLen > maxLen) {
          System.err.println("Data file has more data than max footer offset:" + maxLen +
              ". Adding data file to recovery list.");
          if (corruptFiles != null) {
            corruptFiles.add(path);
          }
        }
      } catch (Exception e) {
        if (corruptFiles != null) {
          corruptFiles.add(path);
        }
        System.err.println("Unable to read data from max footer offset." +
            " Adding data file to recovery list.");
        return null;
      }
    } else {
      reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    }

    return reader;
  }

  public static String removeProtocol(String url) {
    if (url.startsWith("file:")) {
      try {
        URL u = new URL(url);
        return u.getPath();
      } catch (MalformedURLException e) {
        // PASS
      }
    }
    return url;
  }

  public static Collection<String> getAllFilesInPath(Path path) throws IOException {
    List<String> filesInPath = new ArrayList<>();
    if (Files.isDirectory(path)) {
      Files.walkFileTree(path, new FileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          filesInPath.add(file.toString());
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
          System.err.println("Can't open " + file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          return FileVisitResult.CONTINUE;
        }
      });
    } else if (Files.isRegularFile(path)) {
      filesInPath.add(path.toString());
    }
    return filesInPath;
  }

  private static void printMetaData(List<String> files, Configuration conf,
      List<Integer> rowIndexCols, boolean printTimeZone, final boolean recover,
      final String backupPath)
      throws IOException {
    List<String> corruptFiles = new ArrayList<>();
    for (String filename : files) {
      printMetaDataImpl(filename, conf, rowIndexCols, printTimeZone, corruptFiles);
      System.out.println(SEPARATOR);
    }

    if (!corruptFiles.isEmpty()) {
      System.err.println(corruptFiles.size() + " file(s) are corrupted." +
          " Run the following command to recover corrupted files.\n");
      StringBuilder buffer = new StringBuilder();
      buffer.append("hive --orcfiledump --recover --skip-dump");
      for(String file: corruptFiles) {
        buffer.append(' ');
        buffer.append(file);
      }
      System.err.println(buffer.toString());
      System.out.println(SEPARATOR);
    }
  }

  static void printTypeAnnotations(TypeDescription type, String prefix) {
    List<String> attributes = type.getAttributeNames();
    if (attributes.size() > 0) {
      System.out.println("Attributes on " + prefix);
      for(String attr: attributes) {
        System.out.println("  " + attr + ": " + type.getAttributeValue(attr));
      }
    }
    List<TypeDescription> children = type.getChildren();
    if (children != null) {
      switch (type.getCategory()) {
        case STRUCT:
          List<String> fields = type.getFieldNames();
          for(int c = 0; c < children.size(); ++c) {
            printTypeAnnotations(children.get(c), prefix + "." + fields.get(c));
          }
          break;
        case MAP:
          printTypeAnnotations(children.get(0), prefix + "._key");
          printTypeAnnotations(children.get(1), prefix + "._value");
          break;
        case LIST:
          printTypeAnnotations(children.get(0), prefix + "._elem");
          break;
        case UNION:
          for(int c = 0; c < children.size(); ++c) {
            printTypeAnnotations(children.get(c), prefix + "._" + c);
          }
          break;
      }
    }
  }

  private static void printMetaDataImpl(String filename,
      final Configuration conf, List<Integer> rowIndexCols, final boolean printTimeZone,
      final List<String> corruptFiles) throws IOException {
    Reader reader = getReader(filename, conf, corruptFiles);
    // if we can create reader then footer is not corrupt and file will readable
    if (reader == null) {
      return;
    }
    TypeDescription schema = reader.getSchema();
    Path path = Paths.get(filename);
    System.out.println("Structure for " + path.getFileName());
    System.out.println("File Version: " + reader.getFileVersion().getName() +
        " with " + reader.getWriterVersion());
    RecordReaderImpl rows = (RecordReaderImpl) reader.rows();
    System.out.println("Rows: " + reader.getNumberOfRows());
    System.out.println("Compression: " + reader.getCompressionKind());
    if (reader.getCompressionKind() != CompressionKind.NONE) {
      System.out.println("Compression size: " + reader.getCompressionSize());
    }
    System.out.println("Calendar: " + (reader.writerUsedProlepticGregorian()
                           ? "Proleptic Gregorian"
                           : "Julian/Gregorian"));
    System.out.println("Type: " + reader.getSchema().toString());
    printTypeAnnotations(reader.getSchema(), "root");
    System.out.println("\nStripe Statistics:");
    List<StripeStatistics> stripeStats = reader.getStripeStatistics();
    for (int n = 0; n < stripeStats.size(); n++) {
      System.out.println("  Stripe " + (n + 1) + ":");
      StripeStatistics ss = stripeStats.get(n);
      for (int i = 0; i < ss.getColumnStatistics().length; ++i) {
        System.out.println("    Column " + i + ": " +
            ss.getColumnStatistics()[i].toString());
      }
    }
    ColumnStatistics[] stats = reader.getStatistics();
    int colCount = stats.length;
    if (rowIndexCols == null) {
      rowIndexCols = new ArrayList<>(colCount);
      for (int i = 0; i < colCount; ++i) {
        rowIndexCols.add(i);
      }
    }
    System.out.println("\nFile Statistics:");
    for (int i = 0; i < stats.length; ++i) {
      System.out.println("  Column " + i + ": " + stats[i].toString());
    }
    System.out.println("\nStripes:");
    int stripeIx = -1;
    for (StripeInformation stripe : reader.getStripes()) {
      ++stripeIx;
      long stripeStart = stripe.getOffset();
      OrcProto.StripeFooter footer = rows.readStripeFooter(stripe);
      if (printTimeZone) {
        String tz = footer.getWriterTimezone();
        if (tz == null || tz.isEmpty()) {
          tz = UNKNOWN;
        }
        System.out.println("  Stripe: " + stripe.toString() + " timezone: " + tz);
      } else {
        System.out.println("  Stripe: " + stripe.toString());
      }
      long sectionStart = stripeStart;
      for (OrcProto.Stream section : footer.getStreamsList()) {
        String kind = section.hasKind() ? section.getKind().name() : UNKNOWN;
        System.out.println("    Stream: column " + section.getColumn() +
            " section " + kind + " start: " + sectionStart +
            " length " + section.getLength());
        sectionStart += section.getLength();
      }
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        OrcProto.ColumnEncoding encoding = footer.getColumns(i);
        StringBuilder buf = new StringBuilder();
        buf.append("    Encoding column ");
        buf.append(i);
        buf.append(": ");
        buf.append(encoding.getKind());
        if (encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY ||
            encoding.getKind() == OrcProto.ColumnEncoding.Kind.DICTIONARY_V2) {
          buf.append("[");
          buf.append(encoding.getDictionarySize());
          buf.append("]");
        }
        System.out.println(buf);
      }
      if (rowIndexCols != null && !rowIndexCols.isEmpty()) {
        // include the columns that are specified, only if the columns are included, bloom filter
        // will be read
        boolean[] sargColumns = new boolean[colCount];
        for (int colIdx : rowIndexCols) {
          sargColumns[colIdx] = true;
        }
        OrcIndex indices = rows.readRowIndex(stripeIx, null, sargColumns);
        for (int col : rowIndexCols) {
          StringBuilder buf = new StringBuilder();
          String rowIdxString = getFormattedRowIndices(col,
              indices.getRowGroupIndex(), schema, (ReaderImplCore) reader);
          buf.append(rowIdxString);
          String bloomFilString = getFormattedBloomFilters(col, indices,
              reader.getWriterVersion(),
              reader.getSchema().findSubtype(col).getCategory(),
              footer.getColumns(col));
          buf.append(bloomFilString);
          System.out.println(buf);
        }
      }
    }

    FileIO fs = HadoopShimsFactory.get().createFileIO(filename, conf);
    long fileLen = fs.getFileStatus(filename).getLength();
    long paddedBytes = getTotalPaddingSize(reader);
    // empty ORC file is ~45 bytes. Assumption here is file length always >0
    double percentPadding = ((double) paddedBytes / (double) fileLen) * 100;
    DecimalFormat format = new DecimalFormat("##.##");
    System.out.println("\nFile length: " + fileLen + " bytes");
    System.out.println("Padding length: " + paddedBytes + " bytes");
    System.out.println("Padding ratio: " + format.format(percentPadding) + "%");
    //print out any user metadata properties
    List<String> keys = reader.getMetadataKeys();
    for(int i = 0; i < keys.size(); i++) {
      if(i == 0) {
        System.out.println("\nUser Metadata:");
      }
      ByteBuffer byteBuffer = reader.getMetadataValue(keys.get(i));
      System.out.println("  " + keys.get(i) + "="
        + StandardCharsets.UTF_8.decode(byteBuffer));
    }
    rows.close();
  }

  private static boolean isReadable(String corruptPath, Configuration conf,
      final long maxLen) {
    try {
      OrcFile.createReader(corruptPath,
          OrcFile.readerOptions(conf).maxLength(maxLen));
      return true;
    } catch (Exception e) {
      // ignore this exception as maxLen is unreadable
      return false;
    }
  }

  // search for byte pattern in another byte array
  private static int indexOf(final byte[] data, final byte[] pattern, final int index) {
    if (data == null || data.length == 0 || pattern == null || pattern.length == 0 ||
        index > data.length || index < 0) {
      return -1;
    }

    int j = 0;
    for (int i = index; i < data.length; i++) {
      if (pattern[j] == data[i]) {
        j++;
      } else {
        j = 0;
      }

      if (j == pattern.length) {
        return i - pattern.length + 1;
      }
    }

    return -1;
  }

  private static String getFormattedBloomFilters(int col, OrcIndex index,
                                                 OrcFile.WriterVersion version,
                                                 TypeDescription.Category type,
                                                 OrcProto.ColumnEncoding encoding) {
    OrcProto.BloomFilterIndex[] bloomFilterIndex = index.getBloomFilterIndex();
    StringBuilder buf = new StringBuilder();
    BloomFilter stripeLevelBF = null;
    if (bloomFilterIndex != null && bloomFilterIndex[col] != null) {
      int idx = 0;
      buf.append("\n    Bloom filters for column ").append(col).append(":");
      for (OrcProto.BloomFilter bf : bloomFilterIndex[col].getBloomFilterList()) {
        BloomFilter toMerge = BloomFilterIO.deserialize(
            index.getBloomFilterKinds()[col], encoding, version, type, bf);
        buf.append("\n      Entry ").append(idx++).append(":").append(getBloomFilterStats(toMerge));
        if (stripeLevelBF == null) {
          stripeLevelBF = toMerge;
        } else {
          stripeLevelBF.merge(toMerge);
        }
      }
      String bloomFilterStats = getBloomFilterStats(stripeLevelBF);
      buf.append("\n      Stripe level merge:").append(bloomFilterStats);
    }
    return buf.toString();
  }

  private static String getBloomFilterStats(BloomFilter bf) {
    StringBuilder sb = new StringBuilder();
    int bitCount = bf.getBitSize();
    int popCount = 0;
    for (long l : bf.getBitSet()) {
      popCount += Long.bitCount(l);
    }
    int k = bf.getNumHashFunctions();
    float loadFactor = (float) popCount / (float) bitCount;
    float expectedFpp = (float) Math.pow(loadFactor, k);
    DecimalFormat df = new DecimalFormat("###.####");
    sb.append(" numHashFunctions: ").append(k);
    sb.append(" bitCount: ").append(bitCount);
    sb.append(" popCount: ").append(popCount);
    sb.append(" loadFactor: ").append(df.format(loadFactor));
    sb.append(" expectedFpp: ").append(expectedFpp);
    return sb.toString();
  }

  private static String getFormattedRowIndices(int col,
                                               OrcProto.RowIndex[] rowGroupIndex,
                                               TypeDescription schema,
                                               ReaderImplCore reader) {
    StringBuilder buf = new StringBuilder();
    OrcProto.RowIndex index;
    buf.append("    Row group indices for column ").append(col).append(":");
    if (rowGroupIndex == null || (col >= rowGroupIndex.length) ||
        ((index = rowGroupIndex[col]) == null)) {
      buf.append(" not found\n");
      return buf.toString();
    }

    TypeDescription colSchema = schema.findSubtype(col);
    for (int entryIx = 0; entryIx < index.getEntryCount(); ++entryIx) {
      buf.append("\n      Entry ").append(entryIx).append(": ");
      OrcProto.RowIndexEntry entry = index.getEntry(entryIx);
      if (entry == null) {
        buf.append("unknown\n");
        continue;
      }
      OrcProto.ColumnStatistics colStats = entry.getStatistics();
      if (colStats == null) {
        buf.append("no stats at ");
      } else {
        ColumnStatistics cs =
            ColumnStatisticsImpl.deserialize(colSchema, colStats,
                reader.writerUsedProlepticGregorian(),
                reader.getConvertToProlepticGregorian());
        buf.append(cs.toString());
      }
      buf.append(" positions: ");
      for (int posIx = 0; posIx < entry.getPositionsCount(); ++posIx) {
        if (posIx != 0) {
          buf.append(",");
        }
        buf.append(entry.getPositions(posIx));
      }
    }
    return buf.toString();
  }

  public static long getTotalPaddingSize(Reader reader) throws IOException {
    long paddedBytes = 0;
    List<StripeInformation> stripes = reader.getStripes();
    for (int i = 1; i < stripes.size(); i++) {
      long prevStripeOffset = stripes.get(i - 1).getOffset();
      long prevStripeLen = stripes.get(i - 1).getLength();
      paddedBytes += stripes.get(i).getOffset() - (prevStripeOffset + prevStripeLen);
    }
    return paddedBytes;
  }

  @SuppressWarnings("static-access")
  static Options createOptions() {
    Options result = new Options();

    // add -d and --data to print the rows
    result.addOption(OptionBuilder
        .withLongOpt("data")
        .withDescription("Should the data be printed")
        .create('d'));

    // to avoid breaking unit tests (when run in different time zones) for file dump, printing
    // of timezone is made optional
    result.addOption(OptionBuilder
        .withLongOpt("timezone")
        .withDescription("Print writer's time zone")
        .create('t'));

    result.addOption(OptionBuilder
        .withLongOpt("help")
        .withDescription("print help message")
        .create('h'));

    result.addOption(OptionBuilder
        .withLongOpt("rowindex")
        .withArgName("comma separated list of column ids for which row index should be printed")
        .withDescription("Dump stats for column number(s)")
        .hasArg()
        .create('r'));

    result.addOption(OptionBuilder
        .withLongOpt("json")
        .withDescription("Print metadata in JSON format")
        .create('j'));

    result.addOption(OptionBuilder
        .withLongOpt("pretty")
        .withDescription("Pretty print json metadata output")
        .create('p'));

    result.addOption(OptionBuilder
        .withLongOpt("recover")
        .withDescription("recover corrupted orc files generated by streaming")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("skip-dump")
        .withDescription("used along with --recover to directly recover files without dumping")
        .create());

    result.addOption(OptionBuilder
        .withLongOpt("backup-path")
        .withDescription("specify a backup path to store the corrupted files (default: /tmp)")
        .hasArg()
        .create());
    return result;
  }

}
