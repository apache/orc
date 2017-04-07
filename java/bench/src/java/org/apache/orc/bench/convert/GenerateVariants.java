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

package org.apache.orc.bench.convert;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.CompressionKind;
import org.apache.orc.bench.SalesGenerator;
import org.apache.orc.bench.Utilities;
import org.apache.orc.bench.convert.avro.AvroReader;
import org.apache.orc.bench.convert.avro.AvroWriter;
import org.apache.orc.bench.convert.csv.CsvReader;
import org.apache.orc.bench.convert.json.JsonReader;
import org.apache.orc.bench.convert.json.JsonWriter;
import org.apache.orc.bench.convert.orc.OrcReader;
import org.apache.orc.bench.convert.orc.OrcWriter;
import org.apache.orc.bench.convert.parquet.ParquetReader;
import org.apache.orc.bench.convert.parquet.ParquetWriter;

import java.io.IOException;

/**
 * A tool to create the different variants that we need to benchmark against.
 */
public class GenerateVariants {

  public static BatchWriter createFileWriter(Path file,
                                             String format,
                                             TypeDescription schema,
                                             Configuration conf,
                                             CompressionKind compress
                                             ) throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    fs.delete(file, false);
    fs.mkdirs(file.getParent());
    switch (format) {
      case "json":
        return new JsonWriter(file, schema, conf, compress);
      case "orc":
        return new OrcWriter(file, schema, conf, compress);
      case "avro":
        return new AvroWriter(file, schema, conf, compress);
      case "parquet":
        return new ParquetWriter(file, schema, conf, compress);
      default:
        throw new IllegalArgumentException("Unknown format " + format);
    }
  }

  public static BatchReader createFileReader(Path file,
                                             String format,
                                             TypeDescription schema,
                                             Configuration conf,
                                             CompressionKind compress
                                             ) throws IOException {
    switch (format) {
      case "csv":
        return new CsvReader(file, schema, conf, compress);
      case "json":
        return new JsonReader(file, schema, conf, compress);
      case "orc":
        return new OrcReader(file, schema, conf);
      case "avro":
        return new AvroReader(file, schema, conf);
      case "parquet":
        return new ParquetReader(file, schema, conf);
      default:
        throw new IllegalArgumentException("Unknown format " + format);
    }
  }

  static class RecursiveReader implements BatchReader {
    private final RemoteIterator<LocatedFileStatus> filenames;
    private final String format;
    private final TypeDescription schema;
    private final Configuration conf;
    private final CompressionKind compress;
    private BatchReader current = null;

    RecursiveReader(Path root,
                    String format,
                    TypeDescription schema,
                    Configuration conf,
                    CompressionKind compress) throws IOException {
      FileSystem fs = root.getFileSystem(conf);
      filenames = fs.listFiles(root, true);
      this.format = format;
      this.schema = schema;
      this.conf = conf;
      this.compress = compress;
    }

    @Override
    public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
      while (current == null || !current.nextBatch(batch)) {
        if (filenames.hasNext()) {
          LocatedFileStatus next = filenames.next();
          if (next.isFile()) {
            current = createFileReader(next.getPath(), format, schema, conf,
                compress);
          }
        } else {
          return false;
        }
      }
      return true;
    }

    @Override
    public void close() throws IOException {
      if (current != null) {
        current.close();
      }
    }
  }

  public static BatchReader createReader(Path root,
                                         String dataName,
                                         TypeDescription schema,
                                         Configuration conf,
                                         long salesRecords) throws IOException {
    switch (dataName) {
      case "taxi":
        return new RecursiveReader(new Path(root, "sources/" + dataName), "csv",
            schema, conf, CompressionKind.ZLIB);
      case "sales":
        return new SalesGenerator(salesRecords);
      case "github":
        return new RecursiveReader(new Path(root, "sources/" + dataName), "json",
            schema, conf, CompressionKind.ZLIB);
      default:
        throw new IllegalArgumentException("Unknown data name " + dataName);
    }
  }

  static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = new Options()
        .addOption("h", "help", false, "Provide help")
        .addOption("c", "compress", true, "List of compression")
        .addOption("d", "data", true, "List of data sets")
        .addOption("f", "format", true, "List of formats")
        .addOption("s", "sales", true, "Number of records for sales");
    CommandLine result = new DefaultParser().parse(options, args);
    if (result.hasOption("help") || result.getArgs().length == 0) {
      new HelpFormatter().printHelp("convert <root>", options);
      System.exit(1);
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    CommandLine cli = parseCommandLine(args);
    String[] compressList =
        cli.getOptionValue("compress", "none,snappy,zlib").split(",");
    String[] dataList =
        cli.getOptionValue("data", "taxi,sales,github").split(",");
    String[] formatList =
        cli.getOptionValue("format", "avro,json,orc,parquet").split(",");
    long records = Long.parseLong(cli.getOptionValue("sales", "25000000"));
    Configuration conf = new Configuration();
    Path root = new Path(cli.getArgs()[0]);
    for(String data: dataList) {
      // Set up the reader
      TypeDescription schema = Utilities.loadSchema(data + ".schema");
      BatchReader reader = createReader(root, data, schema, conf, records);

      // Set up the writers for each combination
      BatchWriter[] writers = new BatchWriter[compressList.length * formatList.length];
      for(int compress=0; compress < compressList.length; ++compress) {
        CompressionKind compressionKind =
            CompressionKind.valueOf(compressList[compress].toUpperCase());
        for(int format=0; format < formatList.length; ++format) {
          Path outPath = Utilities.getVariant(root, data, formatList[format],
              compressList[compress]);
          writers[compress * formatList.length + format] =
              createFileWriter(outPath, formatList[format], schema, conf,
                  compressionKind);
        }
      }

      // Copy the rows
      VectorizedRowBatch batch = schema.createRowBatch();
      while (reader.nextBatch(batch)) {
        for(BatchWriter writer: writers) {
          writer.writeBatch(batch);
        }
      }
      reader.close();
      for(BatchWriter writer: writers) {
        writer.close();
      }
    }
  }
}
