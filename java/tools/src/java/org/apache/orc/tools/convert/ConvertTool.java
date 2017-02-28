/**
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
package org.apache.orc.tools.convert;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.tools.json.JsonSchemaFinder;

import java.io.IOException;

/**
 * A conversion tool to convert JSON files into ORC files.
 */
public class ConvertTool {

  static TypeDescription computeSchema(String[] filename) throws IOException {
    JsonSchemaFinder schemaFinder = new JsonSchemaFinder();
    for(String file: filename) {
      System.err.println("Scanning " + file + " for schema");
      schemaFinder.addFile(file);
    }
    return schemaFinder.getSchema();
  }

  public static void main(Configuration conf,
                          String[] args) throws IOException, ParseException {
    CommandLine opts = parseOptions(args);
    TypeDescription schema;
    if (opts.hasOption('s')) {
      schema = TypeDescription.fromString(opts.getOptionValue('s'));
    } else {
      schema = computeSchema(opts.getArgs());
    }
    String outFilename = opts.hasOption('o')
        ? opts.getOptionValue('o') : "output.orc";
    Writer writer = OrcFile.createWriter(new Path(outFilename),
        OrcFile.writerOptions(conf).setSchema(schema));
    VectorizedRowBatch batch = schema.createRowBatch();
    for (String file: opts.getArgs()) {
      System.err.println("Processing " + file);
      RecordReader reader = new JsonReader(new Path(file), schema, conf);
      while (reader.nextBatch(batch)) {
        writer.addRowBatch(batch);
      }
      reader.close();
    }
    writer.close();
  }

  static CommandLine parseOptions(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption(
        Option.builder("h").longOpt("help").desc("Provide help").build());
    options.addOption(
        Option.builder("s").longOpt("schema").hasArg()
            .desc("The schema to write in to the file").build());
    options.addOption(
        Option.builder("o").longOpt("output").desc("Output filename")
            .hasArg().build());
    CommandLine cli = new GnuParser().parse(options, args);
    if (cli.hasOption('h') || cli.getArgs().length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("convert", options);
      System.exit(1);
    }
    return cli;
  }
}
