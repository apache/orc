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
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONWriter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Print the contents of an ORC file as JSON.
 */
public class PrintHeadData extends PrintData
{
  static void printHeadJsonData(PrintStream printStream,
          Reader reader, Integer numberOfRows) throws IOException, JSONException {
    OutputStreamWriter out = new OutputStreamWriter(printStream, StandardCharsets.UTF_8);
    RecordReader rows = reader.rows();
    try {
      TypeDescription schema = reader.getSchema();
      VectorizedRowBatch batch = schema.createRowBatch();
      Integer counter = 0;
      while (rows.nextBatch(batch)) {
        for (int r=0; r < batch.size; ++r) {
          JSONWriter writer = new JSONWriter(out);
          printRow(writer, batch, schema, r);
          out.write("\n");
          out.flush();
          if (printStream.checkError()) {
            throw new IOException("Error encountered when writing to stdout.");
          }

          counter++;
          if (counter >= numberOfRows){
            break;
          }
        }
      }
    } finally {
      rows.close();
    }
  }

  private static Options getOptions() {
    Option help = new Option("h", "help", false, "Provide help");
    Option linesOpt = Option.builder("n").longOpt("lines")
            .argName("LINES")
            .hasArg()
            .build();

    Options options = new Options()
            .addOption(help)
            .addOption(linesOpt);
    return options;
  }

  private static void printHelp(){
    Options opts = getOptions();

    PrintWriter pw = new PrintWriter(System.err);
    new HelpFormatter().printHelp(pw, HelpFormatter.DEFAULT_WIDTH,
            "java -jar orc-tools-*.jar head <orc file>*",
            null,
            opts,
            HelpFormatter.DEFAULT_LEFT_PAD,
            HelpFormatter.DEFAULT_DESC_PAD, null);
    pw.flush();
  }

  static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = getOptions();
    return new DefaultParser().parse(options, args);
  }

  static void main(Configuration conf, String[] args
                   ) throws IOException, JSONException, ParseException {
    CommandLine cli = parseCommandLine(args);
    if (cli.hasOption('h') || cli.getArgs().length == 0) {
      printHelp();
      System.exit(1);
    } else {
      int numberOfRows = Integer.parseInt(cli.getOptionValue("n", "10"));

      List<String> badFiles = new ArrayList<>();
      for (String file : cli.getArgs()) {
        try {
          Path path = new Path(file);
          Reader reader = FileDump.getReader(path, conf, badFiles);
          if (reader == null) {
            continue;
          }
          printHeadJsonData(System.out, reader, numberOfRows);
          System.out.println(FileDump.SEPARATOR);
        } catch (Exception e) {
          System.err.println("Unable to dump data for file: " + file);
          e.printStackTrace();
          continue;
        }
      }
    }
  }
}
