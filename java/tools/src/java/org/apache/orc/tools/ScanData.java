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
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONWriter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Scan the contents of an ORC file.
 */
public class ScanData {


  static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = new Options()
        .addOption("help", "h", false, "Provide help");
    return new GnuParser().parse(options, args);
  }


  static void main(Configuration conf, String[] args
                   ) throws IOException, JSONException, ParseException {
    CommandLine cli = parseCommandLine(args);
    if (cli.hasOption('h') || cli.getArgs().length == 0) {
      System.err.println("usage: java -jar orc-tools-*.jar scan [--help] <orc file>*");
      System.exit(1);
    } else {
      List<String> badFiles = new ArrayList<>();
      for (String file : cli.getArgs()) {
        try {
          Path path = new Path(file);
          Reader reader = FileDump.getReader(path, conf, badFiles);
          if (reader == null) {
            continue;
          }
          RecordReader rows = reader.rows();
          VectorizedRowBatch batch = reader.getSchema().createRowBatch();
          long batchCount = 0;
          long rowCount = 0;
          while (rows.nextBatch(batch)) {
            batchCount += 1;
            rowCount += batch.size;
          }
          System.out.println("File " + path + ": " + batchCount +
              " batches, and " + rowCount + " rows.");
        } catch (Exception e) {
          System.err.println("Unable to dump data for file: " + file);
          continue;
        }
      }
    }
  }
}
