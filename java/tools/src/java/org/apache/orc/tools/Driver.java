/**
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
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.tools.convert.ConvertTool;
import org.apache.orc.tools.json.JsonSchemaFinder;

import java.util.Map;
import java.util.Properties;

/**
 * Driver program for the java ORC utilities.
 */
public class Driver {

  @SuppressWarnings("static-access")
  static Options createOptions() {
    Options result = new Options();

    result.addOption(OptionBuilder
         .withLongOpt("help")
         .withDescription("Print help message")
         .create('h'));

    result.addOption(OptionBuilder
        .withLongOpt("define")
        .withDescription("Set a configuration property")
        .hasArg()
        .withValueSeparator()
        .create('D'));
    return result;
  }

  static class DriverOptions {
    final CommandLine genericOptions;
    final String command;
    final String[] commandArgs;

    DriverOptions(String[] args) throws ParseException {
      genericOptions = new GnuParser().parse(createOptions(), args, true);
      String[] unprocessed = genericOptions.getArgs();
      if (unprocessed.length == 0) {
        command = null;
        commandArgs = new String[0];
      } else {
        command = unprocessed[0];
        if (genericOptions.hasOption('h')) {
          commandArgs = new String[]{"-h"};
        } else {
          commandArgs = new String[unprocessed.length - 1];
          System.arraycopy(unprocessed, 1, commandArgs, 0, commandArgs.length);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    DriverOptions options = new DriverOptions(args);

    if (options.command == null) {
      System.err.println("ORC Java Tools");
      System.err.println();
      System.err.println("usage: java -jar orc-tools-*.jar [--help]" +
          " [--define X=Y] <command> <args>");
      System.err.println();
      System.err.println("Commands:");
      System.err.println("   version - print the version of this ORC tool");
      System.err.println("   meta - print the metadata about the ORC file");
      System.err.println("   data - print the data from the ORC file");
      System.err.println("   scan - scan the ORC file");
      System.err.println("   convert - convert CSV and JSON files to ORC");
      System.err.println("   json-schema - scan JSON files to determine their schema");
      System.err.println("   key - print information about the keys");
      System.err.println();
      System.err.println("To get more help, provide -h to the command");
      System.exit(1);
    }
    Configuration conf = new Configuration();
    Properties confSettings = options.genericOptions.getOptionProperties("D");
    for(Map.Entry pair: confSettings.entrySet()) {
      conf.set(pair.getKey().toString(), pair.getValue().toString());
    }
    if ("version".equals(options.command)) {
      PrintVersion.main(conf, options.commandArgs);
    } else if ("meta".equals(options.command)) {
      FileDump.main(conf, options.commandArgs);
    } else if ("data".equals(options.command)) {
      PrintData.main(conf, options.commandArgs);
    } else if ("scan".equals(options.command)) {
      ScanData.main(conf, options.commandArgs);
    } else if ("json-schema".equals(options.command)) {
      JsonSchemaFinder.main(conf, options.commandArgs);
    } else if ("convert".equals(options.command)) {
      ConvertTool.main(conf, options.commandArgs);
    } else if ("key".equals(options.command)) {
      KeyTool.main(conf, options.commandArgs);
    } else {
      System.err.println("Unknown subcommand: " + options.command);
      System.exit(1);
    }
  }
}
