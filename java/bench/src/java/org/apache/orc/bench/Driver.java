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

package org.apache.orc.bench;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.bench.convert.GenerateVariants;
import org.apache.orc.bench.convert.ScanVariants;

import java.util.Arrays;

/**
 * A driver tool to call the various benchmark classes.
 */
public class Driver {

  static CommandLine parseCommandLine(String[] args) throws ParseException {
    Options options = new Options()
        .addOption("h", "help", false, "Provide help")
        .addOption("D", "define", true, "Change configuration settings");
    CommandLine result = new DefaultParser().parse(options, args, true);
    if (result.hasOption("help") || result.getArgs().length == 0) {
      new HelpFormatter().printHelp("benchmark <command>", options);
      System.err.println();
      System.err.println("Commands:");
      System.err.println("  generate  - Generate data variants");
      System.err.println("  scan      - Scan data variants");
      System.err.println("  read-all  - Full table scan benchmark");
      System.err.println("  read-some - Column projection benchmark");
      System.exit(1);
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    CommandLine cli = parseCommandLine(args);
    args = cli.getArgs();
    String command = args[0];
    args = Arrays.copyOfRange(args, 1, args.length);
    switch (command) {
      case "generate":
        GenerateVariants.main(args);
        break;
      case "scan":
        ScanVariants.main(args);
        break;
      case "read-all":
        FullReadBenchmark.main(args);
        break;
      case "read-some":
        ColumnProjectionBenchmark.main(args);
        break;
      default:
        System.err.println("Unknown command " + command);
        System.exit(1);
    }
  }
}
