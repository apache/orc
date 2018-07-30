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

#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "ToolTest.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

TEST (TestCSVFileImport, test10rows) {
  // create an ORC file from importing the CSV file
  const std::string pgm1 = findProgram("tools/src/csv-import");
  const std::string csvFile = findExample("TestCSVFileImport.test10rows.csv");
  const std::string orcFile = "/tmp/test_csv_import_test_10_rows.orc";
  const std::string schema = "struct<a:bigint,b:string,c:double>";
  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm1, schema, csvFile, orcFile}, output, error));
  EXPECT_EQ("", error);

  // verify the ORC file content
  const std::string pgm2 = findProgram("tools/src/orc-contents");
  const std::string expected =
    "{\"a\": 0, \"b\": \"a\", \"c\": 0}\n"
    "{\"a\": 1, \"b\": \"b\", \"c\": 1.1}\n"
    "{\"a\": 2, \"b\": \"c\", \"c\": 2.2}\n"
    "{\"a\": 3, \"b\": \"d\", \"c\": null}\n"
    "{\"a\": 4, \"b\": null, \"c\": 4.4}\n"
    "{\"a\": null, \"b\": \"f\", \"c\": 5.5}\n"
    "{\"a\": null, \"b\": null, \"c\": null}\n"
    "{\"a\": 7, \"b\": \"h\", \"c\": 7.7}\n"
    "{\"a\": 8, \"b\": \"i\", \"c\": 8.8}\n"
    "{\"a\": 9, \"b\": \"j\", \"c\": 9.9}\n";
  EXPECT_EQ(0, runProgram({pgm2, orcFile}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST (TestCSVFileImport, test10rows_underscore) {
  // create an ORC file from importing the CSV file
  const std::string pgm1 = findProgram("tools/src/csv-import");
  const std::string csvFile = findExample("TestCSVFileImport.test10rows.csv");
  const std::string orcFile = "/tmp/test_csv_import_test_10_rows.orc";
  const std::string schema = "struct<_a:bigint,b_:string,c:double>";
  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm1, schema, csvFile, orcFile}, output, error));
  EXPECT_EQ("", error);

  // verify the ORC file content
  const std::string pgm2 = findProgram("tools/src/orc-contents");
  const std::string expected =
    "{\"_a\": 0, \"b_\": \"a\", \"c\": 0}\n"
    "{\"_a\": 1, \"b_\": \"b\", \"c\": 1.1}\n"
    "{\"_a\": 2, \"b_\": \"c\", \"c\": 2.2}\n"
    "{\"_a\": 3, \"b_\": \"d\", \"c\": null}\n"
    "{\"_a\": 4, \"b_\": null, \"c\": 4.4}\n"
    "{\"_a\": null, \"b_\": \"f\", \"c\": 5.5}\n"
    "{\"_a\": null, \"b_\": null, \"c\": null}\n"
    "{\"_a\": 7, \"b_\": \"h\", \"c\": 7.7}\n"
    "{\"_a\": 8, \"b_\": \"i\", \"c\": 8.8}\n"
    "{\"_a\": 9, \"b_\": \"j\", \"c\": 9.9}\n";
  EXPECT_EQ(0, runProgram({pgm2, orcFile}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}
