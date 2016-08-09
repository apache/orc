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

TEST (TestFileMetadata, testRaw) {
  const std::string pgm = findProgram("tools/src/orc-metadata");
  const std::string file = findExample("orc_split_elim.orc");
  const std::string expected =
    "Raw file tail: " + file + "\n"
    "postscript {\n"
    "  footerLength: 288\n"
    "  compression: NONE\n"
    "  version: 0\n"
    "  version: 12\n"
    "  metadataLength: 526\n"
    "  magic: \"ORC\"\n"
    "}\n"
    "footer {\n"
    "  headerLength: 3\n"
    "  contentLength: 245568\n"
    "  stripes {\n"
    "    offset: 3\n"
    "    indexLength: 137\n"
    "    dataLength: 45282\n"
    "    footerLength: 149\n"
    "    numberOfRows: 5000\n"
    "  }\n"
    "  stripes {\n"
    "    offset: 45571\n"
    "    indexLength: 137\n"
    "    dataLength: 45282\n"
    "    footerLength: 149\n"
    "    numberOfRows: 5000\n"
    "  }\n"
    "  stripes {\n"
    "    offset: 91139\n"
    "    indexLength: 137\n"
    "    dataLength: 45282\n"
    "    footerLength: 149\n"
    "    numberOfRows: 5000\n"
    "  }\n"
    "  stripes {\n"
    "    offset: 136707\n"
    "    indexLength: 138\n"
    "    dataLength: 45283\n"
    "    footerLength: 149\n"
    "    numberOfRows: 5000\n"
    "  }\n"
    "  stripes {\n"
    "    offset: 200000\n"
    "    indexLength: 137\n"
    "    dataLength: 45282\n"
    "    footerLength: 149\n"
    "    numberOfRows: 5000\n"
    "  }\n"
    "  types {\n"
    "    kind: STRUCT\n"
    "    subtypes: 1\n"
    "    subtypes: 2\n"
    "    subtypes: 3\n"
    "    subtypes: 4\n"
    "    subtypes: 5\n"
    "    fieldNames: \"userid\"\n"
    "    fieldNames: \"string1\"\n"
    "    fieldNames: \"subtype\"\n"
    "    fieldNames: \"decimal1\"\n"
    "    fieldNames: \"ts\"\n"
    "  }\n"
    "  types {\n"
    "    kind: LONG\n"
    "  }\n"
    "  types {\n"
    "    kind: STRING\n"
    "  }\n"
    "  types {\n"
    "    kind: DOUBLE\n"
    "  }\n"
    "  types {\n"
    "    kind: DECIMAL\n"
    "  }\n"
    "  types {\n"
    "    kind: TIMESTAMP\n"
    "  }\n"
    "  numberOfRows: 25000\n"
    "  statistics {\n"
    "    numberOfValues: 25000\n"
    "  }\n"
    "  statistics {\n"
    "    numberOfValues: 25000\n"
    "    intStatistics {\n"
    "      minimum: 2\n"
    "      maximum: 100\n"
    "      sum: 2499619\n"
    "    }\n"
    "  }\n"
    "  statistics {\n"
    "    numberOfValues: 25000\n"
    "    stringStatistics {\n"
    "      minimum: \"bar\"\n"
    "      maximum: \"zebra\"\n"
    "      sum: 124990\n"
    "    }\n"
    "  }\n"
    "  statistics {\n"
    "    numberOfValues: 25000\n"
    "    doubleStatistics {\n"
    "      minimum: 0.8\n"
    "      maximum: 80\n"
    "      sum: 200051.40000000002\n"
    "    }\n"
    "  }\n"
    "  statistics {\n"
    "    numberOfValues: 25000\n"
    "    decimalStatistics {\n"
    "      minimum: \"0\"\n"
    "      maximum: \"5.5\"\n"
    "      sum: \"16.6\"\n"
    "    }\n"
    "  }\n"
    "  statistics {\n"
    "    numberOfValues: 25000\n"
    "  }\n"
    "  rowIndexStride: 10000\n"
    "}\n"
    "fileLength: 246402\n"
    "postscriptLength: 19\n";
  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, std::string("-r"), file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);

  EXPECT_EQ(0, runProgram({pgm, std::string("--raw"), file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

TEST (TestFileMetadata, testJson) {
  const std::string pgm = findProgram("tools/src/orc-metadata");
  const std::string file = findExample("orc_split_elim.orc");
  const std::string expected =
    "{ \"name\": \"" + file + "\",\n"
    "  \"type\": \"struct<userid:bigint,string1:string,subtype:double,decimal1:decimal(0,0),ts:timestamp>\",\n"
    "  \"rows\": 25000,\n"
    "  \"stripe count\": 5,\n"
    "  \"format\": \"0.12\", \"writer version\": \"original\",\n"
    "  \"compression\": \"none\",\n"
    "  \"file length\": 246402,\n"
    "  \"content\": 245568, \"stripe stats\": 526, \"footer\": 288, \"postscript\": 19,\n"
    "  \"row index stride\": 10000,\n"
    "  \"user metadata\": {\n"
    "  },\n"
    "  \"stripes\": [\n"
    "    { \"stripe\": 0, \"rows\": 5000,\n"
    "      \"offset\": 3, \"length\": 45568,\n"
    "      \"index\": 137, \"data\": 45282, \"footer\": 149\n"
    "    },\n"
    "    { \"stripe\": 1, \"rows\": 5000,\n"
    "      \"offset\": 45571, \"length\": 45568,\n"
    "      \"index\": 137, \"data\": 45282, \"footer\": 149\n"
    "    },\n"
    "    { \"stripe\": 2, \"rows\": 5000,\n"
    "      \"offset\": 91139, \"length\": 45568,\n"
    "      \"index\": 137, \"data\": 45282, \"footer\": 149\n"
    "    },\n"
    "    { \"stripe\": 3, \"rows\": 5000,\n"
    "      \"offset\": 136707, \"length\": 45570,\n"
    "      \"index\": 138, \"data\": 45283, \"footer\": 149\n"
    "    },\n"
    "    { \"stripe\": 4, \"rows\": 5000,\n"
    "      \"offset\": 200000, \"length\": 45568,\n"
    "      \"index\": 137, \"data\": 45282, \"footer\": 149\n"
    "    }\n"
    "  ]\n"
    "}\n";

  std::string output;
  std::string error;

  EXPECT_EQ(0, runProgram({pgm, file}, output, error));
  EXPECT_EQ(expected, output);
  EXPECT_EQ("", error);
}

