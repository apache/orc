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
#include "orc/Reader.hh"

#include "Adaptor.hh"

#include "wrap/gmock.h"
#include "wrap/gtest-wrapper.h"

namespace orc {

  TEST(TestCorruptFiles, testNegativeDictEntryLengths) {

    std::stringstream ss;
    if(const char* example_dir = std::getenv("ORC_EXAMPLE_DIR")) {
      ss << example_dir;
    } else {
      ss << "../../../examples";
    }
    ss << "/corrupt/negative_dict_entry_lengths.orc";
    try {
      std::unique_ptr<orc::Reader> reader =
        createReader(readLocalFile(ss.str().c_str()), ReaderOptions());

      RowReaderOptions row_reader_opts;
      row_reader_opts.include({"date_string_col"});
      std::unique_ptr<orc::RowReader> row_reader =
          reader->createRowReader(row_reader_opts);
      std::unique_ptr<ColumnVectorBatch> column_vector_batch =
        row_reader->createRowBatch(1024);
      row_reader->next(*column_vector_batch);
    }
    catch (ParseError& err) {
      std::string msg(err.what());
      EXPECT_TRUE(msg.find("Negative dictionary entry length") != std::string::npos);
    }
  }

}  // namespace
