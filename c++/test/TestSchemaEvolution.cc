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

#include "MockStripeStreams.hh"
#include "SchemaEvolution.hh"
#include "TypeImpl.hh"

#include "wrap/gtest-wrapper.h"

namespace orc {

  bool testConvertReader(const std::string file, const std::string& read, bool can, bool need) {
    auto fileType = std::shared_ptr<Type>(Type::buildTypeFromString(file).release());
    auto readType = std::shared_ptr<Type>(Type::buildTypeFromString(read).release());

    if (!can) {
      EXPECT_THROW(SchemaEvolution(readType, fileType.get()), SchemaEvolutionError)
          << "fileType: " << fileType->toString() << "\nreadType: " << readType->toString();
    } else {
      // if can convert, check that there are no excepted be thrown and
      // we can create reader successfully
      SchemaEvolution se(readType, fileType.get());
      EXPECT_FALSE(se.needConvert(*fileType));
      EXPECT_EQ(need, se.needConvert(*fileType->getSubtype(0)))
          << "fileType: " << fileType->toString() << "\nreadType: " << readType->toString();
      MockStripeStreams streams;
      std::vector<bool> selectedColumns(2);
      EXPECT_CALL(streams, getSelectedColumns()).WillRepeatedly(testing::Return(selectedColumns));
      proto::ColumnEncoding directEncoding;
      directEncoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
      EXPECT_CALL(streams, getEncoding(testing::_)).WillRepeatedly(testing::Return(directEncoding));

      std::string dummyStream("dummy");
      EXPECT_CALL(streams, getStreamProxy(testing::_, testing::_, testing::_))
          .WillRepeatedly(testing::ReturnNew<SeekableArrayInputStream>(dummyStream.c_str(),
                                                                       dummyStream.length()));

      EXPECT_CALL(streams, isDecimalAsLong()).WillRepeatedly(testing::Return(false));
      EXPECT_CALL(streams, getSchemaEvolution()).WillRepeatedly(testing::Return(&se));
      EXPECT_CALL(streams, getSelectedColumns())
          .WillRepeatedly(testing::Return(std::vector<bool>{true, true}));

      EXPECT_TRUE(buildReader(*fileType, streams, true) != nullptr);
    }
    return true;
  }

  TEST(SchemaEvolution, createConvertReader) {
    std::map<size_t, std::string> types = {
        {0, "struct<t1:boolean>"},        {1, "struct<t1:tinyint>"},
        {2, "struct<t1:smallint>"},       {3, "struct<t1:int>"},
        {4, "struct<t1:bigint>"},         {5, "struct<t1:float>"},
        {6, "struct<t1:double>"},         {7, "struct<t1:string>"},
        {8, "struct<t1:char(6)>"},        {9, "struct<t1:varchar(6)>"},
        {10, "struct<t1:char(5)>"},       {11, "struct<t1:varchar(5)>"},
        {12, "struct<t1:decimal(25,2)>"}, {13, "struct<t1:decimal(15,2)>"},
        {14, "struct<t1:timestamp>"},     {15, "struct<t1:timestamp with local time zone>"},
        {16, "struct<t1:date>"}};

    size_t typesSize = types.size();
    std::vector<std::vector<bool>> needConvert(typesSize, std::vector<bool>(typesSize, 0));
    std::vector<std::vector<bool>> canConvert(typesSize, std::vector<bool>(typesSize, 0));

    // all types can convert to itselfs
    for (size_t i = 0; i < types.size(); i++) {
      canConvert[i][i] = true;
    }

    // conversion from numeric to numeric
    for (size_t i = 0; i <= 6; i++) {
      for (size_t j = 0; j <= 6; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = (i != j);
      }
    }

    // conversion from numeric to string/char/varchar
    for (size_t i = 0; i <= 6; i++) {
      for (size_t j = 7; j <= 11; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = true;
      }
    }

    // conversion from numeric to decimal
    for (size_t i = 0; i <= 6; i++) {
      for (size_t j = 12; j <= 13; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = true;
      }
    }

    // conversion from numeric to timestamp
    for (size_t i = 0; i <= 6; i++) {
      for (size_t j = 14; j <= 15; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = true;
      }
    }

    // conversion from decimal to numeric
    for (size_t i = 12; i <= 13; i++) {
      for (size_t j = 0; j <= 6; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = true;
      }
    }

    // conversion from decimal to decimal
    for (size_t i = 12; i <= 13; i++) {
      for (size_t j = 12; j <= 13; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = false;
        if (i != j) {
          needConvert[i][j] = true;
        }
      }
    }

    // conversion from decimal to string/char/varchar
    for (size_t i = 12; i <= 13; i++) {
      for (size_t j = 7; j <= 11; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = true;
      }
    }

    // conversion from decimal to timestamp
    for (size_t i = 12; i <= 13; i++) {
      for (size_t j = 14; j <= 15; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = true;
      }
    }

    // conversion from string variant to numeric
    for (size_t i = 7; i <= 11; i++) {
      for (size_t j = 0; j <= 6; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = true;
      }
    }

    // conversion from string variant to string variant
    for (size_t i = 7; i <= 11; i++) {
      for (size_t j = 7; j <= 11; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = (i != j);
      }
    }

    // conversion from string variant to decimal
    for (size_t i = 7; i <= 11; i++) {
      for (size_t j = 12; j <= 13; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = (i != j);
      }
    }

    // conversion from string variant to timestamp
    for (size_t i = 7; i <= 11; i++) {
      for (size_t j = 14; j <= 15; j++) {
        canConvert[i][j] = true;
        needConvert[i][j] = (i != j);
      }
    }

    for (size_t i = 0; i < typesSize; i++) {
      for (size_t j = 0; j < typesSize; j++) {
        testConvertReader(types[i], types[j], canConvert[i][j], needConvert[i][j]);
      }
    }
  }

}  // namespace orc
