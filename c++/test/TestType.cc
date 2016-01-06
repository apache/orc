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

#include "Adaptor.hh"
#include "OrcTest.hh"
#include "orc/Type.hh"
#include "wrap/gtest-wrapper.h"

#include "TypeImpl.hh"

namespace orc {

  uint64_t checkIds(const Type* type, uint64_t next) {
    EXPECT_EQ(next, type->getColumnId())
      << "Wrong id for " << type->toString();
    next += 1;
    for(uint64_t child = 0; child < type->getSubtypeCount(); ++child) {
      next = checkIds(type->getSubtype(child), next) + 1;
    }
    EXPECT_EQ(next - 1, type->getMaximumColumnId())
      << "Wrong maximum id for " << type->toString();
    return type->getMaximumColumnId();
  }

  TEST(TestType, simple) {
    std::unique_ptr<Type> myType = createStructType();
    myType->addStructField("myInt", createPrimitiveType(INT));
    myType->addStructField("myString", createPrimitiveType(STRING));
    myType->addStructField("myFloat", createPrimitiveType(FLOAT));
    myType->addStructField("list", createListType(createPrimitiveType(LONG)));
    myType->addStructField("bool", createPrimitiveType(BOOLEAN));

    EXPECT_EQ(0, myType->getColumnId());
    EXPECT_EQ(6, myType->getMaximumColumnId());
    EXPECT_EQ(5, myType->getSubtypeCount());
    EXPECT_EQ(STRUCT, myType->getKind());
    EXPECT_EQ("struct<myInt:int,myString:string,myFloat:float,"
              "list:array<bigint>,bool:boolean>",
              myType->toString());
    checkIds(myType.get(), 0);

    const Type* child = myType->getSubtype(0);
    EXPECT_EQ(1, child->getColumnId());
    EXPECT_EQ(1, child->getMaximumColumnId());
    EXPECT_EQ(INT, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = myType->getSubtype(1);
    EXPECT_EQ(2, child->getColumnId());
    EXPECT_EQ(2, child->getMaximumColumnId());
    EXPECT_EQ(STRING, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = myType->getSubtype(2);
    EXPECT_EQ(3, child->getColumnId());
    EXPECT_EQ(3, child->getMaximumColumnId());
    EXPECT_EQ(FLOAT, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = myType->getSubtype(3);
    EXPECT_EQ(4, child->getColumnId());
    EXPECT_EQ(5, child->getMaximumColumnId());
    EXPECT_EQ(LIST, child->getKind());
    EXPECT_EQ(1, child->getSubtypeCount());
    EXPECT_EQ("array<bigint>", child->toString());

    child = child->getSubtype(0);
    EXPECT_EQ(5, child->getColumnId());
    EXPECT_EQ(5, child->getMaximumColumnId());
    EXPECT_EQ(LONG, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());

    child = myType->getSubtype(4);
    EXPECT_EQ(6, child->getColumnId());
    EXPECT_EQ(6, child->getMaximumColumnId());
    EXPECT_EQ(BOOLEAN, child->getKind());
    EXPECT_EQ(0, child->getSubtypeCount());
  }

  TEST(TestType, nested) {
    std::unique_ptr<Type> myType = createStructType();
    {
      std::unique_ptr<Type> innerStruct = createStructType();
      innerStruct->addStructField("col0", createPrimitiveType(INT));

      std::unique_ptr<Type> unionType = createUnionType();
      unionType->addUnionChild(std::move(innerStruct));
      unionType->addUnionChild(createPrimitiveType(STRING));

      myType->addStructField("myList",
                             createListType
                             (createMapType(createPrimitiveType(STRING),
                                            std::move(unionType))));
    }

    // get a pointer to the bottom type
    const Type* listType = myType->getSubtype(0);
    const Type* mapType = listType->getSubtype(0);
    const Type* unionType = mapType->getSubtype(1);
    const Type* structType = unionType->getSubtype(0);
    const Type* intType = structType->getSubtype(0);

    // calculate the id of the child to make sure that we climb correctly
    EXPECT_EQ(6, intType->getColumnId());
    EXPECT_EQ(6, intType->getMaximumColumnId());
    EXPECT_EQ("int", intType->toString());

    checkIds(myType.get(), 0);

    EXPECT_EQ(5, structType->getColumnId());
    EXPECT_EQ(6, structType->getMaximumColumnId());
    EXPECT_EQ("struct<col0:int>", structType->toString());

    EXPECT_EQ(4, unionType->getColumnId());
    EXPECT_EQ(7, unionType->getMaximumColumnId());
    EXPECT_EQ("uniontype<struct<col0:int>,string>", unionType->toString());

    EXPECT_EQ(2, mapType->getColumnId());
    EXPECT_EQ(7, mapType->getMaximumColumnId());
    EXPECT_EQ("map<string,uniontype<struct<col0:int>,string>>",
              mapType->toString());

    EXPECT_EQ(1, listType->getColumnId());
    EXPECT_EQ(7, listType->getMaximumColumnId());
    EXPECT_EQ("array<map<string,uniontype<struct<col0:int>,string>>>",
              listType->toString());

    EXPECT_EQ(0, myType->getColumnId());
    EXPECT_EQ(7, myType->getMaximumColumnId());
    EXPECT_EQ("struct<myList:array<map<string,uniontype<struct<col0:int>,"
              "string>>>>",
              myType->toString());
  }

  TEST(TestType, selectedType) {
    std::unique_ptr<Type> myType = createStructType();
    myType->addStructField("col0", createPrimitiveType(BYTE));
    myType->addStructField("col1", createPrimitiveType(SHORT));
    myType->addStructField("col2",
                           createListType(createPrimitiveType(STRING)));
    myType->addStructField("col3",
                           createMapType(createPrimitiveType(FLOAT),
                                         createPrimitiveType(DOUBLE)));
    std::unique_ptr<Type> unionType = createUnionType();
    unionType->addUnionChild(createCharType(CHAR, 100));
    unionType->addUnionChild(createCharType(VARCHAR, 200));
    myType->addStructField("col4", std::move(unionType));
    myType->addStructField("col5", createPrimitiveType(INT));
    myType->addStructField("col6", createPrimitiveType(LONG));
    myType->addStructField("col7", createDecimalType(10, 2));

    checkIds(myType.get(), 0);
    EXPECT_EQ("struct<col0:tinyint,col1:smallint,col2:array<string>,"
              "col3:map<float,double>,col4:uniontype<char(100),varchar(200)>,"
              "col5:int,col6:bigint,col7:decimal(10,2)>", myType->toString());
    EXPECT_EQ(0, myType->getColumnId());
    EXPECT_EQ(13, myType->getMaximumColumnId());

    std::vector<bool> selected(14);
    selected[0] = true;
    selected[2] = true;
    std::unique_ptr<Type> cutType = buildSelectedType(myType.get(),
                                                      selected);
    EXPECT_EQ("struct<col1:smallint>", cutType->toString());
    EXPECT_EQ(0, cutType->getColumnId());
    EXPECT_EQ(13, cutType->getMaximumColumnId());
    EXPECT_EQ(2, cutType->getSubtype(0)->getColumnId());

    selected.assign(14, true);
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col0:tinyint,col1:smallint,col2:array<string>,"
              "col3:map<float,double>,col4:uniontype<char(100),varchar(200)>,"
              "col5:int,col6:bigint,col7:decimal(10,2)>", cutType->toString());
    EXPECT_EQ(0, cutType->getColumnId());
    EXPECT_EQ(13, cutType->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[8] = true;
    selected[10] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col4:uniontype<varchar(200)>>", cutType->toString());
    EXPECT_EQ(0, cutType->getColumnId());
    EXPECT_EQ(13, cutType->getMaximumColumnId());
    EXPECT_EQ(8, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(10, cutType->getSubtype(0)->getMaximumColumnId());
    EXPECT_EQ(10, cutType->getSubtype(0)->getSubtype(0)->getColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[8] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col4:uniontype<>>", cutType->toString());

    selected.assign(14, false);
    selected[0] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<>", cutType->toString());

    selected.assign(14, false);
    selected[0] = true;
    selected[3] = true;
    selected[4] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col2:array<string>>", cutType->toString());

    selected.assign(14, false);
    selected[0] = true;
    selected[3] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col2:array<void>>", cutType->toString());
    EXPECT_EQ(3, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(4, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[5] = true;
    selected[6] = true;
    selected[7] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col3:map<float,double>>", cutType->toString());
    EXPECT_EQ(5, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(7, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[5] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col3:map<void,void>>", cutType->toString());
    EXPECT_EQ(5, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(7, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[5] = true;
    selected[6] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col3:map<float,void>>", cutType->toString());
    EXPECT_EQ(5, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(7, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[5] = true;
    selected[7] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col3:map<void,double>>", cutType->toString());
    EXPECT_EQ(5, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(7, cutType->getSubtype(0)->getMaximumColumnId());

    selected.assign(14, false);
    selected[0] = true;
    selected[1] = true;
    selected[13] = true;
    cutType = buildSelectedType(myType.get(), selected);
    EXPECT_EQ("struct<col0:tinyint,col7:decimal(10,2)>", cutType->toString());
    EXPECT_EQ(1, cutType->getSubtype(0)->getColumnId());
    EXPECT_EQ(1, cutType->getSubtype(0)->getMaximumColumnId());
    EXPECT_EQ(13, cutType->getSubtype(1)->getColumnId());
    EXPECT_EQ(13, cutType->getSubtype(1)->getMaximumColumnId());
  }
}
