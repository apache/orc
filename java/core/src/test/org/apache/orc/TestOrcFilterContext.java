/*
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

package org.apache.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.apache.orc.impl.SchemaEvolution;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOrcFilterContext {
  private final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createString())
    .addField("f3",
              TypeDescription.createStruct()
                .addField("a", TypeDescription.createInt())
                .addField("b", TypeDescription.createLong())
                .addField("c",
                          TypeDescription.createMap(TypeDescription.createInt(),
                                                    TypeDescription.createDate())))
    .addField("f4",
              TypeDescription.createList(TypeDescription.createStruct()
                                           .addField("a", TypeDescription.createChar())
                                           .addField("b", TypeDescription.createBoolean())))
    .addField("f5",
              TypeDescription.createMap(TypeDescription.createInt(),
                                        TypeDescription.createDate()))
    .addField("f6",
              TypeDescription.createUnion()
                .addUnionChild(TypeDescription.createInt())
                .addUnionChild(TypeDescription.createStruct()
                                 .addField("a", TypeDescription.createDate())
                                 .addField("b",
                                           TypeDescription.createList(TypeDescription.createChar()))
                )
    );
  private static Configuration configuration;
  private static FileSystem fileSystem;
  private static final Path workDir = new Path(System.getProperty("test.tmp.dir",
          "target" + File.separator + "test"
                  + File.separator + "tmp"));
  private static final Path filePath = new Path(workDir, "orc_filter_file.orc");

  private static final int RowCount = 400;

  private final OrcFilterContext filterContext = new OrcFilterContextImpl(schema, false)
    .setBatch(schema.createRowBatch());
  TypeDescription typeDescriptionACID =
          TypeDescription.fromString("struct<int1:int,string1:string>");
  TypeDescription acidSchema = SchemaEvolution.createEventSchema(typeDescriptionACID);
  private final OrcFilterContext filterContextACID = new OrcFilterContextImpl(acidSchema, true)
          .setBatch(acidSchema.createRowBatch());
  @BeforeEach
  public void setup() {
    filterContext.reset();
  }

  @Test
  public void testTopLevelElementaryType() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f1");
    assertEquals(1, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof LongColumnVector);
  }

  @Test
  public void testTopLevelElementaryTypeCaseInsensitive() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("F1");
    assertEquals(1, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof LongColumnVector);
  }

  @Test
  public void testTopLevelCompositeType() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3");
    assertEquals(1, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof StructColumnVector);

    vectorBranch = filterContext.findColumnVector("f4");
    assertEquals(1, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof ListColumnVector);

    vectorBranch = filterContext.findColumnVector("f5");
    assertEquals(1, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof MapColumnVector);

    vectorBranch = filterContext.findColumnVector("f6");
    assertEquals(1, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof UnionColumnVector);
  }

  @Test
  public void testNestedType() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3.a");
    assertEquals(2, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof StructColumnVector);
    assertTrue(vectorBranch[1] instanceof LongColumnVector);

    vectorBranch = filterContext.findColumnVector("f3.c");
    assertEquals(2, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof StructColumnVector);
    assertTrue(vectorBranch[1] instanceof MapColumnVector);

    vectorBranch = filterContext.findColumnVector("f6.1.b");
    assertEquals(3, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof UnionColumnVector);
    assertTrue(vectorBranch[1] instanceof StructColumnVector);
    assertTrue(vectorBranch[2] instanceof ListColumnVector);
  }

  @Test
  public void testTopLevelVector() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3");
    vectorBranch[0].noNulls = true;
    assertTrue(OrcFilterContext.noNulls(vectorBranch));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 0));

    vectorBranch[0].noNulls = false;
    vectorBranch[0].isNull[0] = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
  }

  @Test
  public void testNestedVector() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = true;
    vectorBranch[1].noNulls = true;
    assertTrue(OrcFilterContext.noNulls(vectorBranch));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 2));

    vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = false;
    vectorBranch[0].isNull[0] = true;
    vectorBranch[1].noNulls = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 2));

    vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = true;
    vectorBranch[1].noNulls = false;
    vectorBranch[1].isNull[2] = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 2));

    vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = false;
    vectorBranch[0].isNull[0] = true;
    vectorBranch[1].noNulls = false;
    vectorBranch[1].isNull[2] = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 2));
  }

  @Test
  public void testTopLevelList() {
    TypeDescription topListSchema = TypeDescription.createList(
      TypeDescription.createStruct()
        .addField("a", TypeDescription.createChar())
        .addField("b", TypeDescription
          .createBoolean()));
    OrcFilterContext fc = new OrcFilterContextImpl(topListSchema, false)
      .setBatch(topListSchema.createRowBatch());
    ColumnVector[] vectorBranch = fc.findColumnVector("_elem");
    assertEquals(2, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof ListColumnVector);
    assertTrue(vectorBranch[1] instanceof StructColumnVector);
  }

  @Test
  public void testUnsupportedIsNullUse() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f4._elem.a");
    assertEquals(3, vectorBranch.length);
    assertTrue(vectorBranch[0] instanceof ListColumnVector);
    assertTrue(vectorBranch[1] instanceof StructColumnVector);
    assertTrue(vectorBranch[2] instanceof BytesColumnVector);

    assertTrue(OrcFilterContext.noNulls(vectorBranch));
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                                                      () -> OrcFilterContext.isNull(vectorBranch,
                                                                                    0));
    assertTrue(exception.getMessage().contains("ListColumnVector"));
    assertTrue(exception.getMessage().contains("List and Map vectors are not supported"));
  }

  @Test
  public void testRepeatingVector() {
    ColumnVector[] vectorBranch = filterContext.findColumnVector("f3.a");
    vectorBranch[0].noNulls = true;
    vectorBranch[0].isRepeating = true;
    vectorBranch[1].noNulls = true;
    assertTrue(OrcFilterContext.noNulls(vectorBranch));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 0));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 1));
    assertFalse(OrcFilterContext.isNull(vectorBranch, 2));

    vectorBranch[0].noNulls = false;
    vectorBranch[0].isRepeating = true;
    vectorBranch[0].isNull[0] = true;
    vectorBranch[1].noNulls = true;
    assertFalse(OrcFilterContext.noNulls(vectorBranch));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 0));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 1));
    assertTrue(OrcFilterContext.isNull(vectorBranch, 2));
  }

  @Test
  public void testACIDTable() {
    ColumnVector[] columnVector = filterContextACID.findColumnVector("string1");
    assertEquals(1, columnVector.length);
    assertTrue(columnVector[0] instanceof BytesColumnVector, "Expected a  BytesColumnVector, but found "+ columnVector[0].getClass());
    columnVector = filterContextACID.findColumnVector("int1");
    assertEquals(1, columnVector.length);
    assertTrue(columnVector[0] instanceof LongColumnVector, "Expected a  LongColumnVector, but found "+ columnVector[0].getClass());
  }

  @Test
  public void testRowFilterWithACIDTable() throws IOException {
    createAcidORCFile();
    readSingleRowWithFilter(new Random().nextInt(RowCount));
    fileSystem.delete(filePath, false);
  }

  private void createAcidORCFile() throws IOException {
    configuration = new Configuration();
    fileSystem = FileSystem.get(configuration);

    try (Writer writer = OrcFile.createWriter(filePath,
            OrcFile.writerOptions(configuration)
                    .fileSystem(fileSystem)
                    .overwrite(true)
                    .rowIndexStride(8192)
                    .setSchema(acidSchema))) {

      Random random = new Random(1024);
      VectorizedRowBatch vectorizedRowBatch = acidSchema.createRowBatch();
      for (int rowId = 0; rowId < RowCount; rowId++) {
        long v = random.nextLong();
        populateColumnValues(acidSchema, vectorizedRowBatch.cols,vectorizedRowBatch.size, v);
        // Populate the rowId
        ((LongColumnVector) vectorizedRowBatch.cols[3]).vector[vectorizedRowBatch.size] = rowId;
        StructColumnVector row = (StructColumnVector) vectorizedRowBatch.cols[5];
        ((LongColumnVector) row.fields[0]).vector[vectorizedRowBatch.size] = rowId;
        vectorizedRowBatch.size += 1;
        if (vectorizedRowBatch.size == vectorizedRowBatch.getMaxSize()) {
          writer.addRowBatch(vectorizedRowBatch);
          vectorizedRowBatch.reset();
        }
      }
      if (vectorizedRowBatch.size > 0) {
        writer.addRowBatch(vectorizedRowBatch);
        vectorizedRowBatch.reset();
      }
    }
  }

  private void populateColumnValues(TypeDescription typeDescription, ColumnVector[] columnVectors, int index, long value) {
    for (int columnId = 0; columnId < typeDescription.getChildren().size() ; columnId++) {
      switch (typeDescription.getChildren().get(columnId).getCategory()) {
        case INT:
          ((LongColumnVector)columnVectors[columnId]).vector[index] = value;
          break;
        case LONG:
          ((LongColumnVector)columnVectors[columnId]).vector[index] = value;
          break;
        case STRING:
          ((BytesColumnVector) columnVectors[columnId]).setVal(index,
                  ("String-"+ index).getBytes(StandardCharsets.UTF_8));
          break;
        case STRUCT:
          populateColumnValues(typeDescription.getChildren().get(columnId), ((StructColumnVector)columnVectors[columnId]).fields, index, value);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  private void readSingleRowWithFilter(int id) throws IOException {
    Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(configuration).filesystem(fileSystem));
    SearchArgument searchArgument = SearchArgumentFactory.newBuilder()
            .in("int1", PredicateLeaf.Type.LONG, new Long(id))
            .build();
    Reader.Options readerOptions = reader.options()
            .searchArgument(searchArgument, new String[] {"int1"})
            .useSelected(true)
            .allowSARGToFilter(true);
    VectorizedRowBatch vectorizedRowBatch = acidSchema.createRowBatch();
    long rowCount = 0;
    try (RecordReader recordReader = reader.rows(readerOptions)) {
      assertTrue(recordReader.nextBatch(vectorizedRowBatch));
      rowCount += vectorizedRowBatch.size;
      assertEquals(6, vectorizedRowBatch.cols.length);
      assertTrue(vectorizedRowBatch.cols[5] instanceof StructColumnVector);
      assertTrue(((StructColumnVector) vectorizedRowBatch.cols[5]).fields[0] instanceof LongColumnVector);
      assertTrue(((StructColumnVector) vectorizedRowBatch.cols[5]).fields[1] instanceof BytesColumnVector);
      assertEquals(id, ((LongColumnVector) ((StructColumnVector) vectorizedRowBatch.cols[5]).fields[0]).vector[vectorizedRowBatch.selected[0]]);
      checkStringColumn(id, vectorizedRowBatch);
      assertFalse(recordReader.nextBatch(vectorizedRowBatch));
    }
    assertEquals(1, rowCount);
  }

  private static void checkStringColumn(int id, VectorizedRowBatch vectorizedRowBatch) {
    BytesColumnVector bytesColumnVector = (BytesColumnVector) ((StructColumnVector) vectorizedRowBatch.cols[5]).fields[1];
    assertEquals("String-"+ id, bytesColumnVector.toString(id));
  }
}
