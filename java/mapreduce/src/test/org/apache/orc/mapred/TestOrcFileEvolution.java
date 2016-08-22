/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.mapred;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.*;
import org.apache.orc.TypeDescription.Category;
import org.apache.orc.impl.SchemaEvolution;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test the behavior of ORC's schema evolution
 */
public class TestOrcFileEvolution {

  // These utility methods are just to make writing tests easier. The values
  // created here will not feed directly to the ORC writers, but are converted
  // within checkEvolution().
  private List<Object> struct(Object... fields) {
    return list(fields);
  }

  private List<Object> list(Object... elements) {
    return Arrays.asList(elements);
  }

  private Map<Object, Object> map(Object... kvs) {
    if (kvs.length != 2) {
      throw new IllegalArgumentException(
          "Map must be provided an even number of arguments");
    }

    Map<Object, Object> result = new HashMap<>();
    for (int i = 0; i < kvs.length; i += 2) {
      result.put(kvs[i], kvs[i + 1]);
    }
    return result;
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @Rule
  public TestName testCaseName = new TestName();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testAddFieldToEnd() {
    checkEvolution("struct<a:int,b:string>", "struct<a:int,b:string,c:double>",
        struct(1, "foo"),
        struct(1, "foo", null));
  }

  @Test
  public void testAddFieldBeforeEnd() {
    checkEvolution("struct<a:int,b:string>", "struct<a:int,c:double,b:string>",
        struct(1, "foo"),
        struct(1, null, "foo"));
  }

  @Test
  public void testRemoveLastField() {
    checkEvolution("struct<a:int,b:string,c:double>", "struct<a:int,b:string>",
        struct(1, "foo", 3.14),
        struct(1, "foo"));
  }

  @Test
  public void testRemoveFieldBeforeEnd() {
    checkEvolution("struct<a:int,b:string,c:double>", "struct<a:int,c:double>",
        struct(1, "foo", 3.14),
        struct(1, 3.14));
  }

  @Test
  public void testRemoveAndAddField() {
    checkEvolution("struct<a:int,b:string>", "struct<a:int,c:double>",
        struct(1, "foo"), struct(1, null));
  }

  @Test
  public void testReorderFields() {
    checkEvolution("struct<a:int,b:string>", "struct<b:string,a:int>",
        struct(1, "foo"), struct("foo", 1));
  }

  @Test
  public void testAddFieldEndOfStruct() {
    checkEvolution("struct<a:struct<b:int>,c:string>",
        "struct<a:struct<b:int,d:double>,c:string>",
        struct(struct(2), "foo"), struct(struct(2, null), "foo"));
  }

  @Test
  public void testAddFieldBeforeEndOfStruct() {
    checkEvolution("struct<a:struct<b:int>,c:string>",
        "struct<a:struct<d:double,b:int>,c:string>",
        struct(struct(2), "foo"), struct(struct(null, 2), "foo"));
  }

  @Test
  public void testAddSimilarField() {
    checkEvolution("struct<a:struct<b:int>>",
        "struct<a:struct<b:int>,c:struct<b:int>>", struct(struct(2)),
        struct(struct(2), null));
  }

  @Test
  public void testConvergentEvolution() {
    checkEvolution("struct<a:struct<a:int,b:string>,c:struct<a:int>>",
        "struct<a:struct<a:int,b:string>,c:struct<a:int,b:string>>",
        struct(struct(2, "foo"), struct(3)),
        struct(struct(2, "foo"), struct(3, null)));
  }

  @Test
  public void testMapKeyEvolution() {
    checkEvolution("struct<a:map<struct<a:int>,int>>",
        "struct<a:map<struct<a:int,b:string>,int>>",
        struct(map(struct(1), 2)),
        struct(map(struct(1, null), 2)));
  }

  @Test
  public void testMapValueEvolution() {
    checkEvolution("struct<a:map<int,struct<a:int>>>",
        "struct<a:map<int,struct<a:int,b:string>>>",
        struct(map(2, struct(1))),
        struct(map(2, struct(1, null))));
  }

  @Test
  public void testListEvolution() {
    checkEvolution("struct<a:array<struct<b:int>>>",
        "struct<a:array<struct<b:int,c:string>>>",
        struct(list(struct(1), struct(2))),
        struct(list(struct(1, null), struct(2, null))));
  }

  @Test
  public void testPreHive4243CheckEqual() {
    // Expect success on equal schemas
    checkEvolution("struct<_col0:int,_col1:string>",
        "struct<_col0:int,_col1:string>",
        struct(1, "foo"),
        struct(1, "foo", null), false);
  }

  @Test
  public void testPreHive4243Check() {
    // Expect exception on strict compatibility check
    thrown.expectMessage("HIVE-4243");
    checkEvolution("struct<_col0:int,_col1:string>",
        "struct<_col0:int,_col1:string,_col2:double>",
        struct(1, "foo"),
        struct(1, "foo", null), false);
  }

  @Test
  public void testPreHive4243AddColumn() {
    checkEvolution("struct<_col0:int,_col1:string>",
        "struct<_col0:int,_col1:string,_col2:double>",
        struct(1, "foo"),
        struct(1, "foo", null), true);
  }

  @Test
  public void testPreHive4243AddColumnMiddle() {
    // Expect exception on type mismatch
    thrown.expect(SchemaEvolution.IllegalEvolutionException.class);
    checkEvolution("struct<_col0:int,_col1:double>",
        "struct<_col0:int,_col1:date,_col2:double>",
        struct(1, 1.0),
        null, true);
  }

  @Test
  public void testPreHive4243AddColumnWithFix() {
    checkEvolution("struct<_col0:int,_col1:string>",
        "struct<a:int,b:string,c:double>",
        struct(1, "foo"),
        struct(1, "foo", null), true);
  }

  @Test
  public void testPreHive4243AddColumnMiddleWithFix() {
    // Expect exception on type mismatch
    thrown.expect(SchemaEvolution.IllegalEvolutionException.class);
    checkEvolution("struct<_col0:int,_col1:double>",
        "struct<a:int,b:date,c:double>",
        struct(1, 1.0),
        null, true);
  }

  private void checkEvolution(String writerType, String readerType,
      Object inputRow, Object expectedOutput) {
    checkEvolution(writerType, readerType,
        inputRow, expectedOutput,
        (boolean) OrcConf.TOLERATE_MISSING_SCHEMA.getDefaultValue());
  }

  private void checkEvolution(String writerType, String readerType,
      Object inputRow, Object expectedOutput, boolean tolerateSchema) {
    TypeDescription readTypeDescr = TypeDescription.fromString(readerType);
    TypeDescription writerTypeDescr = TypeDescription.fromString(writerType);

    OrcStruct inputStruct = assembleStruct(writerTypeDescr, inputRow);
    OrcStruct expectedStruct = assembleStruct(readTypeDescr, expectedOutput);
    try {
      Writer writer = OrcFile.createWriter(testFilePath,
          OrcFile.writerOptions(conf).setSchema(writerTypeDescr)
              .stripeSize(100000).bufferSize(10000)
              .version(OrcFile.Version.CURRENT));

      OrcMapredRecordWriter<OrcStruct> recordWriter =
          new OrcMapredRecordWriter<OrcStruct>(writer);
      recordWriter.write(NullWritable.get(), inputStruct);
      recordWriter.close(mock(Reporter.class));
      Reader reader = OrcFile.createReader(testFilePath,
          OrcFile.readerOptions(conf).filesystem(fs));
      OrcMapredRecordReader<OrcStruct> recordReader =
          new OrcMapredRecordReader<>(reader,
              reader.options().schema(readTypeDescr)
                  .tolerateMissingSchema(tolerateSchema));
      OrcStruct result = recordReader.createValue();
      recordReader.next(recordReader.createKey(), result);
      assertEquals(expectedStruct, result);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private OrcStruct assembleStruct(TypeDescription type, Object row) {
    Preconditions.checkArgument(
        type.getCategory() == Category.STRUCT, "Top level type must be STRUCT");

    return (OrcStruct) assembleRecord(type, row);
  }

  private WritableComparable assembleRecord(TypeDescription type, Object row) {
    if (row == null) {
      return null;
    }
    switch (type.getCategory()) {
    case STRUCT:
      OrcStruct structResult = new OrcStruct(type);
      for (int i = 0; i < structResult.getNumFields(); i++) {
        List<TypeDescription> childTypes = type.getChildren();
        structResult.setFieldValue(i,
            assembleRecord(childTypes.get(i), ((List<Object>) row).get(i)));
      }
      return structResult;
    case LIST:
      OrcList<WritableComparable> listResult = new OrcList<>(type);
      TypeDescription elemType = type.getChildren().get(0);
      List<Object> elems = (List<Object>) row;
      for (int i = 0; i < elems.size(); i++) {
        listResult.add(assembleRecord(elemType, elems.get(i)));
      }
      return listResult;
    case MAP:
      OrcMap<WritableComparable, WritableComparable> mapResult =
          new OrcMap<>(type);
      TypeDescription keyType = type.getChildren().get(0);
      TypeDescription valueType = type.getChildren().get(1);
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) row)
          .entrySet()) {
        mapResult.put(assembleRecord(keyType, entry.getKey()),
            assembleRecord(valueType, entry.getValue()));
      }
      return mapResult;
    case INT:
      return new IntWritable((Integer) row);
    case DOUBLE:
      return new DoubleWritable((Double) row);
    case STRING:
      return new Text((String) row);
    default:
      throw new UnsupportedOperationException(String
          .format("Not expecting to have a field of type %s in unit tests",
              type.getCategory()));
    }
  }
}
