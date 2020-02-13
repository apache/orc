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
package org.apache.orc.impl;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TestVectorOrcFile;
import org.apache.orc.TypeDescription;
import org.junit.Test;

public class TestConvertTreeReaderFactory {

  @Test
  public void testArraySizeBiggerThan1024AndConvertToDecimal() throws Exception {
    Decimal64ColumnVector columnVector = testArraySizeBiggerThan1024("decimal(6,1)", Decimal64ColumnVector.class);
    assertEquals(columnVector.vector.length, 1025);
  }

  public <TExpectedColumn extends ColumnVector> TExpectedColumn testArraySizeBiggerThan1024(
          String typeString, Class<TExpectedColumn> expectedColumnType) throws Exception {
    Reader.Options options = new Reader.Options();
    TypeDescription schema = TypeDescription.fromString("struct<col1:array<"+ typeString +">>");
    options.schema(schema);
    String expected = options.toString();

    Configuration conf = new Configuration();
    Path path = new Path(TestVectorOrcFile.getFileFromClasspath("bigarray.orc"));

    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows(options);
    VectorizedRowBatch batch = schema.createRowBatchV2();
    while (rows.nextBatch(batch)) {
      assertTrue(batch.size > 0);
    }

    assertEquals(expected, options.toString());
    assertEquals(batch.cols.length, 1);
    assertTrue(batch.cols[0] instanceof ListColumnVector);
    assertEquals(((ListColumnVector) batch.cols[0]).child.getClass(), expectedColumnType);
    return (TExpectedColumn) ((ListColumnVector) batch.cols[0]).child;
  }

  @Test
  public void testArraySizeBiggerThan1024AndConvertToVarchar() throws Exception {
    BytesColumnVector columnVector = testArraySizeBiggerThan1024("varchar(10)", BytesColumnVector.class);
    assertEquals(columnVector.vector.length, 1025);
  }

  @Test
  public void testArraySizeBiggerThan1024AndConvertToDouble() throws Exception {
    DoubleColumnVector columnVector = testArraySizeBiggerThan1024("double", DoubleColumnVector.class);
    assertEquals(columnVector.vector.length, 1025);
  }

  @Test
  public void testArraySizeBiggerThan1024AndConvertToInteger() throws Exception {
    LongColumnVector columnVector = testArraySizeBiggerThan1024("int", LongColumnVector.class);
    assertEquals(columnVector.vector.length, 1025);
  }

  @Test
  public void testArraySizeBiggerThan1024AndConvertToTimestamp() throws Exception {
    TimestampColumnVector columnVector = testArraySizeBiggerThan1024("timestamp", TimestampColumnVector.class);
    assertEquals(columnVector.time.length, 1025);
  }
}