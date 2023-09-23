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

import org.apache.orc.OrcFile.WriterImplementation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.orc.OrcUtils.getSoftwareVersion;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for OrcUtils.
 */
public class TestOrcUtils {

  @Test
  public void testBloomFilterIncludeColumns() {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("msisdn", TypeDescription.createString())
        .addField("imsi",  TypeDescription.createVarchar())
        .addField("imei", TypeDescription.createInt());

    boolean[] includeColumns = new boolean[3+1];
    includeColumns[1] = true;
    includeColumns[3] = true;

    assertArrayEquals(includeColumns, OrcUtils.includeColumns("msisdn, imei", schema));
  }

  @Test
  public void testBloomFilterIncludeColumns_ACID() {
    TypeDescription rowSchema = TypeDescription.createStruct()
        .addField("msisdn", TypeDescription.createString())
        .addField("imei", TypeDescription.createInt());

    TypeDescription schema = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createString())
        .addField("originalTransaction", TypeDescription.createInt())
        .addField("bucket", TypeDescription.createInt())
        .addField("rowId", TypeDescription.createInt())
        .addField("currentTransaction", TypeDescription.createInt())
        .addField("row", rowSchema);

    boolean[] includeColumns = new boolean[8+1];
    includeColumns[7] = true;

    assertArrayEquals(includeColumns, OrcUtils.includeColumns("msisdn", schema));
  }

  @Test
  public void testBloomFilterIncludeColumns_Nested() {
    TypeDescription rowSchema = TypeDescription.createStruct()
        .addField("msisdn", TypeDescription.createString())
        .addField("imei", TypeDescription.createInt());

    TypeDescription schema = TypeDescription.createStruct()
        .addField("row", rowSchema);

    boolean[] includeColumns = new boolean[3+1];
    includeColumns[2] = true;

    assertArrayEquals(includeColumns, OrcUtils.includeColumns("row.msisdn", schema));
  }

  @Test
  public void testBloomFilterIncludeColumns_NonExisting() {
    TypeDescription rowSchema = TypeDescription.createStruct()
        .addField("msisdn", TypeDescription.createString())
        .addField("imei", TypeDescription.createInt());

    TypeDescription schema = TypeDescription.createStruct()
        .addField("row", rowSchema);

    boolean[] includeColumns = new boolean[3+1];

    assertArrayEquals(includeColumns, OrcUtils.includeColumns("msisdn, row.msisdn2", schema));
  }

  @Test
  public void testGetSoftwareVersion_withoutVersion() {
    assertEquals("ORC Java", getSoftwareVersion(WriterImplementation.ORC_JAVA.getId(), null));
    assertEquals("ORC C++", getSoftwareVersion(WriterImplementation.ORC_CPP.getId(), null));
    assertEquals("Presto", getSoftwareVersion(WriterImplementation.PRESTO.getId(), null));
    assertEquals("Scritchley Go", getSoftwareVersion(WriterImplementation.SCRITCHLEY_GO.getId(), null));
    assertEquals("Trino", getSoftwareVersion(WriterImplementation.TRINO.getId(), null));
    assertEquals("CUDF", getSoftwareVersion(WriterImplementation.CUDF.getId(), null));
    assertEquals("Unknown(2147483647)", getSoftwareVersion(WriterImplementation.UNKNOWN.getId(), null));
  }

  @Test
  public void testGetSoftwareVersion_withVersion() {
    assertEquals("ORC Java 2.0.0", getSoftwareVersion(WriterImplementation.ORC_JAVA.getId(), "2.0.0"));
    assertEquals("ORC C++ 2.0.0", getSoftwareVersion(WriterImplementation.ORC_CPP.getId(), "2.0.0"));
    assertEquals("Presto 2.0.0", getSoftwareVersion(WriterImplementation.PRESTO.getId(), "2.0.0"));
    assertEquals("Scritchley Go 2.0.0", getSoftwareVersion(WriterImplementation.SCRITCHLEY_GO.getId(), "2.0.0"));
    assertEquals("Trino 2.0.0", getSoftwareVersion(WriterImplementation.TRINO.getId(), "2.0.0"));
    assertEquals("CUDF 2.0.0", getSoftwareVersion(WriterImplementation.CUDF.getId(), "2.0.0"));
    assertEquals("Unknown(2147483647) 2.0.0", getSoftwareVersion(WriterImplementation.UNKNOWN.getId(), "2.0.0"));
  }

  @Test
  public void testGetOrcTypes() {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createString())
        .addField("originalTransaction", TypeDescription.createInt())
        .addField("bucket", TypeDescription.createInt())
        .addField("rowId", TypeDescription.createInt())
        .addField("currentTransaction", TypeDescription.createInt());

    List<OrcProto.Type> orcTypes = OrcUtils.getOrcTypes(schema);

    assertEquals(6, orcTypes.size());

    OrcProto.Type operationType = OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).build();
    OrcProto.Type originalTransactionType = OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build();
    OrcProto.Type bucketType = OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build();
    OrcProto.Type rowIdType = OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build();
    OrcProto.Type currentTransactionType = OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).build();
    OrcProto.Type.Builder structType = OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT);
    structType.addSubtypes(schema.findSubtype("operation").getId());
    structType.addAllFieldNames(schema.getFieldNames());
    structType.addSubtypes(schema.findSubtype("originalTransaction").getId());
    structType.addSubtypes(schema.findSubtype("bucket").getId());
    structType.addSubtypes(schema.findSubtype("rowId").getId());
    structType.addSubtypes(schema.findSubtype("currentTransaction").getId());
    List<OrcProto.Type> expectedType = new ArrayList<>();
    expectedType.add(structType.build());
    expectedType.add(operationType);
    expectedType.add(originalTransactionType);
    expectedType.add(bucketType);
    expectedType.add(rowIdType);
    expectedType.add(currentTransactionType);

    assertEquals(expectedType, orcTypes);
  }

  @Test
  public void testIsValidTypeTree() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createString())
        .addField("originalTransaction", TypeDescription.createInt())
        .addField("bucket", TypeDescription.createInt())
        .addField("rowId", TypeDescription.createInt())
        .addField("currentTransaction", TypeDescription.createInt());

    assertEquals(6, OrcUtils.isValidTypeTree(OrcUtils.getOrcTypes(schema), 0));
  }

  @Test
  public void testConvertTypeFromProtobuf() throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createString())
        .addField("originalTransaction", TypeDescription.createInt())
        .addField("bucket", TypeDescription.createInt())
        .addField("rowId", TypeDescription.createInt())
        .addField("currentTransaction", TypeDescription.createInt());
    assertEquals(schema, OrcUtils.convertTypeFromProtobuf(OrcUtils.getOrcTypes(schema), 0));
  }
}
