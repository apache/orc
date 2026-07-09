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

package org.apache.orc.impl.reader;

import org.apache.orc.DataReader;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.MockDataReader;
import org.apache.orc.impl.MockStripe;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.StreamName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStripePlanner {

  private static ByteBuffer createDataStream(int bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(bytes);
    buffer.position(0);
    buffer.limit(bytes);
    return buffer;
  }

  /**
   * A stream whose declared length pushes its offset past the stripe's
   * index+data region must be rejected, matching the C++ reader's
   * StripeStreamsImpl::getStream boundary check.
   */
  @Test
  public void testStreamLengthExceedsStripeBoundary() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int>");
    // stripe data region is [offset, offset + indexLength + dataLength) = [3, 103)
    OrcProto.StripeInformation protoStripe = OrcProto.StripeInformation.newBuilder()
        .setOffset(3)
        .setIndexLength(0)
        .setDataLength(100)
        .setNumberOfRows(1000)
        .build();
    StripeInformation stripe =
        new ReaderImpl.StripeInformationImpl(protoStripe, 0, -1, null);
    // a single DATA stream whose length (1000) far exceeds the 100-byte data region
    OrcProto.StripeFooter footer = OrcProto.StripeFooter.newBuilder()
        .addStreams(OrcProto.Stream.newBuilder()
            .setColumn(1)
            .setKind(OrcProto.Stream.Kind.DATA)
            .setLength(1000)
            .build())
        .addColumns(OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build())
        .addColumns(OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build())
        .build();
    DataReader dataReader = mock(DataReader.class);
    when(dataReader.readStripeFooter(any())).thenReturn(footer);

    StripePlanner planner = new StripePlanner(schema, new ReaderEncryption(),
        dataReader, OrcFile.WriterVersion.ORC_14, false, Integer.MAX_VALUE);
    FileFormatException e = assertThrows(FileFormatException.class,
        () -> planner.parseStripe(stripe, new boolean[]{true, true}));
    assertTrue(e.getMessage().contains("Malformed ORC file"), e.getMessage());
  }

  /**
   * A well-formed stripe, whose stream lengths sum exactly to the index+data
   * region, still plans correctly.
   */
  @Test
  public void testWellFormedStripePlans() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int>");
    MockDataReader dataReader = new MockDataReader(schema)
        .addStream(1, OrcProto.Stream.Kind.PRESENT, createDataStream(1000))
        .addStream(1, OrcProto.Stream.Kind.DATA, createDataStream(9000))
        .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
        .addStripeFooter(1000, null);
    MockStripe stripe = dataReader.getStripe(0);

    StripePlanner planner = new StripePlanner(schema, new ReaderEncryption(),
        dataReader, OrcFile.WriterVersion.ORC_14, false, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, true});
    planner.readData(null, null, false,
        org.apache.orc.impl.reader.tree.TypeReader.ReadPhase.ALL);
    assertNotNull(planner.getStream(new StreamName(1, OrcProto.Stream.Kind.DATA)));
  }
}
