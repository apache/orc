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

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.apache.orc.impl.writer.StreamOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestTreeReaderFactory {

  private static final OrcProto.ColumnEncoding DIRECT =
      OrcProto.ColumnEncoding.newBuilder()
          .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();

  private static InStream toInStream(TestInStream.OutputCollector collect) {
    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    return InStream.create("test", new BufferChunk(inBuf, 0), 0,
        inBuf.remaining());
  }

  private static RunLengthByteReader createTagReader(byte... tags)
      throws IOException {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    RunLengthByteWriter writer = new RunLengthByteWriter(
        new OutStream("test", new StreamOptions(100), collect));
    for (byte tag : tags) {
      writer.write(tag);
    }
    writer.flush();
    return new RunLengthByteReader(toInStream(collect));
  }

  private static InStream createLongStream(long... values) throws IOException {
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    RunLengthIntegerWriter writer = new RunLengthIntegerWriter(
        new OutStream("test", new StreamOptions(100), collect), true);
    for (long value : values) {
      writer.write(value);
    }
    writer.flush();
    return toInStream(collect);
  }

  @Test
  public void testUnionWithValidTags() throws Exception {
    TreeReaderFactory.Context context = new TreeReaderFactory.ReaderContext();
    TypeReader[] children = new TypeReader[]{
        new TreeReaderFactory.LongTreeReader(1, null,
            createLongStream(10, 30), DIRECT, context),
        new TreeReaderFactory.LongTreeReader(2, null,
            createLongStream(20), DIRECT, context)};
    TreeReaderFactory.UnionTreeReader reader =
        new TreeReaderFactory.UnionTreeReader(0, null, context, null, children);
    reader.tags = createTagReader((byte) 0, (byte) 1, (byte) 0);

    UnionColumnVector batch = new UnionColumnVector(3,
        new LongColumnVector(3), new LongColumnVector(3));
    reader.nextVector(batch, null, 3, null, TypeReader.ReadPhase.ALL);

    assertEquals(0, batch.tags[0]);
    assertEquals(1, batch.tags[1]);
    assertEquals(0, batch.tags[2]);
    assertEquals(10, ((LongColumnVector) batch.fields[0]).vector[0]);
    assertEquals(20, ((LongColumnVector) batch.fields[1]).vector[1]);
    assertEquals(30, ((LongColumnVector) batch.fields[0]).vector[2]);
  }

  @Test
  public void testUnionWithInvalidTagInNextVector() throws Exception {
    TreeReaderFactory.Context context = new TreeReaderFactory.ReaderContext();
    TypeReader[] children = new TypeReader[]{
        new TreeReaderFactory.LongTreeReader(1, context),
        new TreeReaderFactory.LongTreeReader(2, context)};
    TreeReaderFactory.UnionTreeReader reader =
        new TreeReaderFactory.UnionTreeReader(0, null, context, null, children);
    reader.tags = createTagReader((byte) 0, (byte) 1, (byte) 5);

    UnionColumnVector batch = new UnionColumnVector(3,
        new LongColumnVector(3), new LongColumnVector(3));
    FileFormatException e = assertThrows(FileFormatException.class, () ->
        reader.nextVector(batch, null, 3, null, TypeReader.ReadPhase.ALL));
    assertEquals("Invalid union tag 5 for union with 2 children",
        e.getMessage());
  }

  @Test
  public void testUnionWithInvalidTagInSkipRows() throws Exception {
    TreeReaderFactory.Context context = new TreeReaderFactory.ReaderContext();
    TypeReader[] children = new TypeReader[]{
        new TreeReaderFactory.LongTreeReader(1, context),
        new TreeReaderFactory.LongTreeReader(2, context)};
    TreeReaderFactory.UnionTreeReader reader =
        new TreeReaderFactory.UnionTreeReader(0, null, context, null, children);
    reader.tags = createTagReader((byte) 0, (byte) 1, (byte) 0x80);

    FileFormatException e = assertThrows(FileFormatException.class, () ->
        reader.skipRows(3, TypeReader.ReadPhase.ALL));
    assertEquals("Invalid union tag -128 for union with 2 children",
        e.getMessage());
  }
}
