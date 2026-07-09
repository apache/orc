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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.orc.FileFormatException;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.apache.orc.impl.writer.StreamOptions;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestStringDictionaryTreeReader {

  /**
   * A negative decoded dictionary entry length in the LENGTH stream must be rejected,
   * matching the C++ reader's ParseError in DictionaryLoader.cc.
   */
  @Test
  public void testNegativeDictionaryEntryLength() throws Exception {
    // Build a LENGTH stream (unsigned RLEv1) whose second entry decodes to a negative
    // int64. The reader creates the length IntegerReader with signed == false, so use a
    // matching unsigned writer; writing -1L round-trips back to -1L.
    TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
    RunLengthIntegerWriter out = new RunLengthIntegerWriter(
        new OutStream("test", new StreamOptions(1000), collect), false);
    out.write(10L);
    out.write(-1L);
    out.write(10L);
    out.flush();

    ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
    collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
    inBuf.flip();
    InStream lengthStream = InStream.create("test",
        new BufferChunk(inBuf, 0), 0, inBuf.remaining());

    OrcProto.ColumnEncoding encoding = OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DICTIONARY)
        .setDictionarySize(3)
        .build();

    TypeDescription schema = TypeDescription.fromString("struct<x:string>");
    SchemaEvolution evo = new SchemaEvolution(schema, schema,
        new Reader.Options(new Configuration()));
    TreeReaderFactory.Context context =
        new TreeReaderFactory.ReaderContext().setSchemaEvolution(evo);
    TreeReaderFactory.StringDictionaryTreeReader reader =
        new TreeReaderFactory.StringDictionaryTreeReader(
            1, null, null, lengthStream, null, encoding, context);

    // The lazy dictionary read happens inside nextVector, before super.nextVector and
    // before the FilterContext is used, so the guard fires with a null FilterContext.
    FileFormatException e = assertThrows(FileFormatException.class,
        () -> reader.nextVector(new BytesColumnVector(1024), null, 1, null,
            TypeReader.ReadPhase.ALL));
    assertTrue(e.getMessage().contains("Negative dictionary entry length"),
        "Unexpected message: " + e.getMessage());
  }
}
