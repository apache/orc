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

package org.apache.orc.util;

import com.google.protobuf.ByteString;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for BloomFilter
 */
public class TestBloomFilter {

  @Test
  public void testBitset() {
    BloomFilter.BitSet bitset = new BloomFilter.BitSet(128);
    // set every 9th bit for a rotating pattern
    for(int l=0; l < 8; ++l) {
      bitset.set(l*9);
    }
    // set every non-9th bit
    for(int l=8; l < 16; ++l) {
      for(int b=0; b < 8; ++b) {
        if (b != l - 8) {
          bitset.set(l*8+b);
        }
      }
    }
    for(int b=0; b < 64; ++b) {
      assertEquals(b % 9 == 0, bitset.get(b));
    }
    for(int b=64; b < 128; ++b) {
      assertEquals((b % 8) != (b - 64) / 8, bitset.get(b));
    }
    // test that the longs are mapped correctly
    long[] longs = bitset.getData();
    assertEquals(2, longs.length);
    assertEquals(0x8040201008040201L, longs[0]);
    assertEquals(~0x8040201008040201L, longs[1]);
  }

  @Test
  public void testBloomFilterSerialize() {
    long[] bits = new long[]{0x8040201008040201L, ~0x8040201008040201L};
    BloomFilter bloom = new BloomFilterUtf8(bits, 1);
    OrcProto.BloomFilter.Builder builder = OrcProto.BloomFilter.newBuilder();
    BloomFilterIO.serialize(builder, bloom);
    OrcProto.BloomFilter proto = builder.build();
    assertEquals(1, proto.getNumHashFunctions());
    assertEquals(0, proto.getBitsetCount());
    ByteString bs = proto.getUtf8Bitset();
    byte[] expected = new byte[]{0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40,
        (byte) 0x80, ~0x01, ~0x02, ~0x04, ~0x08, ~0x10, ~0x20, ~0x40,
        (byte) ~0x80};
    OrcProto.ColumnEncoding.Builder encoding =
        OrcProto.ColumnEncoding.newBuilder();
    encoding.setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
        .setBloomEncoding(BloomFilterIO.Encoding.UTF8_UTC.getId());
    assertArrayEquals(expected, bs.toByteArray());
    BloomFilter rebuilt = BloomFilterIO.deserialize(
        OrcProto.Stream.Kind.BLOOM_FILTER_UTF8,
        encoding.build(),
        OrcFile.WriterVersion.ORC_135,
        TypeDescription.Category.INT,
        proto);
    assertEquals(bloom, rebuilt);
  }

  @Test
  public void testBloomFilterEquals() {
    long[] bits = new long[]{0x8040201008040201L, ~0x8040201008040201L};
    BloomFilter bloom = new BloomFilterUtf8(bits, 1);
    BloomFilter other = new BloomFilterUtf8(new long[]{0,0}, 1);
    assertEquals(false, bloom.equals(other));
  }
}
