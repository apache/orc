package org.apache.orc.impl.mask;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class TestSHA256Mask {

  final byte[] inputLong = (
      "Lorem ipsum dolor sit amet, consectetur adipiscing "
          + "elit. Curabitur quis vehicula ligula. In hac habitasse platea dictumst."
          + " Curabitur mollis finibus erat fringilla vestibulum. In eu leo eget"
          + " massa luctus convallis nec vitae ligula. Donec vitae diam convallis,"
          + " efficitur orci in, imperdiet turpis. In quis semper ex. Duis faucibus "
          + "tellus vitae molestie convallis. Fusce fermentum vestibulum lacus "
          + "vel malesuada. Pellentesque viverra odio a justo aliquet tempus.")
      .getBytes(StandardCharsets.UTF_8);

  final byte[] input32 = "Every flight begins with a fall."
      .getBytes(StandardCharsets.UTF_8);

  final byte[] inputShort = "\uD841\uDF0E".getBytes(StandardCharsets.UTF_8);

  public TestSHA256Mask() {
    super();
  }

  /**
   * Test to make sure that the output is always 64 bytes (equal to hash len) <br>
   * This is because String type does not have bounds on length.
   *
   * @throws Exception
   */
  @Test
  public void testStringSHA256Masking() throws Exception {
    final SHA256MaskFactory sha256Mask = new SHA256MaskFactory();
    final BytesColumnVector source = new BytesColumnVector();
    final BytesColumnVector target = new BytesColumnVector();

    target.reset();

    source.setRef(0, input32, 0, input32.length);
    source.setRef(1, inputShort, 0, inputShort.length);

    for (int r = 0; r < 2; ++r) {
      sha256Mask.maskString(source, r, target, TypeDescription.createString());
    }

    /* Make sure the the mask length is equal to 64 length of SHA-256 */
    assertEquals(64, target.length[0]);
    assertEquals(64, target.length[1]);

  }

  /**
   * Test to make sure that the length of input is equal to the output. <br>
   * If input is shorter than the hash, truncate it. <br>
   * If the input is larger than hash (64) pad it with blank space. <br>
   *
   * @throws Exception
   */
  @Test
  public void testChar256Masking() throws Exception {
    final SHA256MaskFactory sha256Mask = new SHA256MaskFactory();
    final BytesColumnVector source = new BytesColumnVector();
    final BytesColumnVector target = new BytesColumnVector();

    target.reset();

    int[] length = new int[3];

    length[0] = input32.length;
    source.setRef(0, input32, 0, input32.length);

    length[1] = inputShort.length;
    source.setRef(1, inputShort, 0, inputShort.length);

    length[2] = inputLong.length;
    source.setRef(2, inputLong, 0, inputLong.length);

    for (int r = 0; r < 3; ++r) {
      sha256Mask.maskString(source, r, target,
          TypeDescription.createChar().withMaxLength(length[r]));
    }

    /* Make sure the the mask length is equal to 64 length of SHA-256 */
    assertEquals(length[0], target.length[0]);
    assertEquals(length[1], target.length[1]);
    assertEquals(length[2], target.length[2]);

  }

  @Test
  public void testVarChar256Masking() throws Exception {
    final SHA256MaskFactory sha256Mask = new SHA256MaskFactory();
    final BytesColumnVector source = new BytesColumnVector();
    final BytesColumnVector target = new BytesColumnVector();

    target.reset();

    source.setRef(0, input32, 0, input32.length);
    source.setRef(1, inputShort, 0, inputShort.length);
    source.setRef(2, inputLong, 0, inputLong.length);

    for (int r = 0; r < 3; ++r) {
      sha256Mask.maskString(source, r, target,
          TypeDescription.createVarchar().withMaxLength(32));
    }

    // Hash is 64 in length greater than max len 32, so make sure output length is 32
    assertEquals(32, target.length[0]);
    assertEquals(32, target.length[1]);
    assertEquals(32, target.length[2]);

    for (int r = 0; r < 3; ++r) {
      sha256Mask.maskString(source, r, target,
          TypeDescription.createVarchar().withMaxLength(100));
    }

    /* Hash is 64 in length, less than max len 100 so the outpur will always be 64 */
    assertEquals(64, target.length[0]);
    assertEquals(64, target.length[1]);
    assertEquals(64, target.length[2]);

  }

  @Test
  public void testBinary() {
    final SHA256MaskFactory sha256Mask = new SHA256MaskFactory();
    final BytesColumnVector source = new BytesColumnVector();
    final BytesColumnVector target = new BytesColumnVector();

    target.reset();

    source.setRef(0, input32, 0, input32.length);
    source.setRef(1, inputShort, 0, inputShort.length);
    source.setRef(2, inputLong, 0, inputLong.length);

    for (int r = 0; r < 3; ++r) {
      sha256Mask.maskBinary(source, r, target);
    }

    /* Hash is 64 in length, less than max len 100 so the outpur will always be 64 */
    assertEquals(64, target.length[0]);
    assertEquals(64, target.length[1]);
    assertEquals(64, target.length[2]);

  }

}
