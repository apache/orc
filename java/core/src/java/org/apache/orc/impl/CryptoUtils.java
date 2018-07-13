/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.orc.EncryptionAlgorithm;
import java.security.SecureRandom;

/**
 * This class has routines to work with encryption within ORC files.
 */
public class CryptoUtils {

  private static final int COLUMN_ID_LENGTH = 3;
  private static final int KIND_LENGTH = 2;
  private static final int STRIPE_ID_LENGTH = 3;
  private static final int MIN_COUNT_BYTES = 8;

  static final int MAX_COLUMN = 0xffffff;
  static final int MAX_KIND = 0xffff;
  static final int MAX_STRIPE = 0xffffff;

  /**
   * Create a unique IV for each stream within a single key.
   * The top bytes are set with the column, stream kind, and stripe id and the
   * lower 8 bytes are always 0.
   * @param name the stream name
   * @param stripeId the stripe id
   * @return the iv for the stream
   */
  public static byte[] createIvForStream(EncryptionAlgorithm algorithm,
                                         StreamName name,
                                         int stripeId) {
    byte[] iv = new byte[algorithm.getIvLength()];
    int columnId = name.getColumn();
    if (columnId < 0 || columnId > MAX_COLUMN) {
      throw new IllegalArgumentException("ORC encryption is limited to " +
          MAX_COLUMN + " columns. Value = " + columnId);
    }
    int k = name.getKind().getNumber();
    if (k < 0 || k > MAX_KIND) {
      throw new IllegalArgumentException("ORC encryption is limited to " +
          MAX_KIND + " stream kinds. Value = " + k);
    }
    if (stripeId < 0 || stripeId > MAX_STRIPE){
      throw new IllegalArgumentException("ORC encryption is limited to " +
          MAX_STRIPE + " stripes. Value = " + stripeId);
    }
    // the rest of the iv is used for counting within the stream
    if (iv.length - (COLUMN_ID_LENGTH + KIND_LENGTH + STRIPE_ID_LENGTH) < MIN_COUNT_BYTES) {
      throw new IllegalArgumentException("Not enough space in the iv for the count");
    }
    iv[0] = (byte)(columnId >> 16);
    iv[1] = (byte)(columnId >> 8);
    iv[2] = (byte)columnId;
    iv[COLUMN_ID_LENGTH] = (byte)(k >> 8);
    iv[COLUMN_ID_LENGTH+1] = (byte)(k);
    iv[COLUMN_ID_LENGTH+KIND_LENGTH] = (byte)(stripeId >> 16);
    iv[COLUMN_ID_LENGTH+KIND_LENGTH+1] = (byte)(stripeId >> 8);
    iv[COLUMN_ID_LENGTH+KIND_LENGTH+2] = (byte)stripeId;
    return iv;
  }
}
