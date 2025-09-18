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

import org.apache.orc.CompressionCodec;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface DirectDecompressionCodec extends CompressionCodec {
  /**
   * Check if the codec is available.
   * @return true if the codec is available
   */
  boolean isAvailable();

  /**
   * Decompress the in buffer to the out buffer.
   * @param in the bytes to decompress
   * @param out the decompressed bytes
   * @throws IOException if there is an error
   */
  void directDecompress(ByteBuffer in, ByteBuffer out) throws IOException;
}
