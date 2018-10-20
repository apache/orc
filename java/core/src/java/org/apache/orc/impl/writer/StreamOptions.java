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
package org.apache.orc.impl.writer;

import org.apache.orc.CompressionCodec;
import org.apache.orc.EncryptionAlgorithm;

import java.security.Key;

/**
 * The compression and encryption options for writing a stream.
 */
public class StreamOptions {
  private CompressionCodec codec;
  private CompressionCodec.Options options;
  private final int bufferSize;
  private EncryptionAlgorithm algorithm;
  private Key key;
  private byte[] iv;

  /**
   * An option object with the given buffer size set.
   * @param bufferSize the size of the buffers.
   */
  public StreamOptions(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  /**
   * Compress using the given codec.
   * @param codec the codec to compress with
   * @return this
   */
  public StreamOptions withCodec(CompressionCodec codec,
                                 CompressionCodec.Options options) {
    this.codec = codec;
    this.options = options;
    return this;
  }

  public StreamOptions withEncryption(EncryptionAlgorithm algorithm,
                                      Key key,
                                      byte[] iv) {
    this.algorithm = algorithm;
    this.key = key;
    this.iv = iv;
    return this;
  }

  public CompressionCodec getCodec() {
    return codec;
  }

  public CompressionCodec.Options getCodecOptions() {
    return options;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public boolean isEncrypted() {
    return key != null;
  }

  public Key getKey() {
    return key;
  }

  public EncryptionAlgorithm getAlgorithm() {
    return algorithm;
  }

  public byte[] getIv() {
    return iv;
  }
}
