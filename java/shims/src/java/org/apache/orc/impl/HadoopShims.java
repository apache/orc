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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.orc.EncryptionAlgorithm;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.Key;
import java.util.List;

public interface HadoopShims {

  enum DirectCompressionType {
    NONE,
    ZLIB_NOHEADER,
    ZLIB,
    SNAPPY,
  }

  interface DirectDecompressor {
    void decompress(ByteBuffer var1, ByteBuffer var2) throws IOException;
    void reset();
    void end();
  }

  /**
   * Get a direct decompressor codec, if it is available
   * @param codec the kind of decompressor that we need
   * @return a direct decompressor or null, if it isn't available
   */
  DirectDecompressor getDirectDecompressor(DirectCompressionType codec);

  /**
   * a hadoop.io ByteBufferPool shim.
   */
  interface ByteBufferPoolShim {
    /**
     * Get a new ByteBuffer from the pool.  The pool can provide this from
     * removing a buffer from its internal cache, or by allocating a
     * new buffer.
     *
     * @param direct     Whether the buffer should be direct.
     * @param length     The minimum length the buffer will have.
     * @return           A new ByteBuffer. Its capacity can be less
     *                   than what was requested, but must be at
     *                   least 1 byte.
     */
    ByteBuffer getBuffer(boolean direct, int length);

    /**
     * Release a buffer back to the pool.
     * The pool may choose to put this buffer into its cache/free it.
     *
     * @param buffer    a direct bytebuffer
     */
    void putBuffer(ByteBuffer buffer);
  }

  /**
   * Provides an HDFS ZeroCopyReader shim.
   * @param in FSDataInputStream to read from (where the cached/mmap buffers are
   *          tied to)
   * @param pool ByteBufferPoolShim to allocate fallback buffers with
   *
   * @return returns null if not supported
   */
  ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                       ByteBufferPoolShim pool
                                       ) throws IOException;

  interface ZeroCopyReaderShim extends Closeable {

    /**
     * Get a ByteBuffer from the FSDataInputStream - this can be either a
     * HeapByteBuffer or an MappedByteBuffer. Also move the in stream by that
     * amount. The data read can be small than maxLength.
     *
     * @return ByteBuffer read from the stream,
     */
    ByteBuffer readBuffer(int maxLength,
                          boolean verifyChecksums) throws IOException;

    /**
     * Release a ByteBuffer obtained from a read on the
     * Also move the in stream by that amount. The data read can be small than
     * maxLength.
     */
    void releaseBuffer(ByteBuffer buffer);

    /**
     * Close the underlying stream.
     */
    void close() throws IOException;
  }

  /**
   * Allow block boundaries to be reached by zero-fill or variable length block
   * markers (in HDFS).
   * @return the number of bytes written
   */
  long padStreamToBlock(OutputStream output, long padding) throws IOException;

  /**
   * A source of crypto keys. This is usually backed by a Ranger KMS.
   */
  interface KeyProvider {

    /**
     * Get the list of key names from the key provider.
     * @return a list of key names
     * @throws IOException
     */
    List<String> getKeyNames() throws IOException;

    /**
     * Get the current metadata for a given key. This is used when encrypting
     * new data.
     * @param keyName the name of a key
     * @return metadata for the current version of the key
     */
    KeyMetadata getCurrentKeyVersion(String keyName) throws IOException;

    /**
     * Create a metadata object while reading.
     * @param keyName the name of the key
     * @param version the version of the key to use
     * @param algorithm the algorithm for that version of the key
     * @return the metadata for the key version
     */
    KeyMetadata getKeyVersion(String keyName, int version,
                              EncryptionAlgorithm algorithm);

    /**
     * Create a local key for the given key version and initialization vector.
     * Given a probabilistically unique iv, it will generate a unique key
     * with the master key at the specified version. This allows the encryption
     * to use this local key for the encryption and decryption without ever
     * having access to the master key.
     *
     * This uses KeyProviderCryptoExtension.decryptEncryptedKey with a fixed key
     * of the appropriate length.
     *
     * @param key the master key version
     * @param iv the unique initialization vector
     * @return the local key's material
     */
    Key getLocalKey(KeyMetadata key, byte[] iv) throws IOException;
  }

  /**
   * Information about a crypto key.
   */
  interface KeyMetadata {
    /**
     * Get the name of the key.
     */
    String getKeyName();

    /**
     * Get the encryption algorithm for this key.
     * @return the algorithm
     */
    EncryptionAlgorithm getAlgorithm();

    /**
     * Get the version of this key.
     * @return the version
     */
    int getVersion();
  }

  /**
   * Create a random key for encrypting.
   * @param conf the configuration
   * @return a key provider or null if none was provided
   */
  KeyProvider getKeyProvider(Configuration conf) throws IOException;

}
