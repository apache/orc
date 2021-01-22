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

import java.io.OutputStream;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import org.apache.orc.EncryptionAlgorithm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.orc.shims.Configuration;
import org.apache.orc.shims.FileIO;
import org.apache.orc.shims.SeekableInputStream;


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
   * @param in SeekableInputStream to read from (where the cached/mmap buffers are
   *          tied to)
   * @param pool ByteBufferPoolShim to allocate fallback buffers with
   *
   * @return returns null if not supported
   */
  ZeroCopyReaderShim getZeroCopyReader(SeekableInputStream in,
                                       ByteBufferPoolShim pool
                                       ) throws IOException;

  interface ZeroCopyReaderShim extends AutoCloseable, Closeable {

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
     * Release a ByteBuffer obtained from a readBuffer on this
     * ZeroCopyReaderShim.
     */
    void releaseBuffer(ByteBuffer buffer);

    /**
     * Close the underlying stream.
     */
    @Override
    void close() throws IOException;
  }

  /**
   * End the OutputStream's current block at the current location.
   * This is only available on HDFS on Hadoop &ge; 2.7, but will return false
   * otherwise.
   * @return was a variable length block created?
   */
  boolean endVariableLengthBlock(OutputStream output) throws IOException;

  /**
   * The known KeyProviders for column encryption.
   * These are identical to OrcProto.KeyProviderKind.
   */
  enum KeyProviderKind {
    UNKNOWN(0),
    HADOOP(1),
    AWS(2),
    GCP(3),
    AZURE(4);

    private final int value;

    KeyProviderKind(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * Information about a crypto key including the key name, version, and the
   * algorithm.
   */
  class KeyMetadata {
    private final String keyName;
    private final int version;
    private final EncryptionAlgorithm algorithm;

    public KeyMetadata(String key, int version, EncryptionAlgorithm algorithm) {
      this.keyName = key;
      this.version = version;
      this.algorithm = algorithm;
    }

    /**
     * Get the name of the key.
     */
    public String getKeyName() {
      return keyName;
    }

    /**
     * Get the encryption algorithm for this key.
     * @return the algorithm
     */
    public EncryptionAlgorithm getAlgorithm() {
      return algorithm;
    }

    /**
     * Get the version of this key.
     * @return the version
     */
    public int getVersion() {
      return version;
    }

    @Override
    public String toString() {
      return keyName + '@' + version + ' ' + algorithm;
    }
  }

  /**
   * Create a KeyProvider to get encryption keys.
   * The checks the factories using the service provider APIs. The
   * built in factories are:
   * <ul>
   *   <li>HadoopKeyProviderFactory (after Hadoop 2.6)</li>
   *   <li>CoreKeyProviderFactory</li>
   * </ul>
   * @param kind the name of the provider desired
   * @param conf the configuration
   * @param random a secure random number generator
   * @return a key provider
   */
  default KeyProvider getKeyProvider(String kind,
                                     Configuration conf,
                                     Random random) throws IOException {
    ServiceLoader<KeyProvider.FactoryCore> loader =
        ServiceLoader.load(KeyProvider.FactoryCore.class);
    for (KeyProvider.FactoryCore factory : loader) {
      KeyProvider result = factory.create(kind, conf, random);
      if (result != null) {
        return result;
      }
    }
    return new NullKeyProvider();
  }

  /**
   * Create a FileIO object to read/write files.
   * @param fileSystem The supplier for the file system or null for the default
   * @return a FileIO object for a given file system
   */
  FileIO createFileIO(Supplier<Object> fileSystem);

  /**
   * Create a FileIO object to read or write files at a given path.
   * @param path The path that we need
   * @param conf The configuration for the FileIO
   * @return a FileIO object
   */
  FileIO createFileIO(String path, Configuration conf) throws IOException;

  /**
   * Create a configuration object.
   * @param tableProperties the table properties
   * @param hadoopConfig a Hadoop Configuration or null if one is not available
   * @return the shim configuration object
   */
  Configuration createConfiguration(Properties tableProperties, Object hadoopConfig);
}
