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
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderExtension;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.orc.EncryptionAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.util.List;

/**
 * Shims for versions of Hadoop less than 2.7.
 *
 * Adds support for:
 * <ul>
 *   <li>Crypto</li>
 * </ul>
 */
public class HadoopShimsPre2_7 implements HadoopShims {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopShimsPre2_7.class);


  public DirectDecompressor getDirectDecompressor( DirectCompressionType codec) {
    return HadoopShimsPre2_6.getDecompressor(codec);
 }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    return ZeroCopyShims.getZeroCopyReader(in, pool);
  }

  @Override
  public long padStreamToBlock(OutputStream output,
                               long padding) throws IOException {
    return HadoopShimsPre2_3.padStream(output, padding);
  }

  static String buildKeyVersionName(KeyMetadata key) {
    return key.getKeyName() + "@" + key.getVersion();
  }

  /**
   * Shim implementation for Hadoop's KeyProvider API that lets applications get
   * access to encryption keys.
   */
  static class KeyProviderImpl implements KeyProvider {
    private final org.apache.hadoop.crypto.key.KeyProvider provider;

    KeyProviderImpl(Configuration conf) throws IOException {
      List<org.apache.hadoop.crypto.key.KeyProvider> result =
          KeyProviderFactory.getProviders(conf);
      if (result.size() != 1) {
        throw new IllegalArgumentException("Can't get KeyProvider for ORC" +
            " encryption. Got " + result.size() + " results.");
      }
      provider = result.get(0);
    }

    @Override
    public List<String> getKeyNames() throws IOException {
      return provider.getKeys();
    }

    @Override
    public KeyMetadata getCurrentKeyVersion(String keyName) throws IOException {
      return new KeyMetadataImpl(keyName, provider.getMetadata(keyName));
    }

    @Override
    public KeyMetadata getKeyVersion(String keyName, int version,
                                     EncryptionAlgorithm algorithm) {
      return new KeyMetadataImpl(keyName, version, algorithm);
    }

    @Override
    public Key getLocalKey(KeyMetadata key, byte[] iv) throws IOException {
      EncryptionAlgorithm algorithm = key.getAlgorithm();
      KeyProviderCryptoExtension.EncryptedKeyVersion encryptedKey =
          KeyProviderCryptoExtension.EncryptedKeyVersion.createForDecryption(
              key.getKeyName(), buildKeyVersionName(key), iv,
              algorithm.getZeroKey());
      try {
        KeyProviderCryptoExtension.KeyVersion decrypted =
            ((KeyProviderCryptoExtension.CryptoExtension) provider)
                .decryptEncryptedKey(encryptedKey);
        return new SecretKeySpec(decrypted.getMaterial(),
            algorithm.getAlgorithm());
      } catch (GeneralSecurityException e) {
        throw new IOException("Problem decrypting key " + key.getKeyName(), e);
      }
    }
  }

  static class KeyMetadataImpl implements KeyMetadata {
    private final String keyName;
    private final int version;
    private final EncryptionAlgorithm algorithm;

    KeyMetadataImpl(String keyName, KeyProviderExtension.Metadata metadata) {
      this.keyName = keyName;
      version = metadata.getVersions() - 1;
      algorithm = findAlgorithm(metadata);
    }

    KeyMetadataImpl(String keyName, int version, EncryptionAlgorithm algorithm){
      this.keyName = keyName;
      this.version = version;
      this.algorithm = algorithm;
    }

    @Override
    public String getKeyName() {
      return keyName;
    }

    @Override
    public EncryptionAlgorithm getAlgorithm() {
      return algorithm;
    }

    @Override
    public int getVersion() {
      return version;
    }

    /**
     * Find the correct algorithm based on the key's metadata.
     * @param meta the key's metadata
     * @return the correct algorithm
     */
    static EncryptionAlgorithm findAlgorithm(KeyProviderCryptoExtension.Metadata meta) {
      String cipher = meta.getCipher();
      if (cipher.startsWith("AES/")) {
        int bitLength = meta.getBitLength();
        if (bitLength == 128) {
          return EncryptionAlgorithm.AES_128;
        } else {
          if (bitLength != 256) {
            LOG.info("ORC column encryption does not support " + bitLength +
                " bit keys. Using 256 bits instead.");
          }
          return EncryptionAlgorithm.AES_256;
        }
      }
      throw new IllegalArgumentException("ORC column encryption only supports" +
          " AES and not " + cipher);
    }
  }

  @Override
  public KeyProvider getKeyProvider(Configuration conf) throws IOException {
    return new KeyProviderImpl(conf);
  }
}
