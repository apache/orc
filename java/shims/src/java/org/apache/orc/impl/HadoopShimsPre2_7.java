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
import java.util.Random;

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
  public boolean endVariableLengthBlock(OutputStream output) {
    return false;
  }

  static String buildKeyVersionName(KeyMetadata key) {
    return key.getKeyName() + "@" + key.getVersion();
  }

  /**
   * Shim implementation for ORC's KeyProvider API that uses Hadoop's
   * KeyProvider API and implementations. Most users use a Hadoop or Ranger
   * KMS and thus should use this default implementation.
   *
   * The main two methods of ORC's KeyProvider are createLocalKey and
   * decryptLocalKey. These are very similar to Hadoop's
   * <pre>
   *   EncryptedKeyVersion generateEncryptedKey(String keyVersionName);
   *   KeyVersion decryptEncryptedKey(EncryptedKeyVersion encrypted)
   * </pre>
   * but there are some important differences.
   * <ul>
   * <li>Hadoop's generateEncryptedKey doesn't return the decrypted key, so it
   *     would require two round trips (generateEncryptedKey and then
   *     decryptEncryptedKey)to the KMS.</li>
   * <li>Hadoop's methods require storing both the IV and the encrypted key, so
   *     for AES256, it is 48 random bytes.</li>
   * </ul>
   *
   * However, since the encryption in the KMS is using AES/CTR we know that the
   * flow is:
   *
   * <pre>
   *   tmpKey = aes(masterKey, iv);
   *   cypher = xor(tmpKey, plain);
   * </pre>
   *
   * which means that encryption and decryption are symmetric. Therefore, if we
   * use the KMS' decryptEncryptedKey, and feed in a random iv and the right
   * number of 0's as the encrypted key, we get the right length of a tmpKey.
   * Since it is symmetric, we can use it for both encryption and decryption
   * and we only need to store the random iv. Since the iv is 16 bytes, it is
   * only a third the size of the other solution, and only requires one trip to
   * the KMS.
   *
   * So the flow looks like:
   * <pre>
   *   encryptedKey = securely random 16 or 32 bytes
   *   iv = first 16 byte of encryptedKey
   *   --- on KMS ---
   *   tmpKey0 = aes(masterKey, iv)
   *   tmpKey1 = aes(masterKey, iv+1)
   *   decryptedKey0 = xor(tmpKey0, encryptedKey0)
   *   decryptedKey1 = xor(tmpKey1, encryptedKey1)
   * </pre>
   *
   * In the long term, we should probably fix Hadoop's generateEncryptedKey
   * to either take the random key or pass it back.
   */
  static class KeyProviderImpl implements KeyProvider {
    private final org.apache.hadoop.crypto.key.KeyProvider provider;
    private final Random random;

    KeyProviderImpl(org.apache.hadoop.crypto.key.KeyProvider provider,
                    Random random) {
      this.provider = provider;
      this.random = random;
    }

    @Override
    public List<String> getKeyNames() throws IOException {
      return provider.getKeys();
    }

    @Override
    public KeyMetadata getCurrentKeyVersion(String keyName) throws IOException {
      org.apache.hadoop.crypto.key.KeyProvider.Metadata meta =
          provider.getMetadata(keyName);
      return new KeyMetadata(keyName, meta.getVersions() - 1,
          findAlgorithm(meta));
    }

    /**
     * The Ranger/Hadoop KMS mangles the IV by bit flipping it in a misguided
     * attempt to improve security. By bit flipping it here, we undo the
     * silliness so that we get
     * @param input the input array to copy from
     * @param output the output array to write to
     */
    private static void unmangleIv(byte[] input, byte[] output) {
      for(int i=0; i < output.length && i < input.length; ++i) {
        output[i] = (byte) (0xff ^ input[i]);
      }
    }

    @Override
    public LocalKey createLocalKey(KeyMetadata key) throws IOException {
      EncryptionAlgorithm algorithm = key.getAlgorithm();
      byte[] encryptedKey = new byte[algorithm.keyLength()];
      random.nextBytes(encryptedKey);
      byte[] iv = new byte[algorithm.getIvLength()];
      unmangleIv(encryptedKey, iv);
      KeyProviderCryptoExtension.EncryptedKeyVersion param =
          KeyProviderCryptoExtension.EncryptedKeyVersion.createForDecryption(
              key.getKeyName(), buildKeyVersionName(key), iv, encryptedKey);
      try {
        KeyProviderCryptoExtension.KeyVersion decryptedKey =
            ((KeyProviderCryptoExtension) provider)
                .decryptEncryptedKey(param);
        return new LocalKey(algorithm, decryptedKey.getMaterial(),
                            encryptedKey);
      } catch (GeneralSecurityException e) {
        throw new IOException("Can't create local encryption key for " + key, e);
      }
    }

    @Override
    public Key decryptLocalKey(KeyMetadata key,
                               byte[] encryptedKey) throws IOException {
      EncryptionAlgorithm algorithm = key.getAlgorithm();
      byte[] iv = new byte[algorithm.getIvLength()];
      unmangleIv(encryptedKey, iv);
      KeyProviderCryptoExtension.EncryptedKeyVersion param =
          KeyProviderCryptoExtension.EncryptedKeyVersion.createForDecryption(
              key.getKeyName(), buildKeyVersionName(key), iv, encryptedKey);
      try {
        KeyProviderCryptoExtension.KeyVersion decrypted =
            ((KeyProviderCryptoExtension) provider)
                .decryptEncryptedKey(param);
        return new SecretKeySpec(decrypted.getMaterial(),
            algorithm.getAlgorithm());
      } catch (GeneralSecurityException e) {
        return null;
      }
    }

    @Override
    public HadoopShims.KeyProviderKind getKind() {
      return HadoopShims.KeyProviderKind.HADOOP;
    }
  }

  static KeyProvider createKeyProvider(Configuration conf,
                                       Random random) throws IOException {
    List<org.apache.hadoop.crypto.key.KeyProvider> result =
        KeyProviderFactory.getProviders(conf);
    if (result.size() == 0) {
      LOG.info("Can't get KeyProvider for ORC encryption from" +
          " hadoop.security.key.provider.path.");
      return new HadoopShimsPre2_3.NullKeyProvider();
    } else {
      return new KeyProviderImpl(result.get(0), random);
    }
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
        return EncryptionAlgorithm.AES_CTR_128;
      } else {
        if (bitLength != 256) {
          LOG.info("ORC column encryption does not support " + bitLength +
              " bit keys. Using 256 bits instead.");
        }
        return EncryptionAlgorithm.AES_CTR_256;
      }
    }
    throw new IllegalArgumentException("ORC column encryption only supports" +
        " AES and not " + cipher);
  }

  @Override
  public KeyProvider getHadoopKeyProvider(Configuration conf,
                                          Random random) throws IOException {
    return createKeyProvider(conf, random);
  }
}
