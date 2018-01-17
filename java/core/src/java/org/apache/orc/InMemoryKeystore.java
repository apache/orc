/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc;

import org.apache.orc.impl.HadoopShims;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This is an in-memory implementation of {@link HadoopShims.KeyProvider}.
 *
 * The primary use of this class is for when the user doesn't have a
 * Hadoop KMS running and wishes to use encryption. It is also useful for
 * testing.
 *
 * This class is not thread safe.
 */
public class InMemoryKeystore implements HadoopShims.KeyProvider {

  /**
   * Support AES 256 ?
   */
  public static final boolean SUPPORTS_AES_256;

  static {
    try {
      SUPPORTS_AES_256 = Cipher.getMaxAllowedKeyLength("AES") > 128;
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("Unknown algorithm", e);
    }
  }

  /**
   * A map that stores the 'keyName@version'
   * and 'metadata + material' mapping.
   */
  private final TreeMap<String, KeyVersion> keys = new TreeMap<>();

  /**
   * A map from the keyName (without version) to the currentVersion.
   */
  private final Map<String, Integer> currentVersion = new HashMap<>();

  /* Create an instance */
  public InMemoryKeystore() {
    super();
  }

  /**
   * Create an instance populating the given keys.
   *
   * @param keys a list of keys that will be added initially
   */
  public InMemoryKeystore(final Map<String, KeyVersion> keys) {
    super();
    this.keys.putAll(keys);
  }

  /**
   * Build a version string from a basename and version number. Converts
   * "/aaa/bbb" and 3 to "/aaa/bbb@3".
   *
   * @param name    the basename of the key
   * @param version the version of the key
   * @return the versionName of the key.
   */
  protected static String buildVersionName(final String name,
      final int version) {
    return name + "@" + version;
  }

  /**
   * Get the list of key names from the key provider.
   *
   * @return a list of key names
   */
  @Override
  public List<String> getKeyNames() {
    return new ArrayList<>(currentVersion.keySet());
  }

  /**
   * Get the current metadata for a given key. This is used when encrypting
   * new data.
   *
   * @param keyName the name of a key
   * @return metadata for the current version of the key
   */
  @Override
  public HadoopShims.KeyMetadata getCurrentKeyVersion(final String keyName) {
    return keys.get(buildVersionName(keyName, currentVersion.get(keyName)));
  }

  /**
   * Create a metadata object while reading.
   *
   * @param keyName   the name of the key
   * @param version   the version of the key to use
   * @param algorithm the algorithm for that version of the key
   * @return the metadata for the key version
   */
  @Override
  public HadoopShims.KeyMetadata getKeyVersion(final String keyName,
      final int version, final EncryptionAlgorithm algorithm) {
    return new HadoopShims.KeyMetadata() {

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
    };
  }

  /**
   * Create a local key for the given key version and initialization vector.
   * Given a probabilistically unique iv, it will generate a unique key
   * with the master key at the specified version. This allows the encryption
   * to use this local key for the encryption and decryption without ever
   * having access to the master key.
   * <p>
   * This uses KeyProviderCryptoExtension.decryptEncryptedKey with a fixed key
   * of the appropriate length.
   *
   * @param key the master key version
   * @param iv  the unique initialization vector
   * @return the local key's material
   */
  @Override
  public Key getLocalKey(final HadoopShims.KeyMetadata key, final byte[] iv) {

    final KeyVersion secret = keys
        .get(buildVersionName(key.getKeyName(), key.getVersion()));
    final EncryptionAlgorithm algorithm = secret.getAlgorithm();
    final Cipher cipher = algorithm.createCipher();

    try {
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(secret.getMaterial(),
          algorithm.getAlgorithm()), new IvParameterSpec(iv));
    } catch (final InvalidKeyException e) {
      throw new IllegalStateException(
          "ORC bad encryption key for " + key.getKeyName(), e);
    } catch (final InvalidAlgorithmParameterException e) {
      throw new IllegalStateException(
          "ORC bad encryption parameter for " + key.getKeyName(), e);
    }

    byte[] buffer = new byte[algorithm.keyLength()];

    try {
      buffer = cipher.doFinal(buffer);
    } catch (final IllegalBlockSizeException e) {
      throw new IllegalStateException(
          "ORC bad block size for " + key.getKeyName(), e);
    } catch (final BadPaddingException e) {
      throw new IllegalStateException(
          "ORC bad padding for " + key.getKeyName(), e);
    }

    return new SecretKeySpec(buffer, algorithm.getAlgorithm());
  }

  /**
   * Function that takes care of adding a new key.<br>
   * A new key can be added only if:
   * <ul>
   * <li>This is a new key and no prior key version exist.</li>
   * <li>If the key exists (has versions), then the new version to be added should be greater than
   * the version that already exists.</li>
   * </ul>
   *
   * @param keyName   Name of the key to be added
   * @param algorithm Algorithm used
   * @param masterKey Master key
   * @return this
   */
  public InMemoryKeystore addKey(String keyName, EncryptionAlgorithm algorithm,
                                 byte[] masterKey) throws IOException {
    return addKey(keyName, 0, algorithm, masterKey);
  }

    /**
     * Function that takes care of adding a new key.<br>
     * A new key can be added only if:
     * <ul>
     * <li>This is a new key and no prior key version exist.</li>
     * <li>If the key exists (has versions), then the new version to be added should be greater than
     * the version that already exists.</li>
     * </ul>
     *
     * @param keyName   Name of the key to be added
     * @param version   Key Version
     * @param algorithm Algorithm used
     * @param masterKey Master key
     * @return this
     */
  public InMemoryKeystore addKey(String keyName, int version,
                                 EncryptionAlgorithm algorithm,
                                 byte[] masterKey) throws IOException {

    /* Test weather platform supports the algorithm */
    if (!SUPPORTS_AES_256 && (algorithm != EncryptionAlgorithm.AES_128)) {
      algorithm = EncryptionAlgorithm.AES_128;
    }

    final byte[] buffer = new byte[algorithm.keyLength()];
    if (algorithm.keyLength() > masterKey.length) {

      System.arraycopy(masterKey, 0, buffer, 0, masterKey.length);
      /* fill with zeros */
      Arrays.fill(buffer, masterKey.length, buffer.length - 1, (byte) 0);

    } else {
      System.arraycopy(masterKey, 0, buffer, 0, algorithm.keyLength());
    }

    final KeyVersion key = new KeyVersion(keyName, version, algorithm,
        buffer);

    /* Check whether the key is already present and has a smaller version */
    if (currentVersion.get(keyName) != null &&
        currentVersion.get(keyName) >= version) {
      throw new IOException(String
          .format("Key %s with equal or higher version %d already exists",
              keyName, version));
    }

    keys.put(buildVersionName(keyName, version), key);
    currentVersion.put(keyName, version);
    return this;
  }

  /**
   * This class contains the meta-data and the material for the key.
   */
  static class KeyVersion implements HadoopShims.KeyMetadata {

    private final String keyName;
    private final int version;
    private final EncryptionAlgorithm algorithm;
    private final byte[] material;

    public KeyVersion(final String keyName, final int version,
        final EncryptionAlgorithm algorithm, final byte[] material) {
      super();
      this.keyName = keyName;
      this.version = version;
      this.algorithm = algorithm;
      this.material = material;
    }

    /**
     * Get the name of the key.
     */
    @Override
    public String getKeyName() {
      return keyName;
    }

    /**
     * Get the encryption algorithm for this key.
     *
     * @return the algorithm
     */
    @Override
    public EncryptionAlgorithm getAlgorithm() {
      return algorithm;
    }

    /**
     * Get the version of this key.
     *
     * @return the version
     */
    @Override
    public int getVersion() {
      return version;
    }

    /**
     * Get the material for the key
     *
     * @return the material
     */
    public byte[] getMaterial() {
      return material;
    }

  }

}

