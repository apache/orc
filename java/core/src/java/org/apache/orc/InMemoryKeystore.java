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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is an in-memory implementation of {@link HadoopShims.KeyProvider}.
 * The primary use of this class is to ease testing.
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
  private final Map<String, KeyVersion> keys;

  /**
   * A map that store the keyName (without version)
   * and metadata associated with it.
   */
  private final Map<String, Deque<HadoopShims.KeyMetadata>> meta = new HashMap<>();

  private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock(true);

  /* Create an instance */
  public InMemoryKeystore() {
    this(new HashMap<String, KeyVersion>());
  }

  /**
   * Create an instance populating the given keys.
   *
   * @param keys Supplied map of keys
   */
  public InMemoryKeystore(final Map<String, KeyVersion> keys) {
    super();
    this.keys = keys;
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
   * @throws IOException
   */
  @Override
  public List<String> getKeyNames() throws IOException {
    return new ArrayList<>(meta.keySet());
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
    return meta.get(keyName).peekFirst();
  }

  /**
   * Return all the versions for a given key
   *
   * @param name
   * @return
   * @throws IOException
   */
  public List<KeyVersion> getKeyVersions(final String name) throws IOException {
    rwl.readLock().lock();
    try {
      final List<KeyVersion> list = new ArrayList<>();
      for (final HadoopShims.KeyMetadata metaData : meta.get(name)) {
        list.add(keys.get(
            buildVersionName(metaData.getKeyName(), metaData.getVersion())));
      }
      return list;
    } finally {
      rwl.readLock().unlock();
    }
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

    rwl.readLock().lock();
    try {
      final KeyVersion secret = keys
          .get(buildVersionName(key.getKeyName(), key.getVersion()));
      final Cipher cipher = secret.getAlgorithm().createCipher();

      try {
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(secret.getMaterial(),
            secret.getAlgorithm().getAlgorithm()), new IvParameterSpec(iv));
      } catch (final InvalidKeyException e) {
        throw new IllegalStateException(
            "ORC bad encryption key for " + key.getKeyName(), e);
      } catch (final InvalidAlgorithmParameterException e) {
        throw new IllegalStateException(
            "ORC bad encryption parameter for " + key.getKeyName(), e);
      }

      final byte[] buffer = new byte[secret.getAlgorithm().keyLength()];

      try {
        cipher.doFinal(buffer);
      } catch (final IllegalBlockSizeException e) {
        throw new IllegalStateException(
            "ORC bad block size for " + key.getKeyName(), e);
      } catch (final BadPaddingException e) {
        throw new IllegalStateException(
            "ORC bad padding for " + key.getKeyName(), e);
      }

      return new SecretKeySpec(buffer, secret.getAlgorithm().getAlgorithm());
    } finally {
      rwl.readLock().unlock();
    }

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
   * @param version   Key Version
   * @param masterKey Master key
   * @return The new key
   */
  public KeyVersion addKey(String keyName, EncryptionAlgorithm algorithm,
      int version, byte[] masterKey) throws IOException {

    /* Test weather platform supports the algorithm */
    if (!SUPPORTS_AES_256 && (algorithm != EncryptionAlgorithm.AES_128)) {
      algorithm = EncryptionAlgorithm.AES_128;
    }

    rwl.writeLock().lock();
    try {
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
      if (meta.get(keyName) != null && meta.get(keyName).peekFirst() != null
          && meta.get(keyName).peekFirst().getVersion() >= version) {
        throw new IOException(String
            .format("Key %s with equal or higher version %d already exists",
                keyName, version));
      }

      keys.put(buildVersionName(keyName, version), key);
      /* our metadata also contains key material, but it should be fine for testing */
      if (meta.containsKey(keyName)) {
        meta.get(keyName).addFirst(key);
      } else {
        final Deque<HadoopShims.KeyMetadata> stack = new ArrayDeque<>();
        stack.addFirst(key);
        meta.put(keyName, stack);
      }

      return key;

    } finally {
      rwl.writeLock().unlock();
    }

  }

  /**
   * Roll new version for the key i.e.
   * increments the version number by 1 and
   * uses the provided secret material.
   * <p>
   * Mainly used for testing.
   *
   * @param name     name of the key to be rolled
   * @param material the new material to be used
   * @return KeyVersion The new rolled key
   * @throws IOException
   */
  public KeyVersion rollNewVersion(final String name, final byte[] material)
      throws IOException {

    final HadoopShims.KeyMetadata metadata = meta.get(name).peekFirst();
    final int newVersion = metadata.getVersion() + 1;
    return addKey(name, metadata.getAlgorithm(), newVersion, material);

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

