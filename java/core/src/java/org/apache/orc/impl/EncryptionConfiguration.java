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


import org.apache.orc.DataMask;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.writer.WriterEncryptionKey;
import org.apache.orc.impl.writer.WriterEncryptionVariant;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Configuration class about the column encryption.
 */
public class EncryptionConfiguration {

  // the list of maskDescriptions, keys, and variants
  private final SortedMap<String, MaskDescriptionImpl> maskDescriptions = new TreeMap<>();
  private final SortedMap<String, WriterEncryptionKey> keys = new TreeMap<>();
  private final WriterEncryptionVariant[] encryption;
  // the mapping of columns to maskDescriptions
  private final MaskDescriptionImpl[] columnMaskDescriptions;
  // the mapping of columns to EncryptionVariants
  private final WriterEncryptionVariant[] columnEncryption;
  private KeyProvider keyProvider;

  private final OrcFile.WriterOptions opts;

  private final TypeDescription schema;

  public EncryptionConfiguration(OrcFile.WriterOptions opts, TypeDescription schema
                                ) throws IOException {
    this.opts = opts;
    this.schema = schema;

    int numColumns = schema.getMaximumId() + 1;
    columnEncryption = new WriterEncryptionVariant[numColumns];
    columnMaskDescriptions = new MaskDescriptionImpl[numColumns];
    encryption = setupEncryption(opts.getKeyProvider(), schema, opts.getKeyOverrides());
  }

  private WriterEncryptionKey getKey(String keyName,
                                   KeyProvider provider) throws IOException {
    WriterEncryptionKey result = keys.get(keyName);
    if (result == null) {
      result = new WriterEncryptionKey(provider.getCurrentKeyVersion(keyName));
      keys.put(keyName, result);
    }
    return result;
  }

  private MaskDescriptionImpl getMask(String maskString) {
    // if it is already there, get the earlier object
    MaskDescriptionImpl result = maskDescriptions.get(maskString);
    if (result == null) {
      result = ParserUtils.buildMaskDescription(maskString);
      maskDescriptions.put(maskString, result);
    }
    return result;
  }

  private int visitTypeTree(TypeDescription schema,
                          boolean encrypted,
                          KeyProvider provider) throws IOException {
    int result = 0;
    String keyName = schema.getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE);
    String maskName = schema.getAttributeValue(TypeDescription.MASK_ATTRIBUTE);
    if (keyName != null) {
      if (provider == null) {
        throw new IllegalArgumentException("Encryption requires a KeyProvider.");
      }
      if (encrypted) {
        throw new IllegalArgumentException("Nested encryption type: " + schema);
      }
      encrypted = true;
      result += 1;
      WriterEncryptionKey key = getKey(keyName, provider);
      HadoopShims.KeyMetadata metadata = key.getMetadata();
      WriterEncryptionVariant variant = new WriterEncryptionVariant(key,
              schema, provider.createLocalKey(metadata));
      key.addRoot(variant);
    }
    if (encrypted && (keyName != null || maskName != null)) {
      MaskDescriptionImpl mask = getMask(maskName == null ? "nullify" : maskName);
      mask.addColumn(schema);
    }
    List<TypeDescription> children = schema.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        result += visitTypeTree(child, encrypted, provider);
      }
    }
    return result;
  }

  /**
   * Iterate through the encryption options given by the user and set up
   * our data structures.
   *
   * @param provider the KeyProvider to use to generate keys
   * @param schema the type tree that we search for annotations
   * @param keyOverrides user specified key overrides
   */
  private WriterEncryptionVariant[] setupEncryption(
        KeyProvider provider,
        TypeDescription schema,
        Map<String, HadoopShims.KeyMetadata> keyOverrides) throws IOException {
    keyProvider = provider != null ? provider :
            CryptoUtils.getKeyProvider(opts.getConfiguration(), new SecureRandom());
    // Load the overrides into the cache so that we use the required key versions.
    for (HadoopShims.KeyMetadata key : keyOverrides.values()) {
      keys.put(key.getKeyName(), new WriterEncryptionKey(key));
    }
    int variantCount = visitTypeTree(schema, false, keyProvider);

    // Now that we have de-duped the keys and maskDescriptions, make the arrays
    int nextId = 0;
    if (variantCount > 0) {
      for (MaskDescriptionImpl mask : maskDescriptions.values()) {
        mask.setId(nextId++);
        for (TypeDescription column : mask.getColumns()) {
          this.columnMaskDescriptions[column.getId()] = mask;
        }
      }
    }
    nextId = 0;
    int nextVariantId = 0;
    WriterEncryptionVariant[] result = new WriterEncryptionVariant[variantCount];
    for (WriterEncryptionKey key : keys.values()) {
      key.setId(nextId++);
      key.sortRoots();
      for (WriterEncryptionVariant variant : key.getEncryptionRoots()) {
        result[nextVariantId] = variant;
        columnEncryption[variant.getRoot().getId()] = variant;
        variant.setId(nextVariantId++);
      }
    }
    return result;
  }

  /**
   * Determine whether the encryption configuration is empty.
   * @return true if the column encryption exists, else false.
   */
  public boolean hasColumnEncryption() {
    return encryption.length > 0;
  }

  /**
   * Get the encryption for the given column.
   * @param columnId the root column id
   * @return the column encryption or null if it isn't encrypted
   */
  public WriterEncryptionVariant getEncryption(int columnId) {
    return columnId < columnEncryption.length ? columnEncryption[columnId] : null;
  }

  /**
   * Get the mask for the unencrypted variant.
   * @param columnId the column id
   * @return the mask to apply to the unencrypted data or null if there is none
   */
  public DataMask getUnencryptedMask(int columnId) {
    if (columnMaskDescriptions != null) {
      MaskDescriptionImpl descr = columnMaskDescriptions[columnId];
      if (descr != null) {
        return DataMask.Factory.build(descr, schema.findSubtype(columnId),
                  (type) -> columnMaskDescriptions[type.getId()]);
      }
    }
    return null;
  }

  public SortedMap<String, MaskDescriptionImpl> getMaskDescriptions() {
    return maskDescriptions;
  }

  public SortedMap<String, WriterEncryptionKey> getKeys() {
    return keys;
  }

  public WriterEncryptionVariant[] getEncryption() {
    return encryption;
  }

  public KeyProvider getKeyProvider() {
    return keyProvider;
  }
}
