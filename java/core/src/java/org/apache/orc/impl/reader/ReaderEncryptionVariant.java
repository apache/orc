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

package org.apache.orc.impl.reader;

import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.EncryptionVariant;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.LocalKey;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Key;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Information about an encrypted column.
 */
public class ReaderEncryptionVariant implements EncryptionVariant {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReaderEncryptionVariant.class);
  private final HadoopShims.KeyProvider provider;
  private final ReaderEncryptionKey key;
  private final TypeDescription column;
  private final int variantId;
  private final LocalKey[] localKeys;
  private final LocalKey footerKey;

  /**
   * Create a reader's view of an encryption variant.
   * @param key the encryption key description
   * @param variantId the of of the variant (0..N-1)
   * @param proto the serialized description of the variant
   * @param schema the file schema
   * @param stripes the stripe information
   * @param provider the key provider
   */
  public ReaderEncryptionVariant(ReaderEncryptionKey key,
                                 int variantId,
                                 OrcProto.EncryptionVariant proto,
                                 TypeDescription schema,
                                 List<StripeInformation> stripes,
                                 HadoopShims.KeyProvider provider) {
    this.key = key;
    this.variantId = variantId;
    this.provider = provider;
    this.column = proto.hasRoot() ? schema.findSubtype(proto.getRoot()) : null;
    this.localKeys = new LocalKey[stripes.size()];
    HashMap<BytesWritable, LocalKey> cache = new HashMap<>();
    for(int s=0; s < localKeys.length; ++s) {
      StripeInformation stripe = stripes.get(s);
      localKeys[s] = getCachedKey(cache, key.getAlgorithm(),
          stripe.getEncryptedLocalKeys()[variantId]);
    }
    if (proto.hasEncryptedKey()) {
      footerKey = getCachedKey(cache, key.getAlgorithm(),
          proto.getEncryptedKey().toByteArray());
    } else {
      footerKey = null;
    }
    key.addVariant(this);
  }

  @Override
  public ReaderEncryptionKey getKeyDescription() {
    return key;
  }

  @Override
  public TypeDescription getRoot() {
    return column;
  }

  @Override
  public int getVariantId() {
    return variantId;
  }

  /**
   * Deduplicate the local keys so that we only decrypt each local key once.
   * @param cache the cache to use
   * @param encrypted the encrypted key
   * @return the local key
   */
  private static LocalKey getCachedKey(Map<BytesWritable, LocalKey> cache,
                                       EncryptionAlgorithm algorithm,
                                       byte[] encrypted) {
    // wrap byte array in BytesWritable to get equality and hash
    BytesWritable wrap = new BytesWritable(encrypted);
    LocalKey result = cache.get(wrap);
    if (result == null) {
      result = new LocalKey(algorithm, null, encrypted);
      cache.put(wrap, result);
    }
    return result;
  }

  private Key getDecryptedKey(LocalKey localKey) throws IOException {
    Key result = localKey.getDecryptedKey();
    if (result == null) {
      switch (this.key.getState()) {
      case UNTRIED:
        try {
          result = provider.decryptLocalKey(key.getMetadata(),
              localKey.getEncryptedKey());
        } catch (IOException ioe) {
          LOG.info("Can't decrypt using key {}", key);
        }
        if (result != null) {
          localKey.setDecryptedKey(result);
          key.setSucess();
        } else {
          key.setFailure();
        }
        break;
      case SUCCESS:
        result = provider.decryptLocalKey(key.getMetadata(),
            localKey.getEncryptedKey());
        if (result == null) {
          throw new IOException("Can't decrypt local key " + key);
        }
        localKey.setDecryptedKey(result);
        break;
      case FAILURE:
        return null;
      }
    }
    return result;
  }

  @Override
  public Key getFileFooterKey() throws IOException {
    return getDecryptedKey(footerKey);
  }

  @Override
  public Key getStripeKey(long stripe) throws IOException {
    return getDecryptedKey(localKeys[(int) stripe]);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    } else {
      return compareTo((EncryptionVariant) other) == 0;
    }
  }

  @Override
  public int hashCode() {
    return key.hashCode() * 127 + column.getId();
  }

  @Override
  public int compareTo(@NotNull EncryptionVariant other) {
    if (other == this) {
      return 0;
    } else if (key == other.getKeyDescription()) {
      return Integer.compare(column.getId(), other.getRoot().getId());
    } else if (key == null) {
      return -1;
    } else {
      return key.compareTo(other.getKeyDescription());
    }
  }
}
