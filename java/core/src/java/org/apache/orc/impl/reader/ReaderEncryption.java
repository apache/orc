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

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.HadoopShimsFactory;
import org.apache.orc.impl.MaskDescriptionImpl;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;

public class ReaderEncryption {
  private final HadoopShims.KeyProvider keyProvider;
  private final ReaderEncryptionKey[] keys;
  private final MaskDescriptionImpl[] masks;
  private final ReaderEncryptionVariant[] variants;
  // Mapping from each column to the next variant to try for that column.
  // A value of variants.length means no encryption
  private final ReaderEncryptionVariant[] columnVariants;

  public ReaderEncryption() throws IOException {
    this(null, null, null, null, null);
  }

  public ReaderEncryption(OrcProto.Footer footer,
                          TypeDescription schema,
                          List<StripeInformation> stripes,
                          HadoopShims.KeyProvider provider,
                          Configuration conf) throws IOException {
    if (footer == null || !footer.hasEncryption()) {
      keyProvider = null;
      keys = new ReaderEncryptionKey[0];
      masks = new MaskDescriptionImpl[0];
      variants = new ReaderEncryptionVariant[0];
      columnVariants = null;
    } else {
      keyProvider = provider != null ? provider :
          HadoopShimsFactory.get().getKeyProvider(conf, new SecureRandom());
      OrcProto.Encryption encrypt = footer.getEncryption();
      masks = new MaskDescriptionImpl[encrypt.getMaskCount()];
      for(int m=0; m < masks.length; ++m) {
        masks[m] = new MaskDescriptionImpl(m, encrypt.getMask(m));
      }
      keys = new ReaderEncryptionKey[encrypt.getKeyCount()];
      for(int k=0; k < keys.length; ++k) {
        keys[k] = new ReaderEncryptionKey(encrypt.getKey(k));
      }
      variants = new ReaderEncryptionVariant[encrypt.getVariantsCount()];
      for(int v=0; v < variants.length; ++v) {
        OrcProto.EncryptionVariant variant = encrypt.getVariants(v);
        variants[v] = new ReaderEncryptionVariant(keys[variant.getKey()], v,
            variant, schema, stripes, keyProvider);
      }
      columnVariants = new ReaderEncryptionVariant[schema.getMaximumId() + 1];
      for(int v = 0; v < variants.length; ++v) {
        TypeDescription root = variants[v].getRoot();
        for(int c = root.getId(); c <= root.getMaximumId(); ++c) {
          if (columnVariants[c] == null) {
            columnVariants[c] = variants[v];
          }
        }
      }
    }
  }

  public MaskDescriptionImpl[] getMasks() {
    return masks;
  }

  public ReaderEncryptionKey[] getKeys() {
    return keys;
  }

  public ReaderEncryptionVariant[] getVariants() {
    return variants;
  }

  /**
   * Find the next possible variant in this file for the given column.
   * @param column the column to find a variant for
   * @param lastVariant the previous variant that we looked at
   * @return the next variant or null if there are none
   */
  private ReaderEncryptionVariant findNextVariant(int column,
                                                  int lastVariant) {
    for(int v = lastVariant + 1; v < variants.length; ++v) {
      TypeDescription root = variants[v].getRoot();
      if (root.getId() <= column && column <= root.getMaximumId()) {
        return variants[v];
      }
    }
    return null;
  }

  /**
   * Get the variant for a given column that the user has access to.
   * If we haven't tried a given key, try to decrypt this variant's footer key
   * to see if the KeyProvider will give it to us. If not, continue to the
   * next variant.
   * @param column the column id
   * @return null for no encryption or the encryption variant
   */
  public ReaderEncryptionVariant getVariant(int column) throws IOException {
    if (keyProvider != null) {
      while (columnVariants[column] != null) {
        ReaderEncryptionVariant result = columnVariants[column];
        switch (result.getKeyDescription().getState()) {
        case FAILURE:
          break;
        case SUCCESS:
          return result;
        case UNTRIED:
          // try to get the footer key, to see if we have access
          if (result.getFileFooterKey() != null) {
            return result;
          }
        }
        columnVariants[column] = findNextVariant(column, result.getVariantId());
      }
    }
    return null;
  }
}
