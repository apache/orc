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
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.EncryptionAlgorithm;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.security.Key;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class TestHadoopShimsPre2_7 {

  @Test(expected = IllegalArgumentException.class)
  public void testFindingUnknownEncryption() {
    KeyProvider.Metadata meta = new KMSClientProvider.KMSMetadata(
        "XXX/CTR/NoPadding", 128, "", new HashMap<String, String>(),
        new Date(0), 1);
    HadoopShimsPre2_7.findAlgorithm(meta);
  }

  @Test
  public void testFindingAesEncryption()  {
    KeyProvider.Metadata meta = new KMSClientProvider.KMSMetadata(
        "AES/CTR/NoPadding", 128, "", new HashMap<String, String>(),
        new Date(0), 1);
    assertEquals(EncryptionAlgorithm.AES_CTR_128,
        HadoopShimsPre2_7.findAlgorithm(meta));
    meta = new KMSClientProvider.KMSMetadata(
        "AES/CTR/NoPadding", 256, "", new HashMap<String, String>(),
        new Date(0), 1);
    assertEquals(EncryptionAlgorithm.AES_CTR_256,
        HadoopShimsPre2_7.findAlgorithm(meta));
    meta = new KMSClientProvider.KMSMetadata(
        "AES/CTR/NoPadding", 512, "", new HashMap<String, String>(),
        new Date(0), 1);
    assertEquals(EncryptionAlgorithm.AES_CTR_256,
        HadoopShimsPre2_7.findAlgorithm(meta));
  }
}
