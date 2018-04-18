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

import org.apache.hadoop.io.BytesWritable;
import org.apache.orc.impl.HadoopShims;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.Key;
import java.util.Random;

/**
 * Test {@link InMemoryKeystore} class
 */
public class TestInMemoryKeystore {

  private InMemoryKeystore memoryKeystore;

  public TestInMemoryKeystore() {
    super();
  }

  @Before
  public void init() throws IOException {
    // For testing, use a fixed random number generator so that everything
    // is repeatable.
    Random random = new Random(2);
    memoryKeystore =
        new InMemoryKeystore(random)
            .addKey("key128", EncryptionAlgorithm.AES_128, "123".getBytes())
            .addKey("key256", EncryptionAlgorithm.AES_256, "secret123".getBytes())
            .addKey("key256short", EncryptionAlgorithm.AES_256, "5".getBytes());

  }

  private static String stringify(byte[] buffer) {
    return new BytesWritable(buffer).toString();
  }

  @Test
  public void testGetKeyNames() {

    Assert.assertTrue(memoryKeystore.getKeyNames().contains("key128"));
    Assert.assertTrue(memoryKeystore.getKeyNames().contains("key256"));
    Assert.assertTrue(memoryKeystore.getKeyNames().contains("key256short"));

  }

  @Test
  public void testGetCurrentKeyVersion() {

    final HadoopShims.KeyMetadata metadata = memoryKeystore
        .getCurrentKeyVersion("key256");

    Assert.assertEquals("key256", metadata.getKeyName());
    if (InMemoryKeystore.SUPPORTS_AES_256) {
      Assert.assertEquals(EncryptionAlgorithm.AES_256, metadata.getAlgorithm());
    } else {
      Assert.assertEquals(EncryptionAlgorithm.AES_128, metadata.getAlgorithm());
    }

    Assert.assertEquals(0, metadata.getVersion());

  }

  @Test
  public void testGetLocalKey() {

    HadoopShims.KeyMetadata metadata128 = memoryKeystore
        .getCurrentKeyVersion("key128");

    HadoopShims.LocalKey key128 = memoryKeystore.createLocalKey(metadata128);
    // we are sure the key is the same because of the random generator.
    Assert.assertEquals("39 72 2c bb f8 b9 1a 4b 90 45 c5 e6 17 5f 10 01",
        stringify(key128.decryptedKey.getEncoded()));
    // used online aes/cbc calculator to encrypt key
    Assert.assertEquals("7f 41 4a 46 81 ee 7c d1 2a 0f ed 39 a8 49 e2 89",
        stringify(key128.encryptedKey));
    Assert.assertEquals("AES", key128.decryptedKey.getAlgorithm());

    // now decrypt the key again
    Key decryptKey = memoryKeystore.decryptLocalKey(metadata128,
        key128.encryptedKey);
    Assert.assertEquals(stringify(key128.decryptedKey.getEncoded()),
        stringify(decryptKey.getEncoded()));

    HadoopShims.KeyMetadata metadata256 = memoryKeystore
        .getCurrentKeyVersion("key256");
    HadoopShims.LocalKey key256 = memoryKeystore.createLocalKey(metadata256);
    // this is forced by the fixed Random in the keystore for this test
    if (InMemoryKeystore.SUPPORTS_AES_256) {
      Assert.assertEquals("ea c3 2f 7f cd 5e cc da 5c 6e 62 fc 4e 63 85 08 0f 7b" +
              " 6c db 79 e5 51 ec 9c 9c c7 fc bd 60 ee 73",
          stringify(key256.decryptedKey.getEncoded()));
      // used online aes/cbc calculator to encrypt key
      Assert.assertEquals("ea 73 33 5b 14 5d 70 d8 3f e9 d1 05 2b 2d 62 a0 86 16"+
              " ad a0 2a d6 8a 20 46 1d 00 ce f9 2a 31 48",
          stringify(key256.encryptedKey));
    } else {
      Assert.assertEquals("ea c3 2f 7f cd 5e cc da 5c 6e 62 fc 4e 63 85 08",
          stringify(key256.decryptedKey.getEncoded()));
      Assert.assertEquals("87 df d0 2a 68 1a b9 cb a7 88 ec f4 83 49 95 e0",
          stringify(key256.encryptedKey));
    }
    Assert.assertEquals("AES", key256.decryptedKey.getAlgorithm());

    // now decrypt the key again
    decryptKey = memoryKeystore.decryptLocalKey(metadata256, key256.encryptedKey);
    Assert.assertEquals(stringify(key256.decryptedKey.getEncoded()),
        stringify(decryptKey.getEncoded()));
  }

  @Test
  public void testRollNewVersion() throws IOException {

    Assert.assertEquals(0,
        memoryKeystore.getCurrentKeyVersion("key128").getVersion());
    memoryKeystore.addKey("key128", 1, EncryptionAlgorithm.AES_128, "NewSecret".getBytes());
    Assert.assertEquals(1,
        memoryKeystore.getCurrentKeyVersion("key128").getVersion());
  }

  @Test
  public void testDuplicateKeyNames() {
    try {
      memoryKeystore.addKey("key128", 0, EncryptionAlgorithm.AES_128,
          "exception".getBytes());
      Assert.fail("Keys with same name cannot be added.");
    } catch (IOException e) {
      Assert.assertTrue(e.toString().contains("equal or higher version"));
    }

  }

  /**
   * This will test:
   * 1. Scenario where key with smaller version then existing should not be allowed
   * 2. Test multiple versions of the key
   * 3. Test get current version
   * 4. Ensure the different versions of the key have different material.
   */
  @Test
  public void testMultipleVersion() throws IOException {
    Assert.assertEquals(0,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());
    memoryKeystore.addKey("key256", 1, EncryptionAlgorithm.AES_256, "NewSecret".getBytes());
    Assert.assertEquals(1,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());

    try {
      memoryKeystore.addKey("key256", 1, EncryptionAlgorithm.AES_256,
          "BadSecret".getBytes());
      Assert.fail("Keys with smaller version should not be added.");
    } catch (final IOException e) {
      Assert.assertTrue(e.toString().contains("equal or higher version"));
    }

    memoryKeystore.addKey("key256", 2, EncryptionAlgorithm.AES_256,
        "NewerSecret".getBytes());
    Assert.assertEquals(2,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());

    // make sure that all 3 versions of key256 exist and have different secrets
    Key key0 = memoryKeystore.decryptLocalKey(
        new HadoopShims.KeyMetadata("key256", 0, EncryptionAlgorithm.AES_256),
        new byte[16]);
    Key key1 = memoryKeystore.decryptLocalKey(
        new HadoopShims.KeyMetadata("key256", 1, EncryptionAlgorithm.AES_256),
        new byte[16]);
    Key key2 = memoryKeystore.decryptLocalKey(
        new HadoopShims.KeyMetadata("key256", 2, EncryptionAlgorithm.AES_256),
        new byte[16]);
    Assert.assertNotEquals(new BytesWritable(key0.getEncoded()).toString(),
        new BytesWritable(key1.getEncoded()).toString());
    Assert.assertNotEquals(new BytesWritable(key1.getEncoded()).toString(),
        new BytesWritable(key2.getEncoded()).toString());
  }

}
