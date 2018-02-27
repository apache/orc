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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.Key;

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

    memoryKeystore = new InMemoryKeystore();
    memoryKeystore
        .addKey("key128", EncryptionAlgorithm.AES_128, 0, "123".getBytes());
    memoryKeystore.addKey("key256", EncryptionAlgorithm.AES_256, 0,
        "secret123".getBytes());
    memoryKeystore
        .addKey("key256short", EncryptionAlgorithm.AES_256, 0, "5".getBytes());

  }

  @Test
  public void testGetKeyNames() throws IOException {

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

    HadoopShims.KeyMetadata metadata = memoryKeystore
        .getCurrentKeyVersion("key128");
    byte[] iv = new byte[EncryptionAlgorithm.AES_128.getIvLength()];

    final Key key128 = memoryKeystore.getLocalKey(metadata, iv);
    Assert.assertEquals("AES", key128.getAlgorithm());

  }

  @Test
  public void testRollNewVersion() throws IOException {

    Assert.assertEquals(0,
        memoryKeystore.getCurrentKeyVersion("key128").getVersion());
    memoryKeystore.rollNewVersion("key128", "NewSecret".getBytes());
    Assert.assertEquals(1,
        memoryKeystore.getCurrentKeyVersion("key128").getVersion());

  }

  @Test
  public void testDuplicateKeyNames() {
    try {
      memoryKeystore.addKey("key128", EncryptionAlgorithm.AES_128, 0,
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
   *
   * @throws IOException
   */
  @Test
  public void testMultipleVersion() throws IOException {

    Assert.assertEquals(0,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());
    memoryKeystore.rollNewVersion("key256", "NewSecret".getBytes());
    Assert.assertEquals(1,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());

    try {
      memoryKeystore.addKey("key256", EncryptionAlgorithm.AES_128, 1,
          "NewSecret".getBytes());
      Assert.fail("Keys with smaller version should not be added.");
    } catch (final IOException e) {
      Assert.assertTrue(e.toString().contains("equal or higher version"));
    }

    memoryKeystore.addKey("key256", EncryptionAlgorithm.AES_128, 2,
        "NewSecret".getBytes());
    Assert.assertEquals(2,
        memoryKeystore.getCurrentKeyVersion("key256").getVersion());

    Assert.assertEquals(3, memoryKeystore.getKeyVersions("key256").size());

  }

}
