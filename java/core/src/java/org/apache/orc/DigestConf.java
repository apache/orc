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
package org.apache.orc;

import com.tdunning.math.stats.TDigest;

public class DigestConf {

  public static final DigestConf NO_CREATE_DIGEST = new DigestConf(false, 0);

  public static final DigestConf DEFAULT_DIGEST = new DigestConf(true, 1000);

  private final boolean createDigest;

  private final double digestCompression;

  private final boolean persistence;

  public DigestConf(boolean createDigest, double digestCompression) {
    this(createDigest, digestCompression, false);
  }

  public DigestConf(boolean createDigest, double digestCompression, boolean persistence) {
    this.createDigest = createDigest;
    this.digestCompression = digestCompression;
    this.persistence = persistence;
  }

  public boolean getCreateDigest() {
    return createDigest;
  }

  public double getDigestCompression() {
    return digestCompression;
  }

  public boolean getPersistence() {
    return persistence;
  }

  public TDigest createDigest() {
    return createDigest ? TDigest.createAvlTreeDigest(digestCompression) : null;
  }

  public DigestConf copy(boolean persistence) {
    return new DigestConf(createDigest, digestCompression, persistence);
  }

  @Override
  public String toString() {
    return "DigestConf{" +
        "createDigest=" + createDigest +
        ", digestCompression=" + digestCompression +
        ", persistence=" + persistence +
        '}';
  }
}
