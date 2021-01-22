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

import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the old Hadoop-based KeyProvider.Factory API.
 */
public class HadoopKeyProviderFactory implements KeyProvider.Factory {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopKeyProviderFactory.class);

    @Override
    public KeyProvider create(String kind,
                              Configuration conf,
                              Random random) throws IOException {
      if ("hadoop".equals(kind)) {
        List<org.apache.hadoop.crypto.key.KeyProvider> result =
            KeyProviderFactory.getProviders(conf);
        if (result.size() == 0) {
          LOG.info("Can't get KeyProvider for ORC encryption from" +
              " hadoop.security.key.provider.path.");
        } else {
          return new HadoopShimsPre2_7.KeyProviderImpl(result.get(0), random);
        }
      }
      return null;
    }
  }
