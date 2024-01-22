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

import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * The factory for getting the proper version of the Hadoop shims.
 */
public class HadoopShimsFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopShimsFactory.class);

  private static final String CURRENT_SHIM_NAME =
      "org.apache.orc.impl.HadoopShimsCurrent";
  private static volatile HadoopShims SHIMS = null;

  private static HadoopShims createShimByName(String name) {
    try {
      Class<? extends HadoopShims> cls =
          (Class<? extends HadoopShims>) Class.forName(name);
      return cls.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException |
             InstantiationException | IllegalAccessException | IllegalArgumentException |
             InvocationTargetException e) {
      throw new IllegalStateException("Can't create shims for " + name, e);
    }
  }

  public static HadoopShims get() {
    if (SHIMS == null) {
      synchronized (HadoopShimsFactory.class) {
        if (SHIMS == null) {
          String[] versionParts = VersionInfo.getVersion().split("[.]");
          int major = Integer.parseInt(versionParts[0]);
          int minor = Integer.parseInt(versionParts[1]);
          if (major < 2 || (major == 2 && minor < 7)) {
            LOG.warn("Hadoop " + VersionInfo.getVersion() + " support is dropped.");
          }
          SHIMS = createShimByName(CURRENT_SHIM_NAME);
        }
      }
    }
    return SHIMS;
  }
}
