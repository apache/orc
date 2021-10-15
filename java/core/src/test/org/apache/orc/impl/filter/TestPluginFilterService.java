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

package org.apache.orc.impl.filter;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPluginFilterService {
  private final Configuration conf;

  public TestPluginFilterService() {
    conf = new Configuration();
    conf.set("my.filter.col.name", "f2");
    conf.set("my.filter.col.value", "aBcd");
    conf.set("my.filter.scope", "file://db/table1/.*");
  }

  @Test
  public void testFoundFilter() {
    conf.set("my.filter.name", "my_str_i_eq");
    assertNotNull(FilterFactory.findPluginFilters("file://db/table1/file1", conf));
  }

  @Test
  public void testErrorCreatingFilter() {
    Configuration localConf = new Configuration(conf);
    localConf.set("my.filter.name", "my_str_i_eq");
    localConf.set("my.filter.col.name", "");
    assertThrows(IllegalArgumentException.class,
                 () -> FilterFactory.findPluginFilters("file://db/table1/file1", localConf),
                 "Filter needs a valid column name");
  }

  @Test
  public void testMissingFilter() {
    assertTrue(FilterFactory.findPluginFilters("file://db/table11/file1", conf).isEmpty());
  }
}