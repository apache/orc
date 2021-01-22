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

package org.apache.orc.shims;

/**
 * A shim for a configuration object.
 */
public interface Configuration {
  /**
   * Get the value of a given key
   * @param key the key to look up
   * @return the value of the given key or null if that key isn't set
   */
  String get(String key);

  /**
   * Set the value for a given key
   * @param key the key to set
   * @param value the new value to set
   */
  void set(String key, String value);
}
