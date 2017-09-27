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
package org.apache.orc.impl.acid;

import java.util.regex.Pattern;

class AcidConstants {
  static final String BASE_PREFIX = "base_";
  static final Pattern BUCKET_PATTERN = Pattern.compile("bucket_([0-9]+)");
  static final Pattern LEGACY_BUCKET_PATTERN = Pattern.compile("[0-9]+_([0-9]+)");
  static final String DELTA_PREFIX = "delta_";
  static final String DELETE_DELTA_PREFIX = "delete_delta_";
  static final String DELTA_SIDE_FILE_SUFFIX = "_flush_length";
}
