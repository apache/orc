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
  // File prefixes
  static final String BASE_PREFIX = "base_";
  static final String DELTA_PREFIX = "delta_";
  static final String DELETE_DELTA_PREFIX = "delete_delta_";

  // File patterns
  static final String DELTA_SIDE_FILE_SUFFIX = "_flush_length";
  static final Pattern BUCKET_PATTERN = Pattern.compile("bucket_([0-9]+)");
  static final Pattern LEGACY_BUCKET_PATTERN = Pattern.compile("[0-9]+_([0-9]+)");

  /**
   * Operation is no longer necessary but is still present for backwards compatibility
   */
  public static final String OPERATION_COL_NAME = "operation";
  public static final int OPERATION_COL_OFFSET = 0;
  public static final String ORIG_TXN_COL_NAME = "original_transaction";
  public static final int ORIG_TXN_COL_OFFSET = 1;
  /**
   * Bucket is now really writer id, but still called bucket for backwards compatibility.
   */
  public static final String BUCKET_COL_NAME = "bucket";
  public static final int BUCKET_COL_OFFSET = 2;
  public static final String ROW_ID_COL_NAME = "row_id";
  public static final int ROW_ID_COL_OFFSET = 3;
  public static final String CURRENT_TXN_COL_NAME = "current_transaction";
  public static final int CURRENT_TXN_COL_OFFSET = 4;

  /**
   * In ACID files the row data is stored in a separate struct that comes after the ROW__ID struct.
   */
  public static final String ROWS_STRUCT_COL_NAME = "ROWS";
  public static final int ROWS_STRUCT_COL_OFFSET = 5;



  // Operations are no longer used but still populated for backward compatibility
  static final int OPERATION_INSERT = 0;
  static final int OPERATION_DELETE = 2;

}
