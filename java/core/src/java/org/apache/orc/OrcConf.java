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

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;


/**
 * Define the configuration properties that Orc understands.
 */
public enum OrcConf {
  STRIPE_SIZE(org.apache.orc.core.OrcConf.STRIPE_SIZE),
  BLOCK_SIZE(org.apache.orc.core.OrcConf.BLOCK_SIZE),
  ENABLE_INDEXES(org.apache.orc.core.OrcConf.ENABLE_INDEXES),
  ROW_INDEX_STRIDE(org.apache.orc.core.OrcConf.ROW_INDEX_STRIDE),
  BUFFER_SIZE(org.apache.orc.core.OrcConf.BUFFER_SIZE),
  BASE_DELTA_RATIO(org.apache.orc.core.OrcConf.BASE_DELTA_RATIO),
  BLOCK_PADDING(org.apache.orc.core.OrcConf.BLOCK_PADDING),
  COMPRESS(org.apache.orc.core.OrcConf.COMPRESS),
  WRITE_FORMAT(org.apache.orc.core.OrcConf.WRITE_FORMAT),
  ENFORCE_COMPRESSION_BUFFER_SIZE(org.apache.orc.core.OrcConf.ENFORCE_COMPRESSION_BUFFER_SIZE),
  ENCODING_STRATEGY(org.apache.orc.core.OrcConf.ENCODING_STRATEGY),
  COMPRESSION_STRATEGY(org.apache.orc.core.OrcConf.COMPRESSION_STRATEGY),
  BLOCK_PADDING_TOLERANCE(org.apache.orc.core.OrcConf.BLOCK_PADDING_TOLERANCE),
  BLOOM_FILTER_FPP(org.apache.orc.core.OrcConf.BLOOM_FILTER_FPP),
  USE_ZEROCOPY(org.apache.orc.core.OrcConf.USE_ZEROCOPY),
  SKIP_CORRUPT_DATA(org.apache.orc.core.OrcConf.SKIP_CORRUPT_DATA),
  TOLERATE_MISSING_SCHEMA(org.apache.orc.core.OrcConf.TOLERATE_MISSING_SCHEMA),
  MEMORY_POOL(org.apache.orc.core.OrcConf.MEMORY_POOL),
  DICTIONARY_KEY_SIZE_THRESHOLD(org.apache.orc.core.OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD),
  ROW_INDEX_STRIDE_DICTIONARY_CHECK(org.apache.orc.core.OrcConf.ROW_INDEX_STRIDE_DICTIONARY_CHECK),
  BLOOM_FILTER_COLUMNS(org.apache.orc.core.OrcConf.BLOOM_FILTER_COLUMNS),
  BLOOM_FILTER_WRITE_VERSION(org.apache.orc.core.OrcConf.BLOOM_FILTER_WRITE_VERSION),
  IGNORE_NON_UTF8_BLOOM_FILTERS(org.apache.orc.core.OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS),
  MAX_FILE_LENGTH(org.apache.orc.core.OrcConf.MAX_FILE_LENGTH),
  MAPRED_INPUT_SCHEMA(org.apache.orc.core.OrcConf.MAPRED_INPUT_SCHEMA),
  MAPRED_SHUFFLE_KEY_SCHEMA(org.apache.orc.core.OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA),
  MAPRED_SHUFFLE_VALUE_SCHEMA(org.apache.orc.core.OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA),
  MAPRED_OUTPUT_SCHEMA(org.apache.orc.core.OrcConf.MAPRED_OUTPUT_SCHEMA),
  INCLUDE_COLUMNS(org.apache.orc.core.OrcConf.INCLUDE_COLUMNS),
  KRYO_SARG(org.apache.orc.core.OrcConf.KRYO_SARG),
  KRYO_SARG_BUFFER(org.apache.orc.core.OrcConf.KRYO_SARG_BUFFER),
  SARG_COLUMNS(org.apache.orc.core.OrcConf.SARG_COLUMNS),
  FORCE_POSITIONAL_EVOLUTION(org.apache.orc.core.OrcConf.FORCE_POSITIONAL_EVOLUTION),
  FORCE_POSITIONAL_EVOLUTION_LEVEL(org.apache.orc.core.OrcConf.FORCE_POSITIONAL_EVOLUTION_LEVEL),
  ROWS_BETWEEN_CHECKS(org.apache.orc.core.OrcConf.ROWS_BETWEEN_CHECKS),
  OVERWRITE_OUTPUT_FILE(org.apache.orc.core.OrcConf.OVERWRITE_OUTPUT_FILE),
  IS_SCHEMA_EVOLUTION_CASE_SENSITIVE(org.apache.orc.core.OrcConf.IS_SCHEMA_EVOLUTION_CASE_SENSITIVE),
  WRITE_VARIABLE_LENGTH_BLOCKS(org.apache.orc.core.OrcConf.WRITE_VARIABLE_LENGTH_BLOCKS),
  DIRECT_ENCODING_COLUMNS(org.apache.orc.core.OrcConf.DIRECT_ENCODING_COLUMNS),
  // some JVM doesn't allow array creation of size Integer.MAX_VALUE, so chunk size is slightly less than max int
  ORC_MAX_DISK_RANGE_CHUNK_LIMIT(org.apache.orc.core.OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT),
  ENCRYPTION(org.apache.orc.core.OrcConf.ENCRYPTION),
  DATA_MASK(org.apache.orc.core.OrcConf.DATA_MASK),
  KEY_PROVIDER(org.apache.orc.core.OrcConf.KEY_PROVIDER),
  PROLEPTIC_GREGORIAN(org.apache.orc.core.OrcConf.PROLEPTIC_GREGORIAN),
  PROLEPTIC_GREGORIAN_DEFAULT(org.apache.orc.core.OrcConf.PROLEPTIC_GREGORIAN_DEFAULT)
  ;

  private final org.apache.orc.core.OrcConf value;

  OrcConf(org.apache.orc.core.OrcConf value) {
    this.value = value;
  }

  public String getAttribute() {
    return value.getAttribute();
  }

  public String getHiveConfName() {
    return value.getHiveConfName();
  }

  public Object getDefaultValue() {
    return value.getDefaultValue();
  }

  public String getDescription() {
    return value.getDescription();
  }

  private String lookupValue(Properties tbl, Configuration conf) {
    String result = null;
    if (tbl != null) {
      result = tbl.getProperty(getAttribute());
    }
    if (result == null && conf != null) {
      result = conf.get(getAttribute());
      if (result == null && getHiveConfName() != null) {
        result = conf.get(getHiveConfName());
      }
    }
    return result;
  }

  public int getInt(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return ((Number) getDefaultValue()).intValue();
  }

  public int getInt(Configuration conf) {
    return getInt(null, conf);
  }

  public void getInt(Configuration conf, int value) {
    conf.setInt(getAttribute(), value);
  }

  public long getLong(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Long.parseLong(value);
    }
    return ((Number) getDefaultValue()).longValue();
  }

  public long getLong(Configuration conf) {
    return getLong(null, conf);
  }

  public void setLong(Configuration conf, long value) {
    conf.setLong(getAttribute(), value);
  }

  public String getString(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    return value == null ? (String) getDefaultValue() : value;
  }

  public String getString(Configuration conf) {
    return getString(null, conf);
  }

  public void setString(Configuration conf, String value) {
    conf.set(getAttribute(), value);
  }

  public boolean getBoolean(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return (Boolean) getDefaultValue();
  }

  public boolean getBoolean(Configuration conf) {
    return getBoolean(null, conf);
  }

  public void setBoolean(Configuration conf, boolean value) {
    conf.setBoolean(getAttribute(), value);
  }

  public double getDouble(Properties tbl, Configuration conf) {
    String value = lookupValue(tbl, conf);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return ((Number) getDefaultValue()).doubleValue();
  }

  public double getDouble(Configuration conf) {
    return getDouble(null, conf);
  }

  public void setDouble(Configuration conf, double value) {
    conf.setDouble(getAttribute(), value);
  }

  /* Add the forward compatible functions using the non-Hadoop Configuration. */

  public void setString(org.apache.orc.shims.Configuration conf, String newValue) {
    value.setString(conf, newValue);
  }

  public void setInt(org.apache.orc.shims.Configuration conf, int newValue) {
    value.setInt(conf, newValue);
  }

  public void setLong(org.apache.orc.shims.Configuration conf, long newValue) {
    value.setLong(conf, newValue);
  }

  public void setBoolean(org.apache.orc.shims.Configuration conf, boolean newValue) {
    value.setBoolean(conf, newValue);
  }

  public void setDouble(org.apache.orc.shims.Configuration conf, double newValue) {
    value.setDouble(conf, newValue);
  }

  public String getString(org.apache.orc.shims.Configuration conf) {
    return value.getString(conf);
  }

  public int getInt(org.apache.orc.shims.Configuration conf) {
    return value.getInt(conf);
  }

  public long getLong(org.apache.orc.shims.Configuration conf) {
    return value.getLong(conf);
  }

  public boolean getBoolean(org.apache.orc.shims.Configuration conf) {
    return value.getBoolean(conf);
  }

  public double getDouble(org.apache.orc.shims.Configuration conf) {
    return value.getDouble(conf);
  }
}
