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

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.sql.Timestamp;

public interface UpdateStatistics {

  void reset();

  /**
   * Update the collection length for Map and List type.
   * @param value length of collection
   */
  default void updateCollectionLength(final long value) {
    throw new UnsupportedOperationException(
        "Can't update collection count");
  }

  default void updateBoolean(boolean value, int repetitions) {
    throw new UnsupportedOperationException("Can't update boolean");
  }

  default void updateInteger(long value, int repetitions) {
    throw new UnsupportedOperationException("Can't update integer");
  }

  default void updateDouble(double value) {
    throw new UnsupportedOperationException("Can't update double");
  }

  default void updateString(Text value) {
    throw new UnsupportedOperationException("Can't update string");
  }

  default void updateString(byte[] bytes, int offset, int length,
                           int repetitions) {
    throw new UnsupportedOperationException("Can't update string");
  }

  default void updateBinary(BytesWritable value) {
    throw new UnsupportedOperationException("Can't update binary");
  }

  default void updateBinary(byte[] bytes, int offset, int length,
                           int repetitions) {
    throw new UnsupportedOperationException("Can't update string");
  }

  default void updateDecimal(HiveDecimalWritable value) {
    throw new UnsupportedOperationException("Can't update decimal");
  }

  default void updateDecimal64(long value, int scale) {
    throw new UnsupportedOperationException("Can't update decimal");
  }

  default void updateDate(DateWritable value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  default void updateDate(int value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  default void updateTimestamp(Timestamp value) {
    throw new UnsupportedOperationException("Can't update timestamp");
  }

  // has to be extended
  default void updateTimestamp(long value, int nanos) {
    throw new UnsupportedOperationException("Can't update timestamp");
  }
}
