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

import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.sql.Timestamp;

public interface CustomStatistics extends UpdateStatistics {

  CustomStatistics EMPTY_IMPL = new CustomStatistics() {
    @Override
    public String name() {
      return "EmptyCustomStatisticsImpl";
    }

    @Override
    public ByteString content() {
      return ByteString.EMPTY;
    }

    @Override
    public boolean needPersistence() {
      return false;
    }

    @Override
    public void merge(CustomStatistics customStatistics) {}

    @Override
    public void reset() {}

    @Override
    public void updateCollectionLength(long value) {}

    @Override
    public void updateBoolean(boolean value, int repetitions) {}

    @Override
    public void updateInteger(long value, int repetitions) {}

    @Override
    public void updateDouble(double value) {}

    @Override
    public void updateString(Text value) {}

    @Override
    public void updateString(byte[] bytes, int offset, int length, int repetitions) {}

    @Override
    public void updateBinary(BytesWritable value) {}

    @Override
    public void updateBinary(byte[] bytes, int offset, int length, int repetitions) {}

    @Override
    public void updateDecimal(HiveDecimalWritable value) {}

    @Override
    public void updateDecimal64(long value, int scale) {}

    @Override
    public void updateDate(DateWritable value) {}

    @Override
    public void updateDate(int value) {}

    @Override
    public void updateTimestamp(Timestamp value) {}

    @Override
    public void updateTimestamp(long value, int nanos) {}
  };

  String name();

  ByteString content();

  boolean needPersistence();

  void merge(CustomStatistics customStatistics);
}
