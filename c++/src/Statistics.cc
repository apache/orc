 /**
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

#include "Exceptions.hh"
#include "RLE.hh"
#include "Statistics.hh"

#include "wrap/coded-stream-wrapper.h"

namespace orc {

  ColumnStatistics* convertColumnStatistics(const proto::ColumnStatistics& s,
                                            bool correctStats) {
    if (s.has_intstatistics()) {
      return new IntegerColumnStatisticsImpl(s);
    } else if (s.has_doublestatistics()) {
      return new DoubleColumnStatisticsImpl(s);
    } else if (s.has_stringstatistics()) {
      return new StringColumnStatisticsImpl(s, correctStats);
    } else if (s.has_bucketstatistics()) {
      return new BooleanColumnStatisticsImpl(s, correctStats);
    } else if (s.has_decimalstatistics()) {
      return new DecimalColumnStatisticsImpl(s, correctStats);
    } else if (s.has_timestampstatistics()) {
      return new TimestampColumnStatisticsImpl(s, correctStats);
    } else if (s.has_datestatistics()) {
      return new DateColumnStatisticsImpl(s, correctStats);
    } else if (s.has_binarystatistics()) {
      return new BinaryColumnStatisticsImpl(s, correctStats);
    } else {
      return new ColumnStatisticsImpl(s);
    }
  }

  StatisticsImpl::StatisticsImpl(const proto::StripeStatistics& stripeStats, bool correctStats) {
    for(int i = 0; i < stripeStats.colstats_size(); i++) {
      colStats.push_back(convertColumnStatistics
          (stripeStats.colstats(i), correctStats));
    }
  }

  StatisticsImpl::StatisticsImpl(const proto::Footer& footer, bool correctStats) {
    for(int i = 0; i < footer.statistics_size(); i++) {
      colStats.push_back(convertColumnStatistics
          (footer.statistics(i), correctStats));
    }
  }

  StatisticsImpl::~StatisticsImpl() {
    for(std::list<ColumnStatistics*>::iterator ptr = colStats.begin();
        ptr != colStats.end();
        ++ptr) {
      delete *ptr;
    }
  }

  Statistics::~Statistics() {
    // PASS
  }

  ColumnStatistics::~ColumnStatistics() {
    // PASS
  }

  BinaryColumnStatistics::~BinaryColumnStatistics() {
    // PASS
  }

  BooleanColumnStatistics::~BooleanColumnStatistics() {
    // PASS
  }

  DateColumnStatistics::~DateColumnStatistics() {
    // PASS
  }

  DecimalColumnStatistics::~DecimalColumnStatistics() {
    // PASS
  }

  DoubleColumnStatistics::~DoubleColumnStatistics() {
    // PASS
  }

  IntegerColumnStatistics::~IntegerColumnStatistics() {
    // PASS
  }

  StringColumnStatistics::~StringColumnStatistics() {
    // PASS
  }

  TimestampColumnStatistics::~TimestampColumnStatistics() {
    // PASS
  }

  ColumnStatisticsImpl::~ColumnStatisticsImpl() {
    // PASS
  }

  BinaryColumnStatisticsImpl::~BinaryColumnStatisticsImpl() {
    // PASS
  }

  BooleanColumnStatisticsImpl::~BooleanColumnStatisticsImpl() {
    // PASS
  }

  DateColumnStatisticsImpl::~DateColumnStatisticsImpl() {
    // PASS
  }

  DecimalColumnStatisticsImpl::~DecimalColumnStatisticsImpl() {
    // PASS
  }

  DoubleColumnStatisticsImpl::~DoubleColumnStatisticsImpl() {
    // PASS
  }

  IntegerColumnStatisticsImpl::~IntegerColumnStatisticsImpl() {
    // PASS
  }

  StringColumnStatisticsImpl::~StringColumnStatisticsImpl() {
    // PASS
  }

  TimestampColumnStatisticsImpl::~TimestampColumnStatisticsImpl() {
    // PASS
  }

  ColumnStatisticsImpl::ColumnStatisticsImpl
  (const proto::ColumnStatistics& pb) {
    valueCount = pb.numberofvalues();
  }

  BinaryColumnStatisticsImpl::BinaryColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_binarystatistics() || !correctStats) {
      _hasTotalLength = false;

      totalLength = 0;
    }else{
      _hasTotalLength = pb.binarystatistics().has_sum();
      totalLength = static_cast<uint64_t>(pb.binarystatistics().sum());
    }
  }

  BooleanColumnStatisticsImpl::BooleanColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_bucketstatistics() || !correctStats) {
      _hasCount = false;
      trueCount = 0;
    }else{
      _hasCount = true;
      trueCount = pb.bucketstatistics().count(0);
    }
  }

  DateColumnStatisticsImpl::DateColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_datestatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;

      minimum = 0;
      maximum = 0;
    } else {
      _hasMinimum = pb.datestatistics().has_minimum();
      _hasMaximum = pb.datestatistics().has_maximum();
      minimum = pb.datestatistics().minimum();
      maximum = pb.datestatistics().maximum();
    }
  }

  DecimalColumnStatisticsImpl::DecimalColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_decimalstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;
    }else{
      const proto::DecimalStatistics& stats = pb.decimalstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  DoubleColumnStatisticsImpl::DoubleColumnStatisticsImpl
  (const proto::ColumnStatistics& pb){
    valueCount = pb.numberofvalues();
    if (!pb.has_doublestatistics()) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;

      minimum = 0;
      maximum = 0;
      sum = 0;
    }else{
      const proto::DoubleStatistics& stats = pb.doublestatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  IntegerColumnStatisticsImpl::IntegerColumnStatisticsImpl
  (const proto::ColumnStatistics& pb){
    valueCount = pb.numberofvalues();
    if (!pb.has_intstatistics()) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;

      minimum = 0;
      maximum = 0;
      sum = 0;
    }else{
      const proto::IntegerStatistics& stats = pb.intstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  StringColumnStatisticsImpl::StringColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_stringstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasTotalLength = false;

      totalLength = 0;
    }else{
      const proto::StringStatistics& stats = pb.stringstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasTotalLength = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      totalLength = static_cast<uint64_t>(stats.sum());
    }
  }

  TimestampColumnStatisticsImpl::TimestampColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats) {
    valueCount = pb.numberofvalues();
    if (!pb.has_timestampstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
      minimum = 0;
      maximum = 0;
    }else{
      const proto::TimestampStatistics& stats = pb.timestampstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();

      minimum = stats.minimum();
      maximum = stats.maximum();
    }
  }

}// namespace
