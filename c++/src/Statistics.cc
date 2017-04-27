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
    hasNullValue = pb.hasnull();
  }

  BinaryColumnStatisticsImpl::BinaryColumnStatisticsImpl
    (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    hasNullValue = pb.hasnull();
    if (!pb.has_binarystatistics() || !correctStats) {
      hasTotalLengthValue = false;
      totalLength = 0;
    }else{
      hasTotalLengthValue = pb.binarystatistics().has_sum();
      totalLength = static_cast<uint64_t>(pb.binarystatistics().sum());
    }
  }

  BooleanColumnStatisticsImpl::BooleanColumnStatisticsImpl
    (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    hasNullValue = pb.hasnull();
    if (!pb.has_bucketstatistics() || !correctStats) {
      hasCountValue = false;
      trueCount = 0;
    }else{
      hasCountValue = true;
      trueCount = pb.bucketstatistics().count(0);
    }
  }

  DateColumnStatisticsImpl::DateColumnStatisticsImpl
    (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    hasNullValue = pb.hasnull();
    if (!pb.has_datestatistics() || !correctStats) {
      hasMinimumValue = false;
      hasMaximumValue = false;

      minimum = 0;
      maximum = 0;
    } else {
      hasMinimumValue = pb.datestatistics().has_minimum();
      hasMaximumValue = pb.datestatistics().has_maximum();
      minimum = pb.datestatistics().minimum();
      maximum = pb.datestatistics().maximum();
    }
  }

  DecimalColumnStatisticsImpl::DecimalColumnStatisticsImpl
    (const proto::ColumnStatistics& pb, bool correctStats): minimum(0, 0),
                                                            maximum(0, 0),
                                                            sum(0, 0){
    valueCount = pb.numberofvalues();
    hasNullValue = pb.hasnull();
    if (!pb.has_decimalstatistics() || !correctStats) {
      hasMinimumValue = false;
      hasMaximumValue = false;
      hasSumValue = false;
    }else{
      const proto::DecimalStatistics& stats = pb.decimalstatistics();
      hasMinimumValue = stats.has_minimum();
      hasMaximumValue = stats.has_maximum();
      hasSumValue = stats.has_sum();

      minimum = Decimal(stats.minimum());
      maximum = Decimal(stats.maximum());
      sum = Decimal(stats.sum());
    }
  }

  DoubleColumnStatisticsImpl::DoubleColumnStatisticsImpl
    (const proto::ColumnStatistics& pb){
    valueCount = pb.numberofvalues();
    hasNullValue = pb.hasnull();
    if (!pb.has_doublestatistics()) {
      hasMinimumValue = false;
      hasMaximumValue = false;
      hasSumValue = false;

      minimum = 0;
      maximum = 0;
      sum = 0;
    }else{
      const proto::DoubleStatistics& stats = pb.doublestatistics();
      hasMinimumValue = stats.has_minimum();
      hasMaximumValue = stats.has_maximum();
      hasSumValue = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  IntegerColumnStatisticsImpl::IntegerColumnStatisticsImpl
    (const proto::ColumnStatistics& pb){
    valueCount = pb.numberofvalues();
    hasNullValue = pb.hasnull();
    if (!pb.has_intstatistics()) {
      hasMinimumValue = false;
      hasMaximumValue = false;
      hasSumValue = false;

      minimum = 0;
      maximum = 0;
      sum = 0;
    }else{
      const proto::IntegerStatistics& stats = pb.intstatistics();
      hasMinimumValue = stats.has_minimum();
      hasMaximumValue = stats.has_maximum();
      hasSumValue = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  StringColumnStatisticsImpl::StringColumnStatisticsImpl
    (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    hasNullValue = pb.hasnull();
    if (!pb.has_stringstatistics() || !correctStats) {
      hasMinimumValue = false;
      hasMaximumValue = false;
      hasTotalLen = false;

      totalLength = 0;
    }else{
      const proto::StringStatistics& stats = pb.stringstatistics();
      hasMinimumValue = stats.has_minimum();
      hasMaximumValue = stats.has_maximum();
      hasTotalLen = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      totalLength = static_cast<uint64_t>(stats.sum());
    }
  }

  TimestampColumnStatisticsImpl::TimestampColumnStatisticsImpl
    (const proto::ColumnStatistics& pb, bool correctStats) {
    valueCount = pb.numberofvalues();
    hasNullValue = pb.hasnull();
    if (!pb.has_timestampstatistics() || !correctStats) {
      hasMinimumValue = false;
      hasMaximumValue = false;
      minimum = 0;
      maximum = 0;
    }else{
      const proto::TimestampStatistics& stats = pb.timestampstatistics();
      hasMinimumValue = stats.has_minimum();
      hasMaximumValue = stats.has_maximum();

      minimum = stats.minimum();
      maximum = stats.maximum();
    }
  }

  std::unique_ptr<ColumnStatistics> createColumnStatistics(
    const Type& type, bool enableStrCmp) {
    switch (static_cast<int64_t>(type.getKind())) {
      case BOOLEAN:
        return std::unique_ptr<ColumnStatistics>(
          new BooleanColumnStatisticsImpl());
      case BYTE:
      case INT:
      case LONG:
      case SHORT:
        return std::unique_ptr<ColumnStatistics>(
          new IntegerColumnStatisticsImpl());
      case STRUCT:
      case MAP:
      case LIST:
      case UNION:
        return std::unique_ptr<ColumnStatistics>(
          new ColumnStatisticsImpl());
      case FLOAT:
      case DOUBLE:
        return std::unique_ptr<ColumnStatistics>(
          new DoubleColumnStatisticsImpl());
      case BINARY:
        return std::unique_ptr<ColumnStatistics>(
          new BinaryColumnStatisticsImpl());
      case STRING:
      case CHAR:
      case VARCHAR:
        return std::unique_ptr<ColumnStatistics>(
          new StringColumnStatisticsImpl(enableStrCmp));
      case DATE:
        return std::unique_ptr<ColumnStatistics>(
          new DateColumnStatisticsImpl());
      case TIMESTAMP:
        return std::unique_ptr<ColumnStatistics>(
          new TimestampColumnStatisticsImpl());
      case DECIMAL:
        return std::unique_ptr<ColumnStatistics>(
          new DecimalColumnStatisticsImpl());
      default:
        throw NotImplementedYet("Not supported type " + type.toString() +
                                  " for ColumnStatistics");
    }
  }

}// namespace
