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

#ifndef ORC_STATISTICS_IMPL_HH
#define ORC_STATISTICS_IMPL_HH

#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "TypeImpl.hh"

namespace orc {

/**
 * ColumnStatistics Implementation
 */

  class ColumnStatisticsImpl: public ColumnStatistics {
  public:
    ColumnStatisticsImpl() {
      reset();
    }
    ColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~ColumnStatisticsImpl();

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Column has " << valueCount << " values"
             << " and has null value: " << (hasNullValue ? "yes " : "no")
             << std::endl;
      return buffer.str();
    }
  };

  class BinaryColumnStatisticsImpl: public BinaryColumnStatistics {
  private:
    bool hasTotalLengthValue;
    uint64_t totalLength;

  public:
    BinaryColumnStatisticsImpl() {
      reset();
    }
    BinaryColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                               bool correctStats);
    virtual ~BinaryColumnStatisticsImpl();

    bool hasTotalLength() const override {
      return hasTotalLengthValue;
    }

    void setHasTotalLength(bool newHasTotalLength) override {
      hasTotalLengthValue = newHasTotalLength;
    }

    uint64_t getTotalLength() const override {
      if(hasTotalLengthValue){
        return totalLength;
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    void setTotalLength(uint64_t length) override {
      this->totalLength = length;
    }

    void reset() override {
      ColumnStatistics::reset();
      hasTotalLengthValue = false;
      totalLength = 0;
    }

    void merge(const ColumnStatistics& other) override {
      ColumnStatistics::merge(other);

      const BinaryColumnStatistics& binColStats =
        dynamic_cast<const BinaryColumnStatistics&>(other);

      totalLength += binColStats.getTotalLength();
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      ColumnStatistics::toProtoBuf(pbStats);

      proto::BinaryStatistics* binStats = pbStats.mutable_binarystatistics();
      binStats->set_sum(static_cast<int64_t>(totalLength));
    }

    void update(const char*, size_t length) override {
      totalLength += length;
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Binary" << std::endl
             << "Values: " << valueCount << std::endl
             << "Has null: " << (hasNullValue ? "yes" : "no") << std::endl;
      if(hasTotalLengthValue){
        buffer << "Total length: " << totalLength << std::endl;
      }else{
        buffer << "Total length: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class BooleanColumnStatisticsImpl: public BooleanColumnStatistics {
  private:
    bool hasCountValue;
    uint64_t trueCount;

  public:
    BooleanColumnStatisticsImpl() {
      reset();
    }
    BooleanColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                bool correctStats);
    virtual ~BooleanColumnStatisticsImpl();

    bool hasCount() const override {
      return hasCountValue;
    }

    uint64_t getFalseCount() const override {
      if(hasCountValue){
        return valueCount - trueCount;
      }else{
        throw ParseError("False count is not defined.");
      }
    }

    uint64_t getTrueCount() const override {
      if(hasCountValue){
        return trueCount;
      }else{
        throw ParseError("True count is not defined.");
      }
    }

    virtual void setTrueCount(uint64_t count) override {
      this->trueCount = count;
    }

    void setHasCount(bool hasCount) override {
      this->hasCountValue = hasCount;
    }

    void reset() override {
      ColumnStatistics::reset();
      hasCountValue = true;
      trueCount = 0;
    }

    void merge(const ColumnStatistics& other) override {
      ColumnStatistics::merge(other);

      const BooleanColumnStatistics& boolStats =
        dynamic_cast<const BooleanColumnStatistics&>(other);

      trueCount += boolStats.getTrueCount();
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      ColumnStatistics::toProtoBuf(pbStats);

      proto::BucketStatistics* bucketStats = pbStats.mutable_bucketstatistics();
      if (hasCountValue) {
        bucketStats->add_count(trueCount);
      }
    }

    void update(bool value, size_t repetitions) override {
      if (value) {
        trueCount += repetitions;
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Boolean" << std::endl
             << "Values: " << valueCount << std::endl
             << "Has null: " << (hasNullValue ? "yes" : "no") << std::endl;
      if(hasCountValue){
        buffer << "(true: " << trueCount << "; false: "
               << valueCount - trueCount << ")" << std::endl;
      } else {
        buffer << "(true: not defined; false: not defined)" << std::endl;
        buffer << "True and false count are not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DateColumnStatisticsImpl: public DateColumnStatistics {
  private:
    bool hasMinimumValue;
    bool hasMaximumValue;
    int32_t minimum;
    int32_t maximum;

  public:
    DateColumnStatisticsImpl() {
      reset();
    }
    DateColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                             bool correctStats);
    virtual ~DateColumnStatisticsImpl();

    bool hasMinimum() const override {
      return hasMinimumValue;
    }

    bool hasMaximum() const override {
      return hasMaximumValue;
    }

    int32_t getMinimum() const override {
      if(hasMinimumValue){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int32_t getMaximum() const override {
      if(hasMaximumValue){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    void setMinimum(int32_t min) override {
      this->minimum = min;
      this->hasMinimumValue = true;
    }

    void setMaximum(int32_t max) override {
      this->maximum = max;
      this->hasMaximumValue = true;
    }

    void reset() override {
      ColumnStatistics::reset();
      hasMinimumValue = false;
      hasMaximumValue = false;
      minimum = std::numeric_limits<int32_t>::min();
      maximum = std::numeric_limits<int32_t>::max();
    }

    void merge(const ColumnStatistics& other) override {
      ColumnStatistics::merge(other);

      const DateColumnStatistics& dateStats =
        dynamic_cast<const DateColumnStatistics &>(other);

      if (dateStats.hasMinimum()) {
        if (!hasMinimumValue) {
          hasMinimumValue = hasMaximumValue = true;
          minimum = dateStats.getMinimum();
          maximum = dateStats.getMaximum();
        } else {
          if (dateStats.getMaximum() > maximum) {
            maximum = dateStats.getMaximum();
          }
          if (dateStats.getMinimum() < minimum) {
            minimum = dateStats.getMinimum();
          }
        }
      }
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      ColumnStatistics::toProtoBuf(pbStats);

      if (hasMinimumValue) {
        proto::DateStatistics* dateStatistics = pbStats.mutable_datestatistics();
        dateStatistics->set_maximum(maximum);
        dateStatistics->set_minimum(minimum);
      }
    }

    void update(int32_t value) override {
      if (!hasMinimumValue) {
        maximum = minimum = value;
        hasMaximumValue = hasMinimumValue = true;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Date" << std::endl
             << "Values: " << valueCount << std::endl
             << "Has null: " << (hasNullValue ? "yes" : "no") << std::endl;
      if(hasMinimumValue){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(hasMaximumValue){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DecimalColumnStatisticsImpl: public DecimalColumnStatistics {
  private:
    bool hasMinimumValue;
    bool hasMaximumValue;
    bool hasSumValue;
    Decimal minimum;
    Decimal maximum;
    Decimal sum;

  public:
    DecimalColumnStatisticsImpl(): minimum(0, 0),
                                   maximum(0, 0),
                                   sum(0, 0) {
      reset();
    }
    DecimalColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                bool correctStats);
    virtual ~DecimalColumnStatisticsImpl();

    bool hasMinimum() const override {
      return hasMinimumValue;
    }

    bool hasMaximum() const override {
      return hasMaximumValue;
    }

    bool hasSum() const override {
      return hasSumValue;
    }

    Decimal getMinimum() const override {
      if(hasMinimumValue){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    Decimal getMaximum() const override {
      if(hasMaximumValue){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    Decimal getSum() const override {
      if(hasSumValue){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    void setMinimum(Decimal min) override {
      this->hasMinimumValue = true;
      minimum = min;
    }

    void setMaximum(Decimal max) override {
      this->hasMaximumValue = true;
      maximum = max;
    }

    void setSum(Decimal newSum) override {
      this->hasSumValue = true;
      sum = newSum;
    }

    void setHasSum(bool hasSum) override {
      this->hasSumValue = hasSum;
    }

    void reset() override {
      ColumnStatistics::reset();
      hasMinimumValue = false;
      hasMaximumValue = false;
      hasSumValue = true;
      maximum = Decimal(0, 0);
      minimum = Decimal(0, 0);
      sum = Decimal(0, 0);
    }

    void merge(const ColumnStatistics& other) override {
      ColumnStatistics::merge(other);

      const DecimalColumnStatistics& decStats =
        dynamic_cast<const DecimalColumnStatistics&>(other);

      if (decStats.hasMinimum()) {
        if (!hasMinimumValue) {
          hasMinimumValue = hasMaximumValue = true;
          minimum = decStats.getMinimum();
          maximum = decStats.getMaximum();
        } else {
          if (decimalCompare(maximum.value,
                             maximum.scale,
                             decStats.getMaximum().value,
                             decStats.getMaximum().scale) < 0) {
            maximum = decStats.getMaximum();
          }
          if (decimalCompare(minimum.value,
                             minimum.scale,
                             decStats.getMinimum().value,
                             decStats.getMinimum().scale) > 0) {
            minimum = decStats.getMinimum();
          }
        }
      }

      // hasSumValue here means no overflow
      hasSumValue &= decStats.hasSum();
      if (hasSumValue) {
        updateSum(decStats.getSum().value, decStats.getSum().scale);
      }
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      ColumnStatistics::toProtoBuf(pbStats);

      proto::DecimalStatistics* decStats = pbStats.mutable_decimalstatistics();
      if (hasMinimumValue) {
        decStats->set_minimum(minimum.toString());
        decStats->set_maximum(maximum.toString());
      }
      if (hasSumValue) {
        decStats->set_sum(sum.toString());
      }
    }

    void update(const Decimal& decimal) override {
      update(decimal.value, decimal.scale);
    }

    void update(int64_t value, int32_t scale) override {
      update(Int128(value), scale);
    }

    void update(Int128 value, int32_t scale) override {
      if (!hasMinimumValue) {
        hasMinimumValue = hasMaximumValue = true;
        minimum = maximum = Decimal(value, scale);
      } else {
        if (decimalCompare(value,
                           scale,
                           minimum.value,
                           minimum.scale) < 0) {
          minimum = Decimal(value, scale);
        } else if (decimalCompare(maximum.value,
                                  maximum.scale,
                                  value,
                                  scale) < 0) {
          maximum = Decimal(value, scale);
        }
      }

      if (hasSumValue) {
        updateSum(value, scale);
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Decimal" << std::endl
             << "Values: " << valueCount << std::endl
             << "Has null: " << (hasNullValue ? "yes" : "no") << std::endl;
      if(hasMinimumValue){
        buffer << "Minimum: " << minimum.toString() << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(hasMaximumValue){
        buffer << "Maximum: " << maximum.toString() << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(hasSumValue){
        buffer << "Sum: " << sum.toString() << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }

      return buffer.str();
    }

  private:
    void updateSum(Int128 value, int32_t scale) {
      if (hasSumValue) {
        bool overflow = false;
        if (sum.scale > scale) {
          value = scaleInt128(value, sum.scale - scale, overflow);
        } else if (sum.scale < scale) {
          sum.value = scaleInt128(sum.value, scale - sum.scale, overflow);
          sum.scale = scale;
        }

        if (!overflow) {
          bool wasPositive = sum.value >= 0;
          sum.value += value;
          if ((value >= 0) == wasPositive) {
            hasSumValue = (sum.value >= 0) == wasPositive;
          }
        } else {
          hasSumValue = false;
        }
      }
    }
  };

  class DoubleColumnStatisticsImpl: public DoubleColumnStatistics {
  private:
    bool hasMinimumValue;
    bool hasMaximumValue;
    bool hasSumValue;
    double minimum;
    double maximum;
    double sum;

  public:
    DoubleColumnStatisticsImpl() {
      reset();
    }
    DoubleColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~DoubleColumnStatisticsImpl();

    bool hasMinimum() const override {
      return hasMinimumValue;
    }

    bool hasMaximum() const override {
      return hasMaximumValue;
    }

    bool hasSum() const override {
      return hasSumValue;
    }

    double getMinimum() const override {
      if(hasMinimumValue){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    double getMaximum() const override {
      if(hasMaximumValue){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    double getSum() const override {
      if(hasSumValue){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    void setMinimum(double min) override {
      this->minimum = min;
      this->hasMinimumValue = true;
    }

    void setMaximum(double max) override {
      this->maximum = max;
      this->hasMaximumValue = true;
    }

    void setSum(double newSum) override {
      this->sum = newSum;
      this->hasSumValue = true;
    }

    void setHasSum(bool hasSum) override {
      this->hasSumValue = hasSum;
    }

    void reset() override {
      ColumnStatistics::reset();
      hasMinimumValue = false;
      hasMaximumValue = false;
      minimum = std::numeric_limits<double>::min();
      maximum = std::numeric_limits<double>::max();
      hasSumValue = true;
      sum = 0.0;
    }

    void merge(const ColumnStatistics& other) override {
      ColumnStatistics::merge(other);

      const DoubleColumnStatistics& doubleColStats =
        dynamic_cast<const DoubleColumnStatistics&>(other);

      if (doubleColStats.hasMinimum()) {
        if (!hasMinimumValue) {
          hasMinimumValue = hasMaximumValue = true;
          minimum = doubleColStats.getMinimum();
          maximum = doubleColStats.getMaximum();
        } else {
          if (doubleColStats.getMaximum() > maximum) {
            maximum = doubleColStats.getMaximum();
          }
          if (doubleColStats.getMinimum() < minimum) {
            minimum = doubleColStats.getMinimum();
          }
        }
      }

      sum += doubleColStats.getSum();
    }

    void update(double value) override {
      if (!hasMinimumValue) {
        maximum = minimum = value;
        hasMaximumValue = hasMinimumValue = true;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      sum += value;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      ColumnStatistics::toProtoBuf(pbStats);

      proto::DoubleStatistics* doubleStats = pbStats.mutable_doublestatistics();
      if (hasMinimumValue) {
        doubleStats->set_minimum(minimum);
        doubleStats->set_maximum(maximum);
      }
      if (hasSumValue) {
        doubleStats->set_sum(sum);
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Double" << std::endl
             << "Values: " << valueCount << std::endl
             << "Has null: " << (hasNullValue ? "yes" : "no") << std::endl;
      if(hasMinimumValue){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(hasMaximumValue){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(hasSumValue){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class IntegerColumnStatisticsImpl: public IntegerColumnStatistics {
  private:
    bool hasMinimumValue;
    bool hasMaximumValue;
    bool hasSumValue;
    int64_t minimum;
    int64_t maximum;
    int64_t sum;

  public:
    IntegerColumnStatisticsImpl() {
      reset();
    }
    IntegerColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~IntegerColumnStatisticsImpl();

    bool hasMinimum() const override {
      return hasMinimumValue;
    }

    bool hasMaximum() const override {
      return hasMaximumValue;
    }

    bool hasSum() const override {
      return hasSumValue;
    }

    int64_t getMinimum() const override {
      if(hasMinimumValue){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(hasMaximumValue){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    int64_t getSum() const override {
      if(hasSumValue){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    void setMinimum(int64_t min) override {
      this->minimum = min;
      this->hasMinimumValue = true;
    }

    void setMaximum(int64_t max) override {
      this->maximum = max;
      this->hasMaximumValue = true;
    }

    void setSum(int64_t newSum) override {
      this->sum = newSum;
      this->hasSumValue = true;
    }

    void setHasSum(bool hasSum) override {
      this->hasSumValue = hasSum;
    }

    void reset() override {
      ColumnStatistics::reset();
      hasMinimumValue = false;
      hasMaximumValue = false;
      minimum = std::numeric_limits<int64_t>::min();
      maximum = std::numeric_limits<int64_t>::max();
      hasSumValue = true;
      sum = 0;
    }

    void merge(const ColumnStatistics& other) override {
      ColumnStatistics::merge(other);

      const IntegerColumnStatistics& intColStats =
        dynamic_cast<const IntegerColumnStatistics&>(other);

      if (intColStats.hasMinimum()) {
        if (!hasMinimumValue) {
          hasMinimumValue = hasMaximumValue = true;
          minimum = intColStats.getMinimum();
          maximum = intColStats.getMaximum();
        } else {
          if (intColStats.getMaximum() > maximum) {
            maximum = intColStats.getMaximum();
          }
          if (intColStats.getMinimum() < minimum) {
            minimum = intColStats.getMinimum();
          }
        }
      }

      // hasSumValue here means no overflow
      hasSumValue &= intColStats.hasSum();
      if (hasSumValue) {
        bool wasPositive = sum >= 0;
        sum += intColStats.getSum();
        if ((intColStats.getSum() >= 0) == wasPositive) {
          hasSumValue = (sum >= 0) == wasPositive;
        }
      }
    }

    void update(int64_t value, int repetitions) override {
      if (!hasMinimumValue) {
        maximum = minimum = value;
        hasMaximumValue = hasMinimumValue = true;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      if (hasSumValue) {
        bool wasPositive = sum >= 0;
        sum += value * repetitions;
        if ((value >= 0) == wasPositive) {
          hasSumValue = (sum >= 0) == wasPositive;
        }
      }
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      ColumnStatistics::toProtoBuf(pbStats);

      proto::IntegerStatistics* intStats = pbStats.mutable_intstatistics();
      if (hasMinimumValue) {
        intStats->set_minimum(minimum);
        intStats->set_maximum(maximum);
      }
      if (hasSumValue) {
        intStats->set_sum(sum);
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Integer" << std::endl
             << "Values: " << valueCount << std::endl
             << "Has null: " << (hasNullValue ? "yes" : "no") << std::endl;

      if(hasMinimumValue){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(hasMaximumValue){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(hasSumValue){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };


  class StringColumnStatisticsImpl: public StringColumnStatistics {
  private:
    bool hasMinimumValue;
    bool hasMaximumValue;
    bool hasTotalLen;
    std::string minimum;
    std::string maximum;
    uint64_t totalLength;

  public:
    StringColumnStatisticsImpl(bool enableStrComparision) {
      enableStringComparison = enableStrComparision;
      reset();
    }
    StringColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                               bool correctStats);
    virtual ~StringColumnStatisticsImpl();

    bool hasMinimum() const override {
      return hasMinimumValue;
    }

    bool hasMaximum() const override {
      return hasMaximumValue;
    }

    bool hasTotalLength() const override {
      return hasTotalLen;
    }

    std::string getMinimum() const override {
      if(hasMinimumValue){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    std::string getMaximum() const override {
      if(hasMaximumValue){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    uint64_t getTotalLength() const override {
      if(hasTotalLen){
        return totalLength;
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    void setMinimum(std::string min) override {
      this->minimum = min;
      this->hasMinimumValue = true;
    }

    void setMaximum(std::string max) override {
      this->maximum = max;
      this->hasMaximumValue = true;
    }

    void setTotalLength(uint64_t newTotalLength) override {
      this->totalLength = newTotalLength;
      this->hasTotalLen = true;
    }

    void setHasTotalLength(bool newHasTotalLength) override {
      this->hasTotalLen = newHasTotalLength;
    }

    void reset() override {
      ColumnStatistics::reset();
      hasMinimumValue = false;
      hasMaximumValue = false;
      minimum = std::string();
      maximum = std::string();
      hasTotalLen = true;
      totalLength = 0;
    }

    void merge(const ColumnStatistics& other) override {
      ColumnStatistics::merge(other);

      const StringColumnStatistics& strColStats =
        dynamic_cast<const StringColumnStatistics&>(other);

      if (enableStringComparison) {
        if (strColStats.hasMinimum()) {
          if (!hasMinimumValue) {
            hasMinimumValue = hasMaximumValue = true;
            minimum = strColStats.getMinimum();
            maximum = strColStats.getMaximum();
          } else {
            if (strColStats.getMaximum() > maximum) {
              maximum = strColStats.getMaximum();
            }
            if (strColStats.getMinimum() < minimum) {
              minimum = strColStats.getMinimum();
            }
          }
        }
      }

      totalLength += strColStats.getTotalLength();
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      ColumnStatistics::toProtoBuf(pbStats);

      proto::StringStatistics* strStats = pbStats.mutable_stringstatistics();
      if (hasMinimumValue) {
        strStats->set_minimum(minimum);
        strStats->set_maximum(maximum);
      }

      strStats->set_sum(static_cast<int64_t>(totalLength));
    }

    void update(const std::string& value) override {
      if (enableStringComparison) {
        if (!hasMinimumValue) {
          maximum = minimum = value;
          hasMaximumValue = hasMinimumValue = true;
        } else if (value < minimum) {
          minimum = value;
        } else if (value > maximum) {
          maximum = value;
        }
      }

      totalLength += value.length();
    }

    void update(const char* value, size_t length) override {
      if (enableStringComparison && value != nullptr) {
        if (!hasMinimumValue) {
          maximum = minimum = std::string(value, value + length);
          hasMaximumValue = hasMinimumValue = true;
        } else {
          // update min
          int minCmp = strncmp(minimum.c_str(),
                               value,
                               std::min(minimum.length(), length));
          if (minCmp > 0 || (minCmp == 0 && length < minimum.length())) {
            minimum = std::string(value, value + length);
          }

          // update max
          int maxCmp = strncmp(maximum.c_str(),
                               value,
                               std::min(maximum.length(), length));
          if (maxCmp < 0 || (maxCmp == 0 && length > minimum.length())) {
            maximum = std::string(value, value + length);
          }
        }
      }

      totalLength += length;
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: String" << std::endl
             << "Values: " << valueCount << std::endl
             << "Has null: " << (hasNullValue ? "yes" : "no") << std::endl;
      if(hasMinimumValue){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(hasMaximumValue){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }

      if(hasTotalLen){
        buffer << "Total length: " << totalLength << std::endl;
      }else{
        buffer << "Total length is not defined" << std::endl;
      }
      return buffer.str();
    }

  private:
    // a flag to enable string comparision for min/max as it is very
    // time-consuming, can be off by default
    bool enableStringComparison;
  };

  class TimestampColumnStatisticsImpl: public TimestampColumnStatistics {
  private:
    bool hasMinimumValue;
    bool hasMaximumValue;
    int64_t minimum;
    int64_t maximum;

  public:
    TimestampColumnStatisticsImpl() {
      reset();
    }
    TimestampColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                  bool correctStats);
    virtual ~TimestampColumnStatisticsImpl();

    bool hasMinimum() const override {
      return hasMinimumValue;
    }

    bool hasMaximum() const override {
      return hasMaximumValue;
    }

    int64_t getMinimum() const override {
      if(hasMinimumValue){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(hasMaximumValue){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    void setMinimum(int64_t min) override {
      this->minimum = min;
      this->hasMinimumValue = true;
    }

    void setMaximum(int64_t max) override {
      this->maximum = max;
      this->hasMaximumValue = true;
    }

    void reset() override {
      ColumnStatistics::reset();
      hasMinimumValue = false;
      hasMaximumValue = false;
      minimum = 0;
      maximum = 0;
    }

    void merge(const ColumnStatistics& other) override {
      ColumnStatistics::merge(other);

      const TimestampColumnStatistics& tsStats =
        dynamic_cast<const TimestampColumnStatistics &>(other);

      if (tsStats.hasMinimum()) {
        if (!hasMinimumValue) {
          hasMinimumValue = hasMaximumValue = true;
          minimum = tsStats.getMinimum();
          maximum = tsStats.getMaximum();
        } else {
          if (tsStats.getMaximum() > maximum) {
            maximum = tsStats.getMaximum();
          }
          if (tsStats.getMinimum() < minimum) {
            minimum = tsStats.getMinimum();
          }
        }
      }
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      ColumnStatistics::toProtoBuf(pbStats);

      if (hasMinimumValue) {
        proto::TimestampStatistics* timestampStatistics =
          pbStats.mutable_timestampstatistics();

        // ORC-135: min and max are deprecated, store UTC instead
        timestampStatistics->set_maximumutc(maximum);
        timestampStatistics->set_minimumutc(minimum);
      }
    }

    void update(int64_t value) override {
      if (!hasMinimumValue) {
        maximum = minimum = value;
        hasMaximumValue = hasMinimumValue = true;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Timestamp" << std::endl
             << "Values: " << valueCount << std::endl
             << "Has null: " << (hasNullValue ? "yes" : "no") << std::endl;
      if(hasMinimumValue){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(hasMaximumValue){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  ColumnStatistics* convertColumnStatistics(const proto::ColumnStatistics& s,
                                            bool correctStats);

  class StatisticsImpl: public Statistics {
  private:
    std::list<ColumnStatistics*> colStats;

    // DELIBERATELY NOT IMPLEMENTED
    StatisticsImpl(const StatisticsImpl&);
    StatisticsImpl& operator=(const StatisticsImpl&);

  public:
    StatisticsImpl(const proto::StripeStatistics& stripeStats, bool correctStats);

    StatisticsImpl(const proto::Footer& footer, bool correctStats);

    virtual const ColumnStatistics* getColumnStatistics(uint32_t columnId
                                                        ) const override {
      std::list<ColumnStatistics*>::const_iterator it = colStats.begin();
      std::advance(it, static_cast<int64_t>(columnId));
      return *it;
    }

    virtual ~StatisticsImpl();

    uint32_t getNumberOfColumns() const override {
      return static_cast<uint32_t>(colStats.size());
    }
  };

}// namespace

#endif
