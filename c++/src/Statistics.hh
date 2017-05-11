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

#include "Timezone.hh"
#include "TypeImpl.hh"

namespace orc {

/**
 * StatContext contains fields required to compute statistics
 */

  struct StatContext {
    const bool correctStats;
    const Timezone* const writerTimezone;
    StatContext() : correctStats(false), writerTimezone(NULL) {}
    StatContext(bool cStat, const Timezone* const timezone = NULL) :
        correctStats(cStat), writerTimezone(timezone) {}
  };

/**
 * ColumnStatistics Implementation
 */

  class ColumnStatisticsImpl: public ColumnStatistics {
  private:
    uint64_t valueCount;
    bool _hasNull;

  public:
    ColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    ColumnStatisticsImpl() {
      reset();
    }
    virtual ~ColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();
    }

    void reset() override {
      _hasNull = false;
      valueCount = 0;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Column has " << valueCount << " values" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl;
      return buffer.str();
    }
  };

  class BinaryColumnStatisticsImpl: public BinaryColumnStatistics {
  private:
    bool _hasNull;
    bool _hasTotalLength;
    uint64_t valueCount;
    uint64_t totalLength;

  public:
    BinaryColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                               const StatContext& statContext);
    BinaryColumnStatisticsImpl() {
      reset();
    }
    virtual ~BinaryColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    bool hasTotalLength() const override {
      return _hasTotalLength;
    }
    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    uint64_t getTotalLength() const override {
      if(_hasTotalLength){
        return totalLength;
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    void update(size_t length) override {
      totalLength += length;
    }

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();

      const BinaryColumnStatistics& binStats =
        dynamic_cast<const BinaryColumnStatistics&>(other);
      totalLength += binStats.getTotalLength();
    }

    void reset() override {
      _hasNull = false;
      _hasTotalLength = true;
      valueCount = 0;
      totalLength = 0;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);

      proto::BinaryStatistics* binStats = pbStats.mutable_binarystatistics();
      binStats->set_sum(static_cast<int64_t>(totalLength));
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Binary" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasTotalLength){
        buffer << "Total length: " << totalLength << std::endl;
      }else{
        buffer << "Total length: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class BooleanColumnStatisticsImpl: public BooleanColumnStatistics {
  private:
    bool _hasNull;
    bool _hasCount;
    uint64_t valueCount;
    uint64_t trueCount;

  public:
    BooleanColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                const StatContext& statContext);
    BooleanColumnStatisticsImpl() {
      reset();
    }
    virtual ~BooleanColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    bool hasCount() const override {
      return _hasCount;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    uint64_t getFalseCount() const override {
      if(_hasCount){
        return valueCount - trueCount;
      }else{
        throw ParseError("False count is not defined.");
      }
    }

    uint64_t getTrueCount() const override {
      if(_hasCount){
        return trueCount;
      }else{
        throw ParseError("True count is not defined.");
      }
    }

    void update(bool value, size_t repetitions) override {
      if (value) {
        trueCount += repetitions;
      }
    }

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();

      const BooleanColumnStatistics& boolStats =
        dynamic_cast<const BooleanColumnStatistics&>(other);
      trueCount += boolStats.getTrueCount();
    }

    void reset() override {
      _hasNull = false;
      _hasCount = true;
      valueCount = 0;
      trueCount = 0;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);

      proto::BucketStatistics* bucketStats = pbStats.mutable_bucketstatistics();
      if (_hasCount) {
        bucketStats->add_count(trueCount);
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Boolean" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasCount){
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
    bool _hasNull;
    bool _hasMinimum;
    bool _hasMaximum;
    uint64_t valueCount;
    int32_t minimum;
    int32_t maximum;

  public:
    DateColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                             const StatContext& statContext);
    DateColumnStatisticsImpl() {
      reset();
    }
    virtual ~DateColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    int32_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int32_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    void update(int32_t value) override {
      if (!_hasMinimum) {
        maximum = minimum = value;
        _hasMinimum = _hasMaximum = true;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
    }

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();

      const DateColumnStatistics& dateStats =
        dynamic_cast<const DateColumnStatistics &>(other);

      if (dateStats.hasMinimum()) {
        if (!_hasMinimum) {
          _hasMinimum = _hasMaximum = true;
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

    void reset() override {
      _hasNull = false;
      _hasMaximum = false;
      _hasMinimum = false;
      valueCount = 0;
      minimum = 0;
      maximum = 0;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);

      if (_hasMinimum) {
        proto::DateStatistics* dateStatistics = pbStats.mutable_datestatistics();
        dateStatistics->set_maximum(maximum);
        dateStatistics->set_minimum(minimum);
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Date" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DecimalColumnStatisticsImpl: public DecimalColumnStatistics {
  private:
    bool _hasNull;
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    Decimal minimum;
    Decimal maximum;
    Decimal sum;

  public:
    DecimalColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                const StatContext& statContext);
    DecimalColumnStatisticsImpl() {
      reset();
    }
    virtual ~DecimalColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    Decimal getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    Decimal getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    Decimal getSum() const override {
      if(_hasSum){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    void update(const Decimal& value) override {
      if (!_hasMinimum) {
        _hasMinimum = _hasMaximum = true;
        minimum = maximum = value;
      } else {
        if (value < minimum) {
          minimum = value;
        } else if (maximum < value) {
          maximum = value;
        }
      }

      if (_hasSum) {
        updateSum(value);
      }
    }

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();

      const DecimalColumnStatistics& decStats =
        dynamic_cast<const DecimalColumnStatistics&>(other);

      if (decStats.hasMinimum()) {
        if (!_hasMinimum) {
          _hasMinimum = _hasMaximum = true;
          minimum = decStats.getMinimum();
          maximum = decStats.getMaximum();
        } else {
          if (maximum < decStats.getMaximum()) {
            maximum = decStats.getMaximum();
          }
          if (decStats.getMinimum() < minimum) {
            minimum = decStats.getMinimum();
          }
        }
      }

      // _hasSum here means no overflow
      _hasSum &= decStats.hasSum();
      if (_hasSum) {
        updateSum(decStats.getSum());
      }
    }

    void reset() override {
      _hasNull = false;
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = true;
      valueCount = 0;
      sum = Decimal();
      maximum = Decimal();
      minimum = Decimal();
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);

      proto::DecimalStatistics* decStats = pbStats.mutable_decimalstatistics();
      if (_hasMinimum) {
        decStats->set_minimum(minimum.toString());
        decStats->set_maximum(maximum.toString());
      }
      if (_hasSum) {
        decStats->set_sum(sum.toString());
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Decimal" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum.toString() << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum.toString() << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum.toString() << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }

      return buffer.str();
    }

  private:
    void updateSum(Decimal value) {
      if (_hasSum) {
        bool overflow = false;
        if (sum.scale > value.scale) {
          value.value = scaleUpInt128ByPowerOfTen(value.value,
                                                  sum.scale - value.scale,
                                                  overflow);
        } else if (sum.scale < value.scale) {
          sum.value = scaleUpInt128ByPowerOfTen(sum.value,
                                                value.scale - sum.scale,
                                                overflow);
          sum.scale = value.scale;
        }

        if (!overflow) {
          bool wasPositive = sum.value >= 0;
          sum.value += value.value;
          if ((value.value >= 0) == wasPositive) {
            _hasSum = (sum.value >= 0) == wasPositive;
          }
        } else {
          _hasSum = false;
        }
      }
    }
  };

  class DoubleColumnStatisticsImpl: public DoubleColumnStatistics {
  private:
    bool _hasNull;
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    double minimum;
    double maximum;
    double sum;

  public:
    DoubleColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    DoubleColumnStatisticsImpl() {
      reset();
    }
    virtual ~DoubleColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    double getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    double getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    double getSum() const override {
      if(_hasSum){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    void update(double value) override {
      if (!_hasMinimum) {
        maximum = minimum = value;
        _hasMinimum = _hasMaximum = true;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      sum += value;
    }

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();

      const DoubleColumnStatistics& doubleStats =
        dynamic_cast<const DoubleColumnStatistics&>(other);

      if (doubleStats.hasMinimum()) {
        if (!_hasMinimum) {
          _hasMinimum = _hasMaximum = true;
          minimum = doubleStats.getMinimum();
          maximum = doubleStats.getMaximum();
        } else {
          if (doubleStats.getMaximum() > maximum) {
            maximum = doubleStats.getMaximum();
          }
          if (doubleStats.getMinimum() < minimum) {
            minimum = doubleStats.getMinimum();
          }
        }
      }

      sum += doubleStats.getSum();
    }

    void reset() override {
      _hasNull = false;
      _hasMaximum = false;
      _hasMinimum = false;
      _hasSum = true;
      valueCount = 0;
      minimum = 0.0;
      maximum = 0.0;
      sum = 0.0;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);

      proto::DoubleStatistics* doubleStats = pbStats.mutable_doublestatistics();
      if (_hasMinimum) {
        doubleStats->set_minimum(minimum);
        doubleStats->set_maximum(maximum);
      }
      if (_hasSum) {
        doubleStats->set_sum(sum);
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Double" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class IntegerColumnStatisticsImpl: public IntegerColumnStatistics {
  private:
    bool _hasNull;
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    int64_t minimum;
    int64_t maximum;
    int64_t sum;

  public:
    IntegerColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    IntegerColumnStatisticsImpl() {
      reset();
    }
    virtual ~IntegerColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    int64_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    int64_t getSum() const override {
      if(_hasSum){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    void update(int64_t value, int repetitions) override {
      if (!_hasMinimum) {
        maximum = minimum = value;
        _hasMinimum = _hasMaximum = true;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      if (_hasSum) {
        bool wasPositive = sum >= 0;
        sum += value * repetitions;
        if ((value >= 0) == wasPositive) {
          _hasSum = (sum >= 0) == wasPositive;
        }
      }
    }

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();

      const IntegerColumnStatistics& intStats =
        dynamic_cast<const IntegerColumnStatistics&>(other);

      if (intStats.hasMinimum()) {
        if (!_hasMinimum) {
          _hasMinimum = _hasMaximum = true;
          minimum = intStats.getMinimum();
          maximum = intStats.getMaximum();
        } else {
          if (intStats.getMaximum() > maximum) {
            maximum = intStats.getMaximum();
          }
          if (intStats.getMinimum() < minimum) {
            minimum = intStats.getMinimum();
          }
        }
      }

      // _hasSum here means no overflow
      _hasSum &= intStats.hasSum();
      if (_hasSum) {
        bool wasPositive = sum >= 0;
        sum += intStats.getSum();
        if ((intStats.getSum() >= 0) == wasPositive) {
          _hasSum = (sum >= 0) == wasPositive;
        }
      }
    }

    void reset() override {
      _hasNull = false;
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = true;
      valueCount = 0;
      minimum = 0;
      maximum = 0;
      sum = 0;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);

      proto::IntegerStatistics* intStats = pbStats.mutable_intstatistics();
      if (_hasMinimum) {
        intStats->set_minimum(minimum);
        intStats->set_maximum(maximum);
      }
      if (_hasSum) {
        intStats->set_sum(sum);
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Integer" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class StringColumnStatisticsImpl: public StringColumnStatistics {
  private:
    bool _hasNull;
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasTotalLength;
    uint64_t valueCount;
    std::string minimum;
    std::string maximum;
    uint64_t totalLength;

    // flag for string comparision for min/max as it is very time-consuming
    bool enableStringComparison;

  public:
    StringColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                               const StatContext& statContext);
    StringColumnStatisticsImpl(bool enable) {
      enableStringComparison = enable;
      reset();
    }
    virtual ~StringColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasTotalLength() const override {
      return _hasTotalLength;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    std::string getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    std::string getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    uint64_t getTotalLength() const override {
      if(_hasTotalLength){
        return totalLength;
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    void update(const std::string& value) override {
      if (enableStringComparison) {
        if (!_hasMinimum) {
          maximum = minimum = value;
          _hasMinimum = _hasMaximum = true;
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
        if (!_hasMinimum) {
          maximum = minimum = std::string(value, value + length);
          _hasMinimum = _hasMaximum = true;
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

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();

      const StringColumnStatistics& strStats =
        dynamic_cast<const StringColumnStatistics&>(other);

      if (enableStringComparison) {
        if (strStats.hasMinimum()) {
          if (!_hasMinimum) {
            _hasMinimum = _hasMaximum = true;
            minimum = strStats.getMinimum();
            maximum = strStats.getMaximum();
          } else {
            if (strStats.getMaximum() > maximum) {
              maximum = strStats.getMaximum();
            }
            if (strStats.getMinimum() < minimum) {
              minimum = strStats.getMinimum();
            }
          }
        }
      }

      totalLength += strStats.getTotalLength();
    }

    void reset() override {
      _hasNull = false;
      _hasMaximum = false;
      _hasMinimum = false;
      _hasTotalLength = true;
      valueCount = 0;
      minimum = "";
      maximum = "";
      totalLength = 0;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);

      proto::StringStatistics* strStats = pbStats.mutable_stringstatistics();
      if (_hasMinimum) {
        strStats->set_minimum(minimum);
        strStats->set_maximum(maximum);
      }
      strStats->set_sum(static_cast<int64_t>(totalLength));
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: String" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }

      if(_hasTotalLength){
        buffer << "Total length: " << totalLength << std::endl;
      }else{
        buffer << "Total length is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class TimestampColumnStatisticsImpl: public TimestampColumnStatistics {
  private:
    bool _hasNull;
    bool _hasMinimum;
    bool _hasMaximum;
    uint64_t valueCount;
    int64_t minimum;
    int64_t maximum;
    bool _hasLowerBound;
    bool _hasUpperBound;
    int64_t lowerBound;
    int64_t upperBound;

  public:
    TimestampColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                  const StatContext& statContext);
    TimestampColumnStatisticsImpl() {
      reset();
    }
    virtual ~TimestampColumnStatisticsImpl();

    bool hasNull() const override {
      return _hasNull;
    }

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    void increase(uint64_t count) override {
      valueCount += count;
    }

    int64_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    void update(int64_t value) override {
      if (!_hasMinimum) {
        maximum = minimum = value;
        _hasMinimum = _hasMaximum = true;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
    }

    void merge(const ColumnStatistics& other) override {
      _hasNull |= other.hasNull();
      valueCount += other.getNumberOfValues();

      const TimestampColumnStatistics& tsStats =
        dynamic_cast<const TimestampColumnStatistics &>(other);

      if (tsStats.hasMinimum()) {
        if (!_hasMinimum) {
          _hasMinimum = _hasMaximum = true;
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

    void reset() override {
      _hasNull = false;
      _hasMinimum = false;
      _hasMaximum = false;
      _hasLowerBound = false;
      _hasUpperBound = false;
      valueCount = 0;
      minimum = 0;
      maximum = 0;
      lowerBound = 0;
      upperBound = 0;
    }

    void toProtoBuf(proto::ColumnStatistics& pbStats) const override {
      pbStats.set_hasnull(_hasNull);
      pbStats.set_numberofvalues(valueCount);

      if (_hasMinimum) {
        proto::TimestampStatistics* timestampStatistics =
          pbStats.mutable_timestampstatistics();

        timestampStatistics->set_maximumutc(maximum);
        timestampStatistics->set_minimumutc(minimum);
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      struct tm tmValue;
      char timeBuffer[20];
      time_t secs = 0;

      buffer << "Data type: Timestamp" << std::endl
             << "Has null: " << (_hasNull ? "yes" : "no") << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        secs = static_cast<time_t>(minimum/1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "Minimum: " << timeBuffer << "." << (minimum % 1000) << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(_hasLowerBound){
        secs = static_cast<time_t>(lowerBound/1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "LowerBound: " << timeBuffer << "." << (lowerBound % 1000) << std::endl;
      }else{
        buffer << "LowerBound is not defined" << std::endl;
      }

      if(_hasMaximum){
        secs = static_cast<time_t>(maximum/1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "Maximum: " << timeBuffer << "." << (maximum % 1000) << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }

      if(_hasUpperBound){
        secs = static_cast<time_t>(upperBound/1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "UpperBound: " << timeBuffer << "." << (upperBound % 1000) << std::endl;
      }else{
        buffer << "UpperBound is not defined" << std::endl;
      }

      return buffer.str();
    }

    bool hasLowerBound() const override {
      return _hasLowerBound;
    }

    bool hasUpperBound() const override {
      return _hasUpperBound;
    }

    int64_t getLowerBound() const override {
      if(_hasLowerBound){
        return lowerBound;
      }else{
        throw ParseError("LowerBound is not defined.");
      }
    }

    int64_t getUpperBound() const override {
      if(_hasUpperBound){
        return upperBound;
      }else{
        throw ParseError("UpperBound is not defined.");
      }
    }
  };


  ColumnStatistics* convertColumnStatistics(const proto::ColumnStatistics& s,
                                            const StatContext& statContext);

  class StatisticsImpl: public Statistics {
  private:
    std::list<ColumnStatistics*> colStats;

    // DELIBERATELY NOT IMPLEMENTED
    StatisticsImpl(const StatisticsImpl&);
    StatisticsImpl& operator=(const StatisticsImpl&);

  public:
    StatisticsImpl(const proto::StripeStatistics& stripeStats, const StatContext& statContext);

    StatisticsImpl(const proto::Footer& footer, const StatContext& statContext);

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
