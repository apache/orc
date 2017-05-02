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
 * Internal Statistics Implementation
 */

  template <typename T>
  class InternalStatisticsImpl {
  private:
    bool _hasNull;
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    bool _hasTotalLength;
    uint64_t _totalLength;
    uint64_t _valueCount;
    T _minimum;
    T _maximum;
    T _sum;
  public:
    InternalStatisticsImpl() {
      _hasNull = false;
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;
      _hasTotalLength = false;
      _totalLength = 0;
      _valueCount = 0;
    }

    ~InternalStatisticsImpl() {}

    // GET / SET totalLength_
    bool hasTotalLength() const { return _hasTotalLength; }

    void setHasTotalLength(bool hasTotalLength) { _hasTotalLength = hasTotalLength; }

    uint64_t getTotalLength() const { return _totalLength; }

    void setTotalLength(uint64_t totalLength) { _totalLength = totalLength; }

    // GET / SET sum_
    bool hasSum() const { return _hasSum; }

    void setHasSum(bool hasSum) { _hasSum = hasSum; }

    T getSum() const { return _sum; }

    void setSum(T sum) { _sum = sum; }

    // GET / SET maximum_
    bool hasMaximum() const { return _hasMaximum; }

    T getMaximum() const { return _maximum; }

    void setHasMaximum(bool hasMax) { _hasMaximum = hasMax; }

    void setMaximum(T max) { _maximum = max; }

    // GET / SET minimum_
    bool hasMinimum() const { return _hasMinimum; }

    void setHasMinimum(bool hasMin) { _hasMinimum = hasMin; }

    T getMinimum() const { return _minimum; }

    void setMinimum(T min) { _minimum = min; }

    // GET / SET valueCount_
    uint64_t getNumberOfValues() const { return _valueCount; }

    void setNumberOfValues(uint64_t numValues) { _valueCount = numValues; }

    // GET / SET hasNullValue_
    bool hasNull() const { return _hasNull; }

    void setHasNull(bool hasNull) { _hasNull = hasNull; }
   };

  typedef InternalStatisticsImpl<char> InternalCharStatistics;
  typedef InternalStatisticsImpl<uint64_t> InternalBooleanStatistics;
  typedef InternalStatisticsImpl<int64_t> InternalIntegerStatistics;
  typedef InternalStatisticsImpl<int32_t> InternalDateStatistics;
  typedef InternalStatisticsImpl<double> InternalDoubleStatistics;
  typedef InternalStatisticsImpl<Decimal> InternalDecimalStatistics;
  typedef InternalStatisticsImpl<std::string> InternalStringStatistics;

/**
 * ColumnStatistics Implementation
 */

  class ColumnStatisticsImpl: public ColumnStatistics {
  private:
    InternalCharStatistics _stats;
  public:
    ColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~ColumnStatisticsImpl();

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Column has " << getNumberOfValues() << " values"
             << " and has null value: " << (hasNull() ? "yes" : "no")
             << std::endl;
      return buffer.str();
    }
  };

  class BinaryColumnStatisticsImpl: public BinaryColumnStatistics {
  private:
    InternalCharStatistics _stats;
  public:
    BinaryColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                               const StatContext& statContext);
    virtual ~BinaryColumnStatisticsImpl();

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    bool hasTotalLength() const override {
      return _stats.hasTotalLength();
    }

    uint64_t getTotalLength() const override {
      if(hasTotalLength()){
        return _stats.getTotalLength();
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Binary" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if(hasTotalLength()){
        buffer << "Total length: " << getTotalLength() << std::endl;
      }else{
        buffer << "Total length: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class BooleanColumnStatisticsImpl: public BooleanColumnStatistics {
  private:
    InternalBooleanStatistics _stats;

  public:
    BooleanColumnStatisticsImpl(const proto::ColumnStatistics& stats, const StatContext& statContext);
    virtual ~BooleanColumnStatisticsImpl();

    bool hasCount() const override {
      return _stats.hasSum();
    }

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    uint64_t getFalseCount() const override {
      if(hasCount()){
        return getNumberOfValues() - _stats.getSum();
      }else{
        throw ParseError("False count is not defined.");
      }
    }

    uint64_t getTrueCount() const override {
      if(hasCount()){
        return _stats.getSum();
      }else{
        throw ParseError("True count is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Boolean" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if(hasCount()){
        buffer << "(true: " << getTrueCount() << "; false: "
               << getFalseCount() << ")" << std::endl;
      } else {
        buffer << "(true: not defined; false: not defined)" << std::endl;
        buffer << "True and false count are not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DateColumnStatisticsImpl: public DateColumnStatistics {
  private:
    InternalDateStatistics _stats; 
  public:
    DateColumnStatisticsImpl(const proto::ColumnStatistics& stats, const StatContext& statContext);
    virtual ~DateColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _stats.hasMinimum();
    }

    bool hasMaximum() const override {
      return _stats.hasMaximum();
    }

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    int32_t getMinimum() const override {
      if(hasMinimum()){
        return _stats.getMinimum();
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int32_t getMaximum() const override {
      if(hasMaximum()){
        return _stats.getMaximum();
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Date" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if(hasMinimum()){
        buffer << "Minimum: " << getMinimum() << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(hasMaximum()){
        buffer << "Maximum: " << getMaximum() << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DecimalColumnStatisticsImpl: public DecimalColumnStatistics {
  private:
    InternalDecimalStatistics _stats; 

  public:
    DecimalColumnStatisticsImpl(const proto::ColumnStatistics& stats, const StatContext& statContext);
    virtual ~DecimalColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _stats.hasMinimum();
    }

    bool hasMaximum() const override {
      return _stats.hasMaximum();
    }

    bool hasSum() const override {
      return _stats.hasSum();
    }

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    Decimal getMinimum() const override {
      if(hasMinimum()){
        return _stats.getMinimum();
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    Decimal getMaximum() const override {
      if(hasMaximum()){
        return _stats.getMaximum();
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    Decimal getSum() const override {
      if(hasSum()){
        return _stats.getSum();
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Decimal" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if(hasMinimum()){
        buffer << "Minimum: " << getMinimum().toString() << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(hasMaximum()){
        buffer << "Maximum: " << getMaximum().toString() << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(hasSum()){
        buffer << "Sum: " << getSum().toString() << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }

      return buffer.str();
    }
  };

  class DoubleColumnStatisticsImpl: public DoubleColumnStatistics {
  private:
    InternalDoubleStatistics _stats;
  public:
    DoubleColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~DoubleColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _stats.hasMinimum();
    }

    bool hasMaximum() const override {
      return _stats.hasMaximum();
    }

    bool hasSum() const override {
      return _stats.hasSum();
    }

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    double getMinimum() const override {
      if(hasMinimum()){
        return _stats.getMinimum();
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    double getMaximum() const override {
      if(hasMaximum()){
        return _stats.getMaximum();
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    double getSum() const override {
      if(hasSum()){
        return _stats.hasSum();
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Double" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if(hasMinimum()){
        buffer << "Minimum: " << getMinimum() << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(hasMaximum()){
        buffer << "Maximum: " << getMaximum() << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(hasSum()){
        buffer << "Sum: " << getSum() << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class IntegerColumnStatisticsImpl: public IntegerColumnStatistics {
  private:
    InternalIntegerStatistics _stats;
  public:
    IntegerColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~IntegerColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _stats.hasMinimum();
    }

    bool hasMaximum() const override {
      return _stats.hasMaximum();
    }

    bool hasSum() const override {
      return _stats.hasSum();
    }

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    int64_t getMinimum() const override {
      if(hasMinimum()){
        return _stats.getMinimum();
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(hasMaximum()){
        return _stats.getMaximum();
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    int64_t getSum() const override {
      if(hasSum()){
        return _stats.getSum();
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Integer" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if(hasMinimum()){
        buffer << "Minimum: " << getMinimum() << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(hasMaximum()){
        buffer << "Maximum: " << getMaximum() << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(hasSum()){
        buffer << "Sum: " << getSum() << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class StringColumnStatisticsImpl: public StringColumnStatistics {
  private:
    InternalStringStatistics _stats;

  public:
    StringColumnStatisticsImpl(const proto::ColumnStatistics& stats, const StatContext& statContext);
    virtual ~StringColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _stats.hasMinimum();
    }

    bool hasMaximum() const override {
      return _stats.hasMaximum();
    }

    bool hasTotalLength() const override {
      return _stats.hasTotalLength();
    }

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    std::string getMinimum() const override {
      if(hasMinimum()){
        return _stats.getMinimum();
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    std::string getMaximum() const override {
      if(hasMaximum()){
        return _stats.getMaximum();
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    uint64_t getTotalLength() const override {
      if(hasTotalLength()){
        return _stats.getTotalLength();
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: String" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if(hasMinimum()){
        buffer << "Minimum: " << getMinimum() << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(hasMaximum()){
        buffer << "Maximum: " << getMaximum() << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }

      if(hasTotalLength()){
        buffer << "Total length: " << getTotalLength() << std::endl;
      }else{
        buffer << "Total length is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class TimestampColumnStatisticsImpl: public TimestampColumnStatistics {
  private:
    InternalIntegerStatistics _stats;
    bool _hasLowerBound;
    bool _hasUpperBound;
    int64_t _lowerBound;
    int64_t _upperBound;

  public:
    TimestampColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                  const StatContext& statContext);
    virtual ~TimestampColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _stats.hasMinimum();
    }

    bool hasMaximum() const override {
      return _stats.hasMaximum();
    }

    uint64_t getNumberOfValues() const override {
      return _stats.getNumberOfValues();
    }

    bool hasNull() const override {
      return _stats.hasNull();
    }

    int64_t getMinimum() const override {
      if(hasMinimum()){
        return _stats.getMinimum();
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(hasMaximum()){
        return _stats.getMaximum();
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      struct tm tmValue;
      char timeBuffer[20];
      time_t secs = 0;

      buffer << "Data type: Timestamp" << std::endl
             << "Values: " << getNumberOfValues() << std::endl
             << "Has null: " << (hasNull() ? "yes" : "no") << std::endl;
      if(hasMinimum()){
        secs = static_cast<time_t>(getMinimum() / 1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "Minimum: " << timeBuffer << "." << (getMinimum() % 1000) << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(hasLowerBound()){
        secs = static_cast<time_t>(getLowerBound() / 1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "LowerBound: " << timeBuffer << "." << (getLowerBound() % 1000) << std::endl;
      }else{
        buffer << "LowerBound is not defined" << std::endl;
      }

      if(hasMaximum()){
        secs = static_cast<time_t>(getMaximum()/1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "Maximum: " << timeBuffer << "." << (getMaximum() % 1000) << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }

      if(hasUpperBound()){
        secs = static_cast<time_t>(getUpperBound() / 1000);
        gmtime_r(&secs, &tmValue);
        strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
        buffer << "UpperBound: " << timeBuffer << "." << (getUpperBound() % 1000) << std::endl;
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
      if(hasLowerBound()){
        return _lowerBound;
      }else{
        throw ParseError("LowerBound is not defined.");
      }
    }

    int64_t getUpperBound() const override {
      if(hasUpperBound()){
        return _upperBound;
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
    StatisticsImpl(const proto::StripeStatistics& stripeStats,
                   const StatContext& statContext);

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

  class StripeStatisticsImpl: public StripeStatistics {
  private:
    std::unique_ptr<StatisticsImpl> columnStats;
    std::vector<std::vector<std::shared_ptr<const ColumnStatistics> > > rowIndexStats;

    // DELIBERATELY NOT IMPLEMENTED
    StripeStatisticsImpl(const StripeStatisticsImpl&);
    StripeStatisticsImpl& operator=(const StripeStatisticsImpl&);

  public:
    StripeStatisticsImpl(const proto::StripeStatistics& stripeStats,
                   std::vector<std::vector<proto::ColumnStatistics> >& indexStats,
                   const StatContext& statContext);

    virtual const ColumnStatistics* getColumnStatistics(uint32_t columnId
                                                        ) const override {
      return columnStats->getColumnStatistics(columnId);
    }

    uint32_t getNumberOfColumns() const override {
      return columnStats->getNumberOfColumns();
    }

    virtual const ColumnStatistics* getRowIndexStatistics(uint32_t columnId, uint32_t rowIndex
                                                        ) const override {
      // check id indices are valid
      return rowIndexStats[columnId][rowIndex].get();
    }

    virtual ~StripeStatisticsImpl();

    uint32_t getNumberOfRowIndexStats(uint32_t columnId) const override {
      return static_cast<uint32_t>(rowIndexStats[columnId].size());
    }
  };


}// namespace

#endif
