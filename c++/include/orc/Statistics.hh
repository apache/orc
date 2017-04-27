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

#ifndef ORC_STATISTICS_HH
#define ORC_STATISTICS_HH

#include "orc/orc-config.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"

namespace orc {

  /**
   * Statistics that are available for all types of columns.
   */
  class ColumnStatistics {
  public:
    virtual ~ColumnStatistics();

    /**
     * Get the number of values in this column. It will differ from the number
     * of rows because of NULL values and repeated values.
     * @return the number of values
     */
    virtual uint64_t getNumberOfValues() const {
      return valueCount;
    }

    /**
     * Set the number of values in this column
     * @param newValueCount new number of values to be set
     */
    virtual void setNumberOfValues(uint64_t newValueCount) {
      valueCount = newValueCount;
    }

    /**
     * Check whether column has null value
     * @return true if has null value
     */
    virtual bool hasNull() const {
      return hasNullValue;
    }

    /**
     * Set whether column has null value
     * @param newHasNull has null value
     */
    virtual void setHasNull(bool newHasNull) {
      hasNullValue = newHasNull;
    }

    /**
     * print out statistics of column if any
     */
    virtual std::string toString() const = 0;

    /**
     * Increases count of values
     * @param count number of values to be increased
     */
    virtual void increase(uint64_t count) {
      valueCount += count;
    }

    /**
     * reset column statistics to initial state
     */
    virtual void reset() {
      hasNullValue = false;
      valueCount = 0;
    }

    /**
     * Merges another statistics
     * @param other statistics to be merged
     */
    virtual void merge(const ColumnStatistics& other) {
      hasNullValue |= other.hasNull();
      valueCount += other.getNumberOfValues();
    }

    /**
     * Convert statistics to protobuf version
     * @param pbStats output of protobuf stats
     */
    virtual void toProtoBuf(proto::ColumnStatistics& pbStats) const {
      pbStats.set_hasnull(hasNullValue);
      pbStats.set_numberofvalues(valueCount);
    }

  protected:
    uint64_t valueCount;
    bool hasNullValue;
  };

  /**
   * Statistics for binary columns.
   */
  class BinaryColumnStatistics: public ColumnStatistics {
  public:
    virtual ~BinaryColumnStatistics();

    /**
     * check whether column has total length
     * @return true if has total length
     */
    virtual bool hasTotalLength() const = 0;

    /**
     * set has total length
     * @param newHasTotalLength has total length
     */
    virtual void setHasTotalLength(bool newHasTotalLength) = 0;

    /**
     * get total length
     * @return total length
     */
    virtual uint64_t getTotalLength() const = 0;

    /**
     * set total length
     * @param newTotalLength new total length value
     */
    virtual void setTotalLength(uint64_t newTotalLength) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     * @param length length of the value
     */
    virtual void update(const char* value, size_t length) = 0;
  };

  /**
   * Statistics for boolean columns.
   */
  class BooleanColumnStatistics: public ColumnStatistics {
  public:
    virtual ~BooleanColumnStatistics();

    /**
     * check whether column has true/false count
     * @return true if has true/false count
     */
    virtual bool hasCount() const = 0;

    /**
     * set hasCount value
     * @param hasCount new hasCount value
     */
    virtual void setHasCount(bool hasCount) = 0;

    virtual uint64_t getFalseCount() const = 0;
    virtual uint64_t getTrueCount() const = 0;

    virtual void setTrueCount(uint64_t trueCount) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     * @param repetitions the repetitions of the boolean value
     */
    virtual void update(bool value, size_t repetitions) = 0;
  };

  /**
   * Statistics for date columns.
   */
  class DateColumnStatistics: public ColumnStatistics {
  public:
    virtual ~DateColumnStatistics();

    /**
     * check whether column has minimum
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * check whether column has maximum
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual int32_t getMinimum() const = 0;

    /**
     * set new minimum value
     * @param min new minimum value
     */
    virtual void setMinimum(int32_t min) = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual int32_t getMaximum() const = 0;

    /**
     * set new maximum value
     * @param max new maximum value
     */
    virtual void setMaximum(int32_t max) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     */
    virtual void update(int32_t value) = 0;
  };

  /**
   * Statistics for decimal columns.
   */
  class DecimalColumnStatistics: public ColumnStatistics {
  public:
    virtual ~DecimalColumnStatistics();

    /**
     * check whether column has minimum
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * check whether column has maximum
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * check whether column has sum
     * @return true if has sum
     */
    virtual bool hasSum() const = 0;

    /**
     * set hasSum value
     * @param newHasSum hasSum value
     */
    virtual void setHasSum(bool newHasSum) = 0;

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual Decimal getMinimum() const = 0;

    /**
     * set new minimum value
     * @param min new minimum value
     */
    virtual void setMinimum(Decimal min) = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual Decimal getMaximum() const = 0;

    /**
     * set new maximum value
     * @param max new maximum value
     */
    virtual void setMaximum(Decimal max) = 0;

    /**
     * Get the sum for the column.
     * @return sum of all the values
     */
    virtual Decimal getSum() const = 0;

    /**
     * set new sum
     * @param newSum sum to be set
     */
    virtual void setSum(Decimal newSum) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     */
    virtual void update(const Decimal& value) = 0;

    /**
     * update stats by a new value
     * @param value new decimal value represented in Int128
     * @param scale scale of the decimal
     */
    virtual void update(Int128 value, int32_t scale) = 0;

    /**
     * update stats by a new value
     * @param value new decimal value represented in int64_t
     * @param scale scale of the decimal
     */
    virtual void update(int64_t value, int32_t scale) = 0;
  };

  /**
   * Statistics for float and double columns.
   */
  class DoubleColumnStatistics: public ColumnStatistics {
  public:
    virtual ~DoubleColumnStatistics();

    /**
     * check whether column has minimum
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * check whether column has maximum
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * check whether column has sum
     * @return true if has sum
     */
    virtual bool hasSum() const = 0;

    /**
     * set hasSum value
     * @param newHasSum hasSum value
     */
    virtual void setHasSum(bool newHasSum) = 0;

    /**
     * Get the smallest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the minimum
     */
    virtual double getMinimum() const = 0;

    /**
     * set new minimum value
     * @param min new minimum value
     */
    virtual void setMinimum(double min) = 0;

    /**
     * Get the largest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the maximum
     */
    virtual double getMaximum() const = 0;

    /**
     * set new maximum value
     * @param max new maximum value
     */
    virtual void setMaximum(double max) = 0;

    /**
     * Get the sum of the values in the column.
     * @return the sum
     */
    virtual double getSum() const = 0;

    /**
     * set new sum
     * @param newSum sum to be set
     */
    virtual void setSum(double newSum) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     */
    virtual void update(double value) = 0;
  };

  /**
   * Statistics for all of the integer columns, such as byte, short, int, and
   * long.
   */
  class IntegerColumnStatistics: public ColumnStatistics {
  public:
    virtual ~IntegerColumnStatistics();

    /**
     * check whether column has minimum
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * check whether column has maximum
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * check whether column has sum
     * @return true if has sum
     */
    virtual bool hasSum() const = 0;

    /**
     * set hasSum value
     * @param newHasSum hasSum value
     */
    virtual void setHasSum(bool newHasSum) = 0;

    /**
     * Get the smallest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the minimum
     */
    virtual int64_t getMinimum() const = 0;

    /**
     * set new minimum value
     * @param min new minimum value
     */
    virtual void setMinimum(int64_t min) = 0;

    /**
     * Get the largest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the maximum
     */
    virtual int64_t getMaximum() const = 0;

    /**
     * set new maximum value
     * @param max new maximum value
     */
    virtual void setMaximum(int64_t max) = 0;

    /**
     * Get the sum of the column. Only valid if isSumDefined returns true.
     * @return the sum of the column
     */
    virtual int64_t getSum() const = 0;

    /**
     * set new sum
     * @param newSum sum to be set
     */
    virtual void setSum(int64_t newSum) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     * @param repetitions repetition of the value
     */
    virtual void update(int64_t value, int repetitions) = 0;
  };

  /**
   * Statistics for string columns.
   */
  class StringColumnStatistics: public ColumnStatistics {
  public:
    virtual ~StringColumnStatistics();

    /**
     * check whether column has minimum
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * check whether column has maximum
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * check whether column
     * @return true if has maximum
     */
    virtual bool hasTotalLength() const = 0;

    /**
     * set has total length
     * @param newHasTotalLength has total length
     */
    virtual void setHasTotalLength(bool newHasTotalLength) = 0;

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual std::string getMinimum() const = 0;

    /**
     * set new minimum value
     * @param min new minimum value
     */
    virtual void setMinimum(std::string min) = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual std::string getMaximum() const = 0;

    /**
     * set new maximum value
     * @param max new maximum value
     */
    virtual void setMaximum(std::string max) = 0;

    /**
     * Get the total length of all values.
     * @return total length of all the values
     */
    virtual uint64_t getTotalLength() const = 0;

    /**
     * set total length
     * @param newTotalLength new total length value
     */
    virtual void setTotalLength(uint64_t newTotalLength) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     */
    virtual void update(const std::string& value) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     * @param length length of the value
     */
    virtual void update(const char* value, size_t length) = 0;
  };

  /**
   * Statistics for timestamp columns.
   */
  class TimestampColumnStatistics: public ColumnStatistics {
  public:
    virtual ~TimestampColumnStatistics();

    /**
     * check whether column minimum
     * @return true if has minimum
     */
    virtual bool hasMinimum() const = 0;

    /**
     * check whether column maximum
     * @return true if has maximum
     */
    virtual bool hasMaximum() const = 0;

    /**
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual int64_t getMinimum() const = 0;

    /**
     * set new minimum value
     * @param min new minimum value
     */
    virtual void setMinimum(int64_t min) = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual int64_t getMaximum() const = 0;

    /**
     * set new maximum value
     * @param max new maximum value
     */
    virtual void setMaximum(int64_t max) = 0;

    /**
     * update stats by a new value
     * @param value new value to update
     */
    virtual void update(int64_t value) = 0;
  };

  class Statistics {
  public:
    virtual ~Statistics();

    /**
     * Get the statistics of colId column.
     * @return one column's statistics
     */
    virtual const ColumnStatistics* getColumnStatistics(uint32_t colId
                                                        ) const = 0;

    /**
     * Get the number of columns
     * @return the number of columns
     */
    virtual uint32_t getNumberOfColumns() const = 0;
  };

  std::unique_ptr<ColumnStatistics> createColumnStatistics(
    const Type& type, bool enableStringComparison);

}

#endif
