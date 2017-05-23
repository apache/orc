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
    virtual uint64_t getNumberOfValues() const = 0;

    /**
     * Check whether column has null value
     * @return true if has null value
     */
    virtual bool hasNull() const = 0;

    /**
     * print out statistics of column if any
     */
    virtual std::string toString() const = 0;
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

    virtual uint64_t getTotalLength() const = 0;
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

    virtual uint64_t getFalseCount() const = 0;
    virtual uint64_t getTrueCount() const = 0;
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
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual int32_t getMaximum() const = 0;
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
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual Decimal getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual Decimal getMaximum() const = 0;

    /**
     * Get the sum for the column.
     * @return sum of all the values
     */
    virtual Decimal getSum() const = 0;
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
     * Get the smallest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the minimum
     */
    virtual double getMinimum() const = 0;

    /**
     * Get the largest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the maximum
     */
    virtual double getMaximum() const = 0;

    /**
     * Get the sum of the values in the column.
     * @return the sum
     */
    virtual double getSum() const = 0;
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
     * Get the smallest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the minimum
     */
    virtual int64_t getMinimum() const = 0;

    /**
     * Get the largest value in the column. Only defined if getNumberOfValues
     * is non-zero.
     * @return the maximum
     */
    virtual int64_t getMaximum() const = 0;

    /**
     * Get the sum of the column. Only valid if isSumDefined returns true.
     * @return the sum of the column
     */
    virtual int64_t getSum() const = 0;
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
     * Get the minimum value for the column.
     * @return minimum value
     */
    virtual std::string getMinimum() const = 0;

    /**
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual std::string getMaximum() const = 0;

    /**
     * Get the total length of all values.
     * @return total length of all the values
     */
    virtual uint64_t getTotalLength() const = 0;
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
     * Get the maximum value for the column.
     * @return maximum value
     */
    virtual int64_t getMaximum() const = 0;

    /**
     * check whether column has a lowerBound
     * @return true if column has a lowerBound
     */
    virtual bool hasLowerBound() const = 0;

    /**
     * check whether column has an upperBound
     * @return true if column has an upperBound
     */
    virtual bool hasUpperBound() const = 0;

    /**
     * Get the lowerBound value for the column.
     * @return lowerBound value
     */
    virtual int64_t getLowerBound() const = 0;

    /**
     * Get the upperBound value for the column.
     * @return upperBound value
     */
    virtual int64_t getUpperBound() const = 0;


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

  class StripeStatistics : public Statistics {
  public:
    virtual ~StripeStatistics();

    /**
     * Get the RowIndex statistics of a column id.
     * @return one stripe RowIndex statistics
     */
    virtual const ColumnStatistics*
                      getRowIndexStatistics(
                          uint32_t columnId, uint32_t IndexId) const = 0;

    /**
     * Get the number of RowIndexes
     * @return the number of RowIndex Statistics
     */
    virtual uint32_t getNumberOfRowIndexStats(uint32_t columnId) const = 0;
  };
}

#endif
