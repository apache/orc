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

#include "PredicateLeaf.hh"
#include "orc/BloomFilter.hh"
#include "orc/Common.hh"
#include "orc/Type.hh"

#include <algorithm>
#include <functional>
#include <sstream>
#include <type_traits>

namespace orc {

  PredicateLeaf::PredicateLeaf(Operator op, PredicateDataType type, const std::string& colName,
                               Literal literal)
      : mOperator_(op), mType_(type), mColumnName_(colName), mHasColumnName_(true), mColumnId_(0) {
    mLiterals_.emplace_back(literal);
    mHashCode_ = hashCode();
    validate();
  }

  PredicateLeaf::PredicateLeaf(Operator op, PredicateDataType type, uint64_t columnId,
                               Literal literal)
      : mOperator_(op), mType_(type), mHasColumnName_(false), mColumnId_(columnId) {
    mLiterals_.emplace_back(literal);
    mHashCode_ = hashCode();
    validate();
  }

  PredicateLeaf::PredicateLeaf(Operator op, PredicateDataType type, const std::string& colName,
                               const std::initializer_list<Literal>& literals)
      : mOperator_(op),
        mType_(type),
        mColumnName_(colName),
        mHasColumnName_(true),
        mLiterals_(literals.begin(), literals.end()) {
    mHashCode_ = hashCode();
    validate();
  }

  PredicateLeaf::PredicateLeaf(Operator op, PredicateDataType type, uint64_t columnId,
                               const std::initializer_list<Literal>& literals)
      : mOperator_(op),
        mType_(type),
        mHasColumnName_(false),
        mColumnId_(columnId),
        mLiterals_(literals.begin(), literals.end()) {
    mHashCode_ = hashCode();
    validate();
  }

  PredicateLeaf::PredicateLeaf(Operator op, PredicateDataType type, const std::string& colName,
                               const std::vector<Literal>& literals)
      : mOperator_(op),
        mType_(type),
        mColumnName_(colName),
        mHasColumnName_(true),
        mLiterals_(literals.begin(), literals.end()) {
    mHashCode_ = hashCode();
    validate();
  }

  PredicateLeaf::PredicateLeaf(Operator op, PredicateDataType type, uint64_t columnId,
                               const std::vector<Literal>& literals)
      : mOperator_(op),
        mType_(type),
        mHasColumnName_(false),
        mColumnId_(columnId),
        mLiterals_(literals.begin(), literals.end()) {
    mHashCode_ = hashCode();
    validate();
  }

  void PredicateLeaf::validateColumn() const {
    if (mHasColumnName_ && mColumnName_.empty()) {
      throw std::invalid_argument("column name should not be empty");
    } else if (!mHasColumnName_ && mColumnId_ == INVALID_COLUMN_ID) {
      throw std::invalid_argument("invalid column id");
    }
  }

  void PredicateLeaf::validate() const {
    switch (mOperator_) {
      case Operator::IS_NULL:
        validateColumn();
        if (!mLiterals_.empty()) {
          throw std::invalid_argument("No literal is required!");
        }
        break;
      case Operator::EQUALS:
      case Operator::NULL_SAFE_EQUALS:
      case Operator::LESS_THAN:
      case Operator::LESS_THAN_EQUALS:
        validateColumn();
        if (mLiterals_.size() != 1) {
          throw std::invalid_argument("One literal is required!");
        }
        if (static_cast<int>(mLiterals_.at(0).getType()) != static_cast<int>(mType_)) {
          throw std::invalid_argument("leaf and literal types do not match!");
        }
        break;
      case Operator::IN:
        validateColumn();
        if (mLiterals_.size() < 2) {
          throw std::invalid_argument("At least two literals are required!");
        }
        for (auto literal : mLiterals_) {
          if (static_cast<int>(literal.getType()) != static_cast<int>(mType_)) {
            throw std::invalid_argument("leaf and literal types do not match!");
          }
        }
        break;
      case Operator::BETWEEN:
        validateColumn();
        for (auto literal : mLiterals_) {
          if (static_cast<int>(literal.getType()) != static_cast<int>(mType_)) {
            throw std::invalid_argument("leaf and literal types do not match!");
          }
        }
        break;
      default:
        break;
    }
  }

  PredicateLeaf::Operator PredicateLeaf::getOperator() const {
    return mOperator_;
  }

  PredicateDataType PredicateLeaf::getType() const {
    return mType_;
  }

  bool PredicateLeaf::hasColumnName() const {
    return mHasColumnName_;
  }

  /**
   * Get the simple column name.
   */
  const std::string& PredicateLeaf::getColumnName() const {
    return mColumnName_;
  }

  uint64_t PredicateLeaf::getColumnId() const {
    return mColumnId_;
  }

  /**
   * Get the literal half of the predicate leaf.
   */
  Literal PredicateLeaf::getLiteral() const {
    return mLiterals_.at(0);
  }

  /**
   * For operators with multiple literals (IN and BETWEEN), get the literals.
   */
  const std::vector<Literal>& PredicateLeaf::getLiteralList() const {
    return mLiterals_;
  }

  static std::string getLiteralString(const std::vector<Literal>& literals) {
    return literals.at(0).toString();
  }

  static std::string getLiteralsString(const std::vector<Literal>& literals) {
    std::ostringstream sstream;
    sstream << "[";
    for (size_t i = 0; i != literals.size(); ++i) {
      sstream << literals[i].toString();
      if (i + 1 != literals.size()) {
        sstream << ", ";
      }
    }
    sstream << "]";
    return sstream.str();
  }

  std::string PredicateLeaf::columnDebugString() const {
    if (mHasColumnName_) return mColumnName_;
    std::ostringstream sstream;
    sstream << "column(id=" << mColumnId_ << ')';
    return sstream.str();
  }

  std::string PredicateLeaf::toString() const {
    std::ostringstream sstream;
    sstream << '(';
    switch (mOperator_) {
      case Operator::IS_NULL:
        sstream << columnDebugString() << " is null";
        break;
      case Operator::EQUALS:
        sstream << columnDebugString() << " = " << getLiteralString(mLiterals_);
        break;
      case Operator::NULL_SAFE_EQUALS:
        sstream << columnDebugString() << " null_safe_= " << getLiteralString(mLiterals_);
        break;
      case Operator::LESS_THAN:
        sstream << columnDebugString() << " < " << getLiteralString(mLiterals_);
        break;
      case Operator::LESS_THAN_EQUALS:
        sstream << columnDebugString() << " <= " << getLiteralString(mLiterals_);
        break;
      case Operator::IN:
        sstream << columnDebugString() << " in " << getLiteralsString(mLiterals_);
        break;
      case Operator::BETWEEN:
        sstream << columnDebugString() << " between " << getLiteralsString(mLiterals_);
        break;
      default:
        sstream << "unknown operator, column: " << columnDebugString()
                << ", literals: " << getLiteralsString(mLiterals_);
    }
    sstream << ')';
    return sstream.str();
  }

  size_t PredicateLeaf::hashCode() const {
    size_t value = 0;
    std::for_each(mLiterals_.cbegin(), mLiterals_.cend(),
                  [&](const Literal& literal) { value = value * 17 + literal.getHashCode(); });
    auto colHash = mHasColumnName_ ? std::hash<std::string>{}(mColumnName_)
                                   : std::hash<uint64_t>{}(mColumnId_);
    return value * 103 * 101 * 3 * 17 + std::hash<int>{}(static_cast<int>(mOperator_)) +
           std::hash<int>{}(static_cast<int>(mType_)) * 17 + colHash * 3 * 17;
  }

  bool PredicateLeaf::operator==(const PredicateLeaf& r) const {
    if (this == &r) {
      return true;
    }
    if (mHashCode_ != r.mHashCode_ || mType_ != r.mType_ || mOperator_ != r.mOperator_ ||
        mHasColumnName_ != r.mHasColumnName_ || mColumnName_ != r.mColumnName_ ||
        mColumnId_ != r.mColumnId_ || mLiterals_.size() != r.mLiterals_.size()) {
      return false;
    }
    for (size_t i = 0; i != mLiterals_.size(); ++i) {
      if (mLiterals_[i] != r.mLiterals_[i]) {
        return false;
      }
    }
    return true;
  }

  // enum to mark the position of predicate in the range
  enum class Location { BEFORE, MIN, MIDDLE, MAX, AFTER };

  DIAGNOSTIC_PUSH
  DIAGNOSTIC_IGNORE("-Wfloat-equal")

  /**
   * Given a point and min and max, determine if the point is before, at the
   * min, in the middle, at the max, or after the range.
   * @param point the point to test
   * @param min the minimum point
   * @param max the maximum point
   * @return the location of the point
   */
  template <typename T>
  Location compareToRange(const T& point, const T& min, const T& max) {
    if (point < min) {
      return Location::BEFORE;
    } else if (point == min) {
      return Location::MIN;
    }

    if (point > max) {
      return Location::AFTER;
    } else if (point == max) {
      return Location::MAX;
    }

    return Location::MIDDLE;
  }

  /**
   * Evaluate a predicate leaf according to min/max values
   * @param op operator of the predicate
   * @param values the value to test
   * @param minValue the minimum value
   * @param maxValue the maximum value
   * @param hasNull whether the statistics contain null
   * @return the TruthValue result of the test
   */
  template <typename T>
  TruthValue evaluatePredicateRange(const PredicateLeaf::Operator op, const std::vector<T>& values,
                                    const T& minValue, const T& maxValue, bool hasNull) {
    Location loc;
    switch (op) {
      case PredicateLeaf::Operator::NULL_SAFE_EQUALS:
        loc = compareToRange(values.at(0), minValue, maxValue);
        if (loc == Location::BEFORE || loc == Location::AFTER) {
          return TruthValue::NO;
        } else {
          return TruthValue::YES_NO;
        }
      case PredicateLeaf::Operator::EQUALS:
        loc = compareToRange(values.at(0), minValue, maxValue);
        if (minValue == maxValue && loc == Location::MIN) {
          return hasNull ? TruthValue::YES_NULL : TruthValue::YES;
        } else if (loc == Location::BEFORE || loc == Location::AFTER) {
          return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
        } else {
          return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
      case PredicateLeaf::Operator::LESS_THAN:
        loc = compareToRange(values.at(0), minValue, maxValue);
        if (loc == Location::AFTER) {
          return hasNull ? TruthValue::YES_NULL : TruthValue::YES;
        } else if (loc == Location::BEFORE || loc == Location::MIN) {
          return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
        } else {
          return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
      case PredicateLeaf::Operator::LESS_THAN_EQUALS:
        loc = compareToRange(values.at(0), minValue, maxValue);
        if (loc == Location::AFTER || loc == Location::MAX ||
            (loc == Location::MIN && minValue == maxValue)) {
          return hasNull ? TruthValue::YES_NULL : TruthValue::YES;
        } else if (loc == Location::BEFORE) {
          return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
        } else {
          return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
      case PredicateLeaf::Operator::IN:
        if (minValue == maxValue) {
          // for a single value, look through to see if that value is in the set
          for (auto& value : values) {
            loc = compareToRange(value, minValue, maxValue);
            if (loc == Location::MIN) {
              return hasNull ? TruthValue::YES_NULL : TruthValue::YES;
            }
          }
          return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
        } else {
          // are all of the values outside of the range?
          for (auto& value : values) {
            loc = compareToRange(value, minValue, maxValue);
            if (loc == Location::MIN || loc == Location::MIDDLE || loc == Location::MAX) {
              return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
            }
          }
          return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
        }
      case PredicateLeaf::Operator::BETWEEN:
        if (values.empty()) {
          return TruthValue::YES_NO;
        }
        loc = compareToRange(values.at(0), minValue, maxValue);
        if (loc == Location::BEFORE || loc == Location::MIN) {
          Location loc2 = compareToRange(values.at(1), minValue, maxValue);
          if (loc2 == Location::AFTER || loc2 == Location::MAX) {
            return hasNull ? TruthValue::YES_NULL : TruthValue::YES;
          } else if (loc2 == Location::BEFORE) {
            return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
          } else {
            return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
          }
        } else if (loc == Location::AFTER) {
          return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
        } else {
          return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
        }
      case PredicateLeaf::Operator::IS_NULL:
        // min = null condition above handles the all-nulls YES case
        return hasNull ? TruthValue::YES_NO : TruthValue::NO;
      default:
        return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
    }
  }

  DIAGNOSTIC_POP

  static TruthValue evaluateBoolPredicate(const PredicateLeaf::Operator op,
                                          const std::vector<Literal>& literals,
                                          const proto::ColumnStatistics& stats) {
    bool hasNull = stats.has_null();
    if (!stats.has_bucket_statistics() || stats.bucket_statistics().count_size() == 0) {
      // does not have bool stats
      return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
    }

    auto trueCount = stats.bucket_statistics().count(0);
    auto falseCount = stats.number_of_values() - trueCount;
    switch (op) {
      case PredicateLeaf::Operator::IS_NULL:
        return hasNull ? TruthValue::YES_NO : TruthValue::NO;
      case PredicateLeaf::Operator::NULL_SAFE_EQUALS: {
        if (literals.at(0).getBool()) {
          if (trueCount == 0) {
            return TruthValue::NO;
          } else if (falseCount == 0) {
            return TruthValue::YES;
          }
        } else {
          if (falseCount == 0) {
            return TruthValue::NO;
          } else if (trueCount == 0) {
            return TruthValue::YES;
          }
        }
        return TruthValue::YES_NO;
      }
      case PredicateLeaf::Operator::EQUALS: {
        if (literals.at(0).getBool()) {
          if (trueCount == 0) {
            return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
          } else if (falseCount == 0) {
            return hasNull ? TruthValue::YES_NULL : TruthValue::YES;
          }
        } else {
          if (falseCount == 0) {
            return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
          } else if (trueCount == 0) {
            return hasNull ? TruthValue::YES_NULL : TruthValue::YES;
          }
        }
        return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
      }
      case PredicateLeaf::Operator::LESS_THAN:
      case PredicateLeaf::Operator::LESS_THAN_EQUALS:
      case PredicateLeaf::Operator::IN:
      case PredicateLeaf::Operator::BETWEEN:
      default:
        return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
    }
  }

  static std::vector<int64_t> literal2Long(const std::vector<Literal>& values) {
    std::vector<int64_t> result;
    std::for_each(values.cbegin(), values.cend(), [&](const Literal& val) {
      if (!val.isNull()) {
        result.emplace_back(val.getLong());
      }
    });
    return result;
  }

  static std::vector<int32_t> literal2Date(const std::vector<Literal>& values) {
    std::vector<int32_t> result;
    std::for_each(values.cbegin(), values.cend(), [&](const Literal& val) {
      if (!val.isNull()) {
        result.emplace_back(val.getDate());
      }
    });
    return result;
  }

  static std::vector<Literal::Timestamp> literal2Timestamp(const std::vector<Literal>& values) {
    std::vector<Literal::Timestamp> result;
    std::for_each(values.cbegin(), values.cend(), [&](const Literal& val) {
      if (!val.isNull()) {
        result.emplace_back(val.getTimestamp());
      }
    });
    return result;
  }

  static std::vector<Decimal> literal2Decimal(const std::vector<Literal>& values) {
    std::vector<Decimal> result;
    std::for_each(values.cbegin(), values.cend(), [&](const Literal& val) {
      if (!val.isNull()) {
        result.emplace_back(val.getDecimal());
      }
    });
    return result;
  }

  static std::vector<double> literal2Double(const std::vector<Literal>& values) {
    std::vector<double> result;
    std::for_each(values.cbegin(), values.cend(), [&](const Literal& val) {
      if (!val.isNull()) {
        result.emplace_back(val.getFloat());
      }
    });
    return result;
  }

  static std::vector<std::string> literal2String(const std::vector<Literal>& values) {
    std::vector<std::string> result;
    std::for_each(values.cbegin(), values.cend(), [&](const Literal& val) {
      if (!val.isNull()) {
        result.emplace_back(val.getString());
      }
    });
    return result;
  }

  TruthValue PredicateLeaf::evaluatePredicateMinMax(const proto::ColumnStatistics& colStats) const {
    TruthValue result = TruthValue::YES_NO_NULL;
    switch (mType_) {
      case PredicateDataType::LONG: {
        if (colStats.has_int_statistics() && colStats.int_statistics().has_minimum() &&
            colStats.int_statistics().has_maximum()) {
          const auto& stats = colStats.int_statistics();
          result = evaluatePredicateRange(mOperator_, literal2Long(mLiterals_), stats.minimum(),
                                          stats.maximum(), colStats.has_null());
        }
        break;
      }
      case PredicateDataType::FLOAT: {
        if (colStats.has_double_statistics() && colStats.double_statistics().has_minimum() &&
            colStats.double_statistics().has_maximum()) {
          const auto& stats = colStats.double_statistics();
          if (!std::isfinite(stats.sum())) {
            result = colStats.has_null() ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
          } else {
            result = evaluatePredicateRange(mOperator_, literal2Double(mLiterals_), stats.minimum(),
                                            stats.maximum(), colStats.has_null());
          }
        }
        break;
      }
      case PredicateDataType::STRING: {
        /// TODO: check lowerBound and upperBound as well
        if (colStats.has_string_statistics() && colStats.string_statistics().has_minimum() &&
            colStats.string_statistics().has_maximum()) {
          const auto& stats = colStats.string_statistics();
          result = evaluatePredicateRange(mOperator_, literal2String(mLiterals_), stats.minimum(),
                                          stats.maximum(), colStats.has_null());
        }
        break;
      }
      case PredicateDataType::DATE: {
        if (colStats.has_date_statistics() && colStats.date_statistics().has_minimum() &&
            colStats.date_statistics().has_maximum()) {
          const auto& stats = colStats.date_statistics();
          result = evaluatePredicateRange(mOperator_, literal2Date(mLiterals_), stats.minimum(),
                                          stats.maximum(), colStats.has_null());
        }
        break;
      }
      case PredicateDataType::TIMESTAMP: {
        if (colStats.has_timestamp_statistics() &&
            colStats.timestamp_statistics().has_minimum_utc() &&
            colStats.timestamp_statistics().has_maximum_utc()) {
          const auto& stats = colStats.timestamp_statistics();
          constexpr int32_t DEFAULT_MIN_NANOS = 0;
          constexpr int32_t DEFAULT_MAX_NANOS = 999999;
          int32_t minNano =
              stats.has_minimum_nanos() ? stats.minimum_nanos() - 1 : DEFAULT_MIN_NANOS;
          int32_t maxNano =
              stats.has_maximum_nanos() ? stats.maximum_nanos() - 1 : DEFAULT_MAX_NANOS;
          Literal::Timestamp minTimestamp(
              stats.minimum_utc() / 1000,
              static_cast<int32_t>((stats.minimum_utc() % 1000) * 1000000) + minNano);
          Literal::Timestamp maxTimestamp(
              stats.maximum_utc() / 1000,
              static_cast<int32_t>((stats.maximum_utc() % 1000) * 1000000) + maxNano);
          result = evaluatePredicateRange(mOperator_, literal2Timestamp(mLiterals_), minTimestamp,
                                          maxTimestamp, colStats.has_null());
        }
        break;
      }
      case PredicateDataType::DECIMAL: {
        if (colStats.has_decimal_statistics() && colStats.decimal_statistics().has_minimum() &&
            colStats.decimal_statistics().has_maximum()) {
          const auto& stats = colStats.decimal_statistics();
          result = evaluatePredicateRange(mOperator_, literal2Decimal(mLiterals_),
                                          Decimal(stats.minimum()), Decimal(stats.maximum()),
                                          colStats.has_null());
        }
        break;
      }
      case PredicateDataType::BOOLEAN: {
        if (colStats.has_bucket_statistics()) {
          result = evaluateBoolPredicate(mOperator_, mLiterals_, colStats);
        }
        break;
      }
      default:
        break;
    }

    // make sure null literal is respected for IN operator
    if (mOperator_ == Operator::IN && colStats.has_null()) {
      for (const auto& literal : mLiterals_) {
        if (literal.isNull()) {
          result = TruthValue::YES_NO_NULL;
          break;
        }
      }
    }

    return result;
  }

  static bool shouldEvaluateBloomFilter(PredicateLeaf::Operator op, TruthValue result,
                                        const BloomFilter* bloomFilter) {
    // evaluate bloom filter only when
    // 1) Bloom filter is available
    // 2) Min/Max evaluation yield YES or MAYBE
    // 3) Predicate is EQUALS or IN list
    // 4) Decimal type stores its string representation
    //    but has inconsistency in trailing zeros
    if (bloomFilter != nullptr && result != TruthValue::NO_NULL && result != TruthValue::NO &&
        (op == PredicateLeaf::Operator::EQUALS || op == PredicateLeaf::Operator::NULL_SAFE_EQUALS ||
         op == PredicateLeaf::Operator::IN)) {
      return true;
    }
    return false;
  }

  static TruthValue checkInBloomFilter(PredicateLeaf::Operator, PredicateDataType type,
                                       const Literal& literal, const BloomFilter* bf,
                                       bool hasNull) {
    TruthValue result = hasNull ? TruthValue::NO_NULL : TruthValue::NO;
    if (literal.isNull()) {
      result = hasNull ? TruthValue::YES_NO_NULL : TruthValue::NO;
    } else if (type == PredicateDataType::LONG) {
      if (bf->testLong(literal.getLong())) {
        result = TruthValue::YES_NO_NULL;
      }
    } else if (type == PredicateDataType::FLOAT) {
      if (bf->testDouble(literal.getFloat())) {
        result = TruthValue::YES_NO_NULL;
      }
    } else if (type == PredicateDataType::STRING) {
      std::string str = literal.getString();
      if (bf->testBytes(str.c_str(), static_cast<int64_t>(str.size()))) {
        result = TruthValue::YES_NO_NULL;
      }
    } else if (type == PredicateDataType::DECIMAL) {
      std::string decimal = literal.getDecimal().toString(true);
      if (bf->testBytes(decimal.c_str(), static_cast<int64_t>(decimal.size()))) {
        result = TruthValue::YES_NO_NULL;
      }
    } else if (type == PredicateDataType::TIMESTAMP) {
      if (bf->testLong(literal.getTimestamp().getMillis())) {
        result = TruthValue::YES_NO_NULL;
      }
    } else if (type == PredicateDataType::DATE) {
      if (bf->testLong(literal.getDate())) {
        result = TruthValue::YES_NO_NULL;
      }
    } else {
      result = TruthValue::YES_NO_NULL;
    }

    if (result == TruthValue::YES_NO_NULL && !hasNull) {
      result = TruthValue::YES_NO;
    }

    return result;
  }

  TruthValue PredicateLeaf::evaluatePredicateBloomFiter(const BloomFilter* bf, bool hasNull) const {
    switch (mOperator_) {
      case Operator::NULL_SAFE_EQUALS:
        // null safe equals does not return *_NULL variant.
        // So set hasNull to false
        return checkInBloomFilter(mOperator_, mType_, mLiterals_.front(), bf, false);
      case Operator::EQUALS:
        return checkInBloomFilter(mOperator_, mType_, mLiterals_.front(), bf, hasNull);
      case Operator::IN:
        for (const auto& literal : mLiterals_) {
          // if at least one value in IN list exist in bloom filter,
          // qualify the row group/stripe
          TruthValue result = checkInBloomFilter(mOperator_, mType_, literal, bf, hasNull);
          if (result == TruthValue::YES_NO_NULL || result == TruthValue::YES_NO) {
            return result;
          }
        }
        return hasNull ? TruthValue::NO_NULL : TruthValue::NO;
      case Operator::LESS_THAN:
      case Operator::LESS_THAN_EQUALS:
      case Operator::BETWEEN:
      case Operator::IS_NULL:
      default:
        return hasNull ? TruthValue::YES_NO_NULL : TruthValue::YES_NO;
    }
  }

  TruthValue PredicateLeaf::evaluate(const WriterVersion writerVersion,
                                     const proto::ColumnStatistics& colStats,
                                     const BloomFilter* bloomFilter) const {
    // files written before ORC-135 stores timestamp wrt to local timezone
    // causing issues with PPD. disable PPD for timestamp for all old files
    if (mType_ == PredicateDataType::TIMESTAMP) {
      if (writerVersion < WriterVersion::WriterVersion_ORC_135) {
        return TruthValue::YES_NO_NULL;
      }
    }

    bool allNull = colStats.has_null() && colStats.number_of_values() == 0;
    if (mOperator_ == Operator::IS_NULL ||
        ((mOperator_ == Operator::EQUALS || mOperator_ == Operator::NULL_SAFE_EQUALS) &&
         mLiterals_.at(0).isNull())) {
      // IS_NULL operator does not need to check min/max stats and bloom filter
      return allNull ? TruthValue::YES
                     : (colStats.has_null() ? TruthValue::YES_NO : TruthValue::NO);
    } else if (allNull) {
      // if we don't have any value, everything must have been null
      return TruthValue::IS_NULL;
    }

    TruthValue result = evaluatePredicateMinMax(colStats);
    if (shouldEvaluateBloomFilter(mOperator_, result, bloomFilter)) {
      return evaluatePredicateBloomFiter(bloomFilter, colStats.has_null());
    } else {
      return result;
    }
  }

}  // namespace orc
