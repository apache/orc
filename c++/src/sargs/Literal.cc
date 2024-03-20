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

#include "orc/sargs/Literal.hh"

#include <cmath>
#include <functional>
#include <limits>
#include <sstream>

namespace orc {

  Literal::Literal(PredicateDataType type) {
    mType_ = type;
    mValue_.DecimalVal = 0;
    mSize_ = 0;
    mIsNull_ = true;
    mPrecision_ = 0;
    mScale_ = 0;
    mHashCode_ = 0;
  }

  Literal::Literal(int64_t val) {
    mType_ = PredicateDataType::LONG;
    mValue_.IntVal = val;
    mSize_ = sizeof(val);
    mIsNull_ = false;
    mPrecision_ = 0;
    mScale_ = 0;
    mHashCode_ = hashCode();
  }

  Literal::Literal(double val) {
    mType_ = PredicateDataType::FLOAT;
    mValue_.DoubleVal = val;
    mSize_ = sizeof(val);
    mIsNull_ = false;
    mPrecision_ = 0;
    mScale_ = 0;
    mHashCode_ = hashCode();
  }

  Literal::Literal(bool val) {
    mType_ = PredicateDataType::BOOLEAN;
    mValue_.BooleanVal = val;
    mSize_ = sizeof(val);
    mIsNull_ = false;
    mPrecision_ = 0;
    mScale_ = 0;
    mHashCode_ = hashCode();
  }

  Literal::Literal(PredicateDataType type, int64_t val) {
    if (type != PredicateDataType::DATE) {
      throw std::invalid_argument("only DATE is supported here!");
    }
    mType_ = type;
    mValue_.IntVal = val;
    mSize_ = sizeof(val);
    mIsNull_ = false;
    mPrecision_ = 0;
    mScale_ = 0;
    mHashCode_ = hashCode();
  }

  Literal::Literal(const char* str, size_t size) {
    mType_ = PredicateDataType::STRING;
    mValue_.Buffer = new char[size];
    memcpy(mValue_.Buffer, str, size);
    mSize_ = size;
    mIsNull_ = false;
    mPrecision_ = 0;
    mScale_ = 0;
    mHashCode_ = hashCode();
  }

  Literal::Literal(Int128 val, int32_t precision, int32_t scale) {
    mType_ = PredicateDataType::DECIMAL;
    mValue_.DecimalVal = val;
    mPrecision_ = precision;
    mScale_ = scale;
    mSize_ = sizeof(Int128);
    mIsNull_ = false;
    mHashCode_ = hashCode();
  }

  Literal::Literal(int64_t second, int32_t nanos) {
    mType_ = PredicateDataType::TIMESTAMP;
    mValue_.TimeStampVal.second = second;
    mValue_.TimeStampVal.nanos = nanos;
    mPrecision_ = 0;
    mScale_ = 0;
    mSize_ = sizeof(Timestamp);
    mIsNull_ = false;
    mHashCode_ = hashCode();
  }

  Literal::Literal(const Literal& r)
      : mType_(r.mType_), mSize_(r.mSize_), mIsNull_(r.mIsNull_), mHashCode_(r.mHashCode_) {
    if (mType_ == PredicateDataType::STRING) {
      mValue_.Buffer = new char[r.mSize_];
      memcpy(mValue_.Buffer, r.mValue_.Buffer, r.mSize_);
      mPrecision_ = 0;
      mScale_ = 0;
    } else if (mType_ == PredicateDataType::DECIMAL) {
      mPrecision_ = r.mPrecision_;
      mScale_ = r.mScale_;
      mValue_ = r.mValue_;
    } else if (mType_ == PredicateDataType::TIMESTAMP) {
      mValue_.TimeStampVal = r.mValue_.TimeStampVal;
    } else {
      mValue_ = r.mValue_;
      mPrecision_ = 0;
      mScale_ = 0;
    }
  }

  Literal::~Literal() {
    if (mType_ == PredicateDataType::STRING && mValue_.Buffer) {
      delete[] mValue_.Buffer;
      mValue_.Buffer = nullptr;
    }
  }

  Literal& Literal::operator=(const Literal& r) {
    if (this != &r) {
      if (mType_ == PredicateDataType::STRING && mValue_.Buffer) {
        delete[] mValue_.Buffer;
        mValue_.Buffer = nullptr;
      }

      mType_ = r.mType_;
      mSize_ = r.mSize_;
      mIsNull_ = r.mIsNull_;
      mPrecision_ = r.mPrecision_;
      mScale_ = r.mScale_;
      if (mType_ == PredicateDataType::STRING) {
        mValue_.Buffer = new char[r.mSize_];
        memcpy(mValue_.Buffer, r.mValue_.Buffer, r.mSize_);
      } else if (mType_ == PredicateDataType::TIMESTAMP) {
        mValue_.TimeStampVal = r.mValue_.TimeStampVal;
      } else {
        mValue_ = r.mValue_;
      }
      mHashCode_ = r.mHashCode_;
    }
    return *this;
  }

  std::string Literal::toString() const {
    if (mIsNull_) {
      return "null";
    }

    std::ostringstream sstream;
    switch (mType_) {
      case PredicateDataType::LONG:
        sstream << mValue_.IntVal;
        break;
      case PredicateDataType::DATE:
        sstream << mValue_.DateVal;
        break;
      case PredicateDataType::TIMESTAMP:
        sstream << mValue_.TimeStampVal.second << "." << mValue_.TimeStampVal.nanos;
        break;
      case PredicateDataType::FLOAT:
        sstream << mValue_.DoubleVal;
        break;
      case PredicateDataType::BOOLEAN:
        sstream << (mValue_.BooleanVal ? "true" : "false");
        break;
      case PredicateDataType::STRING:
        sstream << std::string(mValue_.Buffer, mSize_);
        break;
      case PredicateDataType::DECIMAL:
        sstream << mValue_.DecimalVal.toDecimalString(mScale_);
        break;
    }
    return sstream.str();
  }

  size_t Literal::hashCode() const {
    if (mIsNull_) {
      return 0;
    }

    switch (mType_) {
      case PredicateDataType::LONG:
        return std::hash<int64_t>{}(mValue_.IntVal);
      case PredicateDataType::DATE:
        return std::hash<int64_t>{}(mValue_.DateVal);
      case PredicateDataType::TIMESTAMP:
        return std::hash<int64_t>{}(mValue_.TimeStampVal.second) * 17 +
               std::hash<int32_t>{}(mValue_.TimeStampVal.nanos);
      case PredicateDataType::FLOAT:
        return std::hash<double>{}(mValue_.DoubleVal);
      case PredicateDataType::BOOLEAN:
        return std::hash<bool>{}(mValue_.BooleanVal);
      case PredicateDataType::STRING:
        return std::hash<std::string>{}(std::string(mValue_.Buffer, mSize_));
      case PredicateDataType::DECIMAL:
        // current glibc does not support hash<int128_t>
        return std::hash<int64_t>{}(mValue_.IntVal);
      default:
        return 0;
    }
  }

  bool Literal::operator==(const Literal& r) const {
    if (this == &r) {
      return true;
    }
    if (mHashCode_ != r.mHashCode_ || mType_ != r.mType_ || mIsNull_ != r.mIsNull_) {
      return false;
    }

    if (mIsNull_) {
      return true;
    }

    switch (mType_) {
      case PredicateDataType::LONG:
        return mValue_.IntVal == r.mValue_.IntVal;
      case PredicateDataType::DATE:
        return mValue_.DateVal == r.mValue_.DateVal;
      case PredicateDataType::TIMESTAMP:
        return mValue_.TimeStampVal == r.mValue_.TimeStampVal;
      case PredicateDataType::FLOAT:
        return std::fabs(mValue_.DoubleVal - r.mValue_.DoubleVal) <
               std::numeric_limits<double>::epsilon();
      case PredicateDataType::BOOLEAN:
        return mValue_.BooleanVal == r.mValue_.BooleanVal;
      case PredicateDataType::STRING:
        return mSize_ == r.mSize_ && memcmp(mValue_.Buffer, r.mValue_.Buffer, mSize_) == 0;
      case PredicateDataType::DECIMAL:
        return mValue_.DecimalVal == r.mValue_.DecimalVal;
      default:
        return true;
    }
  }

  bool Literal::operator!=(const Literal& r) const {
    return !(*this == r);
  }

  inline void validate(const bool& isNull, const PredicateDataType& type,
                       const PredicateDataType& expected) {
    if (isNull) {
      throw std::logic_error("cannot get value when it is null!");
    }
    if (type != expected) {
      throw std::logic_error("predicate type mismatch");
    }
  }

  int64_t Literal::getLong() const {
    validate(mIsNull_, mType_, PredicateDataType::LONG);
    return mValue_.IntVal;
  }

  int64_t Literal::getDate() const {
    validate(mIsNull_, mType_, PredicateDataType::DATE);
    return mValue_.DateVal;
  }

  Literal::Timestamp Literal::getTimestamp() const {
    validate(mIsNull_, mType_, PredicateDataType::TIMESTAMP);
    return mValue_.TimeStampVal;
  }

  double Literal::getFloat() const {
    validate(mIsNull_, mType_, PredicateDataType::FLOAT);
    return mValue_.DoubleVal;
  }

  std::string Literal::getString() const {
    validate(mIsNull_, mType_, PredicateDataType::STRING);
    return std::string(mValue_.Buffer, mSize_);
  }

  bool Literal::getBool() const {
    validate(mIsNull_, mType_, PredicateDataType::BOOLEAN);
    return mValue_.BooleanVal;
  }

  Decimal Literal::getDecimal() const {
    validate(mIsNull_, mType_, PredicateDataType::DECIMAL);
    return Decimal(mValue_.DecimalVal, mScale_);
  }

}  // namespace orc
