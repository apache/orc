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

  Literal::Literal(PredicateType type) {
    mType = type;
    mValue.DecimalVal = 0;
    mSize = 0;
    mIsNull = true;
    mPrecision = 0;
    mScale = 0;
    mHashCode = 0;
  }

  Literal::Literal(int64_t val) {
    mType = PredicateType::LONG;
    mValue.IntVal = val;
    mSize = sizeof(val);
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(double val) {
    mType = PredicateType::FLOAT;
    mValue.DoubleVal = val;
    mSize = sizeof(val);
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(bool val) {
    mType = PredicateType::BOOLEAN;
    mValue.BooleanVal = val;
    mSize = sizeof(val);
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(PredicateType type, int64_t val) {
    if (type != PredicateType::DATE && type != PredicateType::TIMESTAMP) {
      throw std::invalid_argument("only DATE & TIMESTAMP are supported here!");
    }
    mType = type;
    mValue.IntVal = val;
    mSize = sizeof(val);
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(const char * str, size_t size) {
    mType = PredicateType::STRING;
    mValue.Buffer = new char[size];
    memcpy(mValue.Buffer, str, size);
    mSize = size;
    mIsNull = false;
    mPrecision = 0;
    mScale = 0;
    mHashCode = hashCode();
  }

  Literal::Literal(Int128 val, int32_t precision, int32_t scale) {
    mType = PredicateType::DECIMAL;
    mValue.DecimalVal = val;
    mPrecision = precision;
    mScale = scale;
    mSize = sizeof(Int128);
    mIsNull = false;
    mHashCode = hashCode();
  }

  Literal::Literal(const Literal& r): mType(r.mType)
                                    , mSize(r.mSize)
                                    , mIsNull(r.mIsNull)
                                    , mHashCode(r.mHashCode) {
    if (mType == PredicateType::STRING) {
      mValue.Buffer = new char[r.mSize];
      memcpy(mValue.Buffer, r.mValue.Buffer, r.mSize);
      mPrecision = 0;
      mScale = 0;
    } else if (mType == PredicateType::DECIMAL) {
      mPrecision = r.mPrecision;
      mScale = r.mScale;
      mValue = r.mValue;
    } else {
      mValue = r.mValue;
      mPrecision = 0;
      mScale = 0;
    }
  }

  Literal::~Literal() {
    if (mType == PredicateType::STRING && mValue.Buffer) {
      delete [] mValue.Buffer;
      mValue.Buffer = nullptr;
    }
  }

  Literal& Literal::operator=(const Literal& r) {
    if (this != &r) {
      if (mType == PredicateType::STRING && mValue.Buffer) {
        delete [] mValue.Buffer;
        mValue.Buffer = nullptr;
      }

      mType = r.mType;
      mSize = r.mSize;
      mIsNull = r.mIsNull;
      mPrecision = r.mPrecision;
      mScale = r.mScale;
      if (mType == PredicateType::STRING) {
        mValue.Buffer = new char[r.mSize];
        memcpy(mValue.Buffer, r.mValue.Buffer, r.mSize);
      } else {
        mValue = r.mValue;
      }
      mHashCode = r.mHashCode;
    }
    return *this;
  }

  std::string Literal::toString() const {
    if (mIsNull) {
      return "null";
    }

    std::ostringstream sstream;
    std::string str;
    switch (mType) {
      case PredicateType::LONG:
        sstream << mValue.IntVal;
        break;
      case PredicateType::DATE:
        sstream << mValue.DateVal;
        break;
      case PredicateType::TIMESTAMP:
        sstream << mValue.TimeStampVal;
        break;
      case PredicateType::FLOAT:
        sstream << mValue.DoubleVal;
        break;
      case PredicateType::BOOLEAN:
        sstream << (mValue.BooleanVal ? "true" : "false");
        break;
      case PredicateType::STRING:
        str.assign(mValue.Buffer, mSize);
        sstream << str;
        break;
      case PredicateType::DECIMAL:
        sstream << mValue.DecimalVal.toDecimalString(mScale);
        break;
    }
    return sstream.str();
  }

  size_t Literal::hashCode() const {
    if (mIsNull) {
      return 0;
    }

    switch (mType) {
      case PredicateType::LONG:
        return std::hash<int64_t>{}(mValue.IntVal);
      case PredicateType::DATE:
        return std::hash<int64_t>{}(mValue.DateVal);
      case PredicateType::TIMESTAMP:
        return std::hash<int64_t>{}(mValue.TimeStampVal);
      case PredicateType::FLOAT:
        return std::hash<double>{}(mValue.DoubleVal);
      case PredicateType::BOOLEAN:
        return std::hash<bool>{}(mValue.BooleanVal);
      case PredicateType::STRING:
        return std::hash<std::string>{}(
          std::string(mValue.Buffer, mSize));
      case PredicateType::DECIMAL:
        // current glibc does not support hash<int128_t>
        return std::hash<int64_t>{}(mValue.IntVal);
      default:
        return 0;
    }
  }

  bool Literal::operator==(const Literal& r) const {
    if (this == &r) {
      return true;
    }
    if (mHashCode != r.mHashCode || mType != r.mType || mIsNull != r.mIsNull) {
      return false;
    }

    if (mIsNull) {
      return true;
    }

    switch (mType) {
      case PredicateType::LONG:
        return mValue.IntVal == r.mValue.IntVal;
      case PredicateType::DATE:
        return mValue.DateVal == r.mValue.DateVal;
      case PredicateType::TIMESTAMP:
        return mValue.TimeStampVal == r.mValue.TimeStampVal;
      case PredicateType::FLOAT:
        return std::fabs(mValue.DoubleVal - r.mValue.DoubleVal) <
          std::numeric_limits<double>::epsilon();
      case PredicateType::BOOLEAN:
        return mValue.BooleanVal == r.mValue.BooleanVal;
      case PredicateType::STRING:
        return mSize == r.mSize && memcmp(
          mValue.Buffer, r.mValue.Buffer, mSize) == 0;
      case PredicateType::DECIMAL:
        return mValue.DecimalVal == r.mValue.DecimalVal;
      default:
        return true;
    }
  }

  bool Literal::operator!=(const Literal& r) const {
    return !(*this == r);
  }

  int64_t Literal::getLong() const {
    if (mIsNull) {
      throw std::logic_error("cannot call getLong for null!");
    }
    if (mType != PredicateType::LONG) {
      throw std::logic_error("cannot call getLong for " + toString());
    }
    return mValue.IntVal;
  }

  int64_t Literal::getDate() const {
    if (mIsNull) {
      throw std::logic_error("cannot call getLong for null!");
    }
    if (mType != PredicateType::DATE) {
      throw std::logic_error("cannot call getDate for " + toString());
    }
    return mValue.DateVal;
  }

  int64_t Literal::getTimestamp() const {
    if (mIsNull) {
      throw std::logic_error("cannot call getLong for null!");
    }
    if (mType != PredicateType::TIMESTAMP) {
      throw std::logic_error("cannot call getTimestamp for " + toString());
    }
    return mValue.TimeStampVal;
  }

  double Literal::getFloat() const {
    if (mIsNull) {
      throw std::logic_error("cannot call getLong for null!");
    }
    if (mType != PredicateType::FLOAT) {
      throw std::logic_error("cannot call getFloat for " + toString());
    }
    return mValue.DoubleVal;
  }

  std::string Literal::getString() const {
    if (mIsNull) {
      throw std::logic_error("cannot call getLong for null!");
    }
    if (mType != PredicateType::STRING) {
      throw std::logic_error("cannot call getString for " + toString());
    }
    return std::string(mValue.Buffer, mSize);
  }

  bool Literal::getBool() const {
    if (mIsNull) {
      throw std::logic_error("cannot call getLong for null!");
    }
    if (mType != PredicateType::BOOLEAN) {
      throw std::logic_error("cannot call getBool for " + toString());
    }
    return mValue.BooleanVal;
  }

  Decimal Literal::getDecimal() const {
    if (mIsNull) {
      throw std::logic_error("cannot call getLong for null!");
    }
    if (mType != PredicateType::DECIMAL) {
      throw std::logic_error("cannot call getDecimal for " + toString());
    }
    return Decimal(mValue.DecimalVal, mScale);
  }

}
