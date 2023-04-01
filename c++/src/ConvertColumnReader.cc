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

#include "ConvertColumnReader.hh"

namespace orc {

  // Assume that we are using tight numeric vector batch
  using BooleanVectorBatch = ByteVectorBatch;

  ConvertColumnReader::ConvertColumnReader(const Type& _readType, const Type& fileType,
                                           StripeStreams& stripe, bool _throwOnOverflow)
      : ColumnReader(_readType, stripe), readType(_readType), throwOnOverflow(_throwOnOverflow) {
    reader = buildReader(fileType, stripe, /*useTightNumericVector=*/true,
                         /*throwOnOverflow=*/false, /*convertToReadType*/ false);
    data =
        fileType.createRowBatch(0, memoryPool, /*encoded=*/false, /*useTightNumericVector=*/true);
  }

  void ConvertColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    if (!rowBatch.isTight) {
      throw SchemaEvolutionError(
          "SchemaEvolution only support tight vector, please create ColumnVectorBatch with option "
          "useTightNumericVector");
    }
    reader->next(*data, numValues, notNull);
    rowBatch.resize(data->capacity);
    rowBatch.numElements = data->numElements;
    rowBatch.hasNulls = data->hasNulls;
    if (!rowBatch.hasNulls) {
      memset(rowBatch.notNull.data(), 1, data->notNull.size());
    } else {
      memcpy(rowBatch.notNull.data(), data->notNull.data(), data->notNull.size());
    }
  }

  uint64_t ConvertColumnReader::skip(uint64_t numValues) {
    return reader->skip(numValues);
  }

  void ConvertColumnReader::seekToRowGroup(
      std::unordered_map<uint64_t, PositionProvider>& positions) {
    reader->seekToRowGroup(positions);
  }

  static inline bool canFitInLong(double value) {
    constexpr double MIN_LONG_AS_DOUBLE = -0x1p63;
    constexpr double MAX_LONG_AS_DOUBLE_PLUS_ONE = 0x1p63;
    return ((MIN_LONG_AS_DOUBLE - value < 1.0) && (value < MAX_LONG_AS_DOUBLE_PLUS_ONE));
  }

  template <typename FileType, typename ReadType>
  static inline void handleOverflow(ColumnVectorBatch& dstBatch, uint64_t idx, bool shouldThrow) {
    if (!shouldThrow) {
      dstBatch.notNull.data()[idx] = 0;
      dstBatch.hasNulls = true;
    } else {
      std::ostringstream ss;
      ss << "Overflow when convert from " << typeid(FileType).name() << " to "
         << typeid(ReadType).name();
      throw SchemaEvolutionError(ss.str());
    }
  }

  // return false if overflow
  template <typename ReadType>
  static bool downCastToInteger(ReadType& dstValue, int64_t inputLong) {
    dstValue = static_cast<ReadType>(inputLong);
    if constexpr (std::is_same<ReadType, int64_t>::value) {
      return true;
    }
    if (static_cast<int64_t>(dstValue) != inputLong) {
      return false;
    }
    return true;
  }

  // set null or throw exception if overflow
  template <typename ReadType, typename FileType>
  static inline void convertNumericElement(const FileType& srcValue, ReadType& destValue,
                                           ColumnVectorBatch& destBatch, uint64_t idx,
                                           bool shouldThrow) {
    constexpr bool isFileTypeFloatingPoint(std::is_floating_point<FileType>::value);
    constexpr bool isReadTypeFloatingPoint(std::is_floating_point<ReadType>::value);
    int64_t longValue = static_cast<int64_t>(srcValue);
    if (isFileTypeFloatingPoint) {
      if (isReadTypeFloatingPoint) {
        destValue = static_cast<ReadType>(srcValue);
      } else {
        if (!canFitInLong(static_cast<double>(srcValue)) ||
            !downCastToInteger(destValue, longValue)) {
          handleOverflow<FileType, ReadType>(destBatch, idx, shouldThrow);
          return;
        }
      }
    } else {
      if (isReadTypeFloatingPoint) {
        destValue = static_cast<ReadType>(srcValue);
        if (destValue != destValue) {  // check is NaN
          handleOverflow<FileType, ReadType>(destBatch, idx, shouldThrow);
        }
      } else {
        if (!downCastToInteger(destValue, static_cast<int64_t>(srcValue))) {
          handleOverflow<FileType, ReadType>(destBatch, idx, shouldThrow);
        }
      }
    }
  }

  // { boolean, byte, short, int, long, float, double } ->
  // { byte, short, int, long, float, double }
  template <typename FileTypeBatch, typename ReadTypeBatch, typename ReadType>
  class NumericConvertColumnReader : public ConvertColumnReader {
   public:
    NumericConvertColumnReader(const Type& _readType, const Type& fileType, StripeStreams& stripe,
                               bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);
      const auto& srcBatch = dynamic_cast<const FileTypeBatch&>(*data);
      auto& dstBatch = dynamic_cast<ReadTypeBatch&>(rowBatch);
      if (rowBatch.hasNulls) {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          if (rowBatch.notNull[i]) {
            convertNumericElement<ReadType>(srcBatch.data[i], dstBatch.data[i], rowBatch, i,
                                            throwOnOverflow);
          }
        }
      } else {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          convertNumericElement<ReadType>(srcBatch.data[i], dstBatch.data[i], rowBatch, i,
                                          throwOnOverflow);
        }
      }
    }
  };

  // { boolean, byte, short, int, long, float, double } -> { boolean }
  template <typename FileTypeBatch>
  class NumericConvertColumnReader<FileTypeBatch, BooleanVectorBatch, bool>
      : public ConvertColumnReader {
   public:
    NumericConvertColumnReader(const Type& _readType, const Type& fileType, StripeStreams& stripe,
                               bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);
      const auto& srcBatch = dynamic_cast<const FileTypeBatch&>(*data);
      auto& dstBatch = dynamic_cast<BooleanVectorBatch&>(rowBatch);
      if (rowBatch.hasNulls) {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          if (rowBatch.notNull[i]) {
            dstBatch.data[i] = (static_cast<int64_t>(srcBatch.data[i]) == 0 ? 0 : 1);
          }
        }
      } else {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          dstBatch.data[i] = (static_cast<int64_t>(srcBatch.data[i]) == 0 ? 0 : 1);
        }
      }
    }
  };

#define DEFINE_NUMERIC_CONVERT_READER(FROM, TO, TYPE) \
  using FROM##To##TO##ColumnReader =                  \
      NumericConvertColumnReader<FROM##VectorBatch, TO##VectorBatch, TYPE>;

  DEFINE_NUMERIC_CONVERT_READER(Boolean, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Short, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Short, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Int, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Float, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Short, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Short, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Int, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Int, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Int, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Long, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Long, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Long, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Long, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Float, float)
  // Floating to integer
  DEFINE_NUMERIC_CONVERT_READER(Float, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Float, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Float, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Float, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Float, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Double, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Long, int64_t)
  // Integer to Floating
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Short, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Int, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Long, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Short, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Int, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Long, Double, double)

#define CASE_CREATE_READER(TYPE, CONVERT) \
  case TYPE:                              \
    return std::make_unique<CONVERT##ColumnReader>(_readType, fileType, stripe, throwOnOverflow);

#define CASE_EXCEPTION                                                                 \
  default:                                                                             \
    throw SchemaEvolutionError("Cannot convert from " + fileType.toString() + " to " + \
                               _readType.toString());

  std::unique_ptr<ColumnReader> buildConvertReader(const Type& fileType, StripeStreams& stripe,
                                                   bool throwOnOverflow) {
    const auto& _readType = *stripe.getSchemaEvolution()->getReadType(fileType);

    switch (fileType.getKind()) {
      case BOOLEAN: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BYTE, BooleanToByte);
          CASE_CREATE_READER(SHORT, BooleanToShort);
          CASE_CREATE_READER(INT, BooleanToInt);
          CASE_CREATE_READER(LONG, BooleanToLong);
          CASE_CREATE_READER(FLOAT, BooleanToFloat);
          CASE_CREATE_READER(DOUBLE, BooleanToDouble);
          case BOOLEAN:
          case STRING:
          case BINARY:
          case TIMESTAMP:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DECIMAL:
          case DATE:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP_INSTANT:
            CASE_EXCEPTION
        }
      }
      case BYTE: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, ByteToBoolean);
          CASE_CREATE_READER(SHORT, ByteToShort);
          CASE_CREATE_READER(INT, ByteToInt);
          CASE_CREATE_READER(LONG, ByteToLong);
          CASE_CREATE_READER(FLOAT, ByteToFloat);
          CASE_CREATE_READER(DOUBLE, ByteToDouble);
          case BYTE:
          case STRING:
          case BINARY:
          case TIMESTAMP:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DECIMAL:
          case DATE:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP_INSTANT:
            CASE_EXCEPTION
        }
      }
      case SHORT: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, ShortToBoolean);
          CASE_CREATE_READER(BYTE, ShortToByte);
          CASE_CREATE_READER(INT, ShortToInt);
          CASE_CREATE_READER(LONG, ShortToLong);
          CASE_CREATE_READER(FLOAT, ShortToFloat);
          CASE_CREATE_READER(DOUBLE, ShortToDouble);
          case SHORT:
          case STRING:
          case BINARY:
          case TIMESTAMP:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DECIMAL:
          case DATE:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP_INSTANT:
            CASE_EXCEPTION
        }
      }
      case INT: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, IntToBoolean);
          CASE_CREATE_READER(BYTE, IntToByte);
          CASE_CREATE_READER(SHORT, IntToShort);
          CASE_CREATE_READER(LONG, IntToLong);
          CASE_CREATE_READER(FLOAT, IntToFloat);
          CASE_CREATE_READER(DOUBLE, IntToDouble);
          case INT:
          case STRING:
          case BINARY:
          case TIMESTAMP:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DECIMAL:
          case DATE:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP_INSTANT:
            CASE_EXCEPTION
        }
      }
      case LONG: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, LongToBoolean);
          CASE_CREATE_READER(BYTE, LongToByte);
          CASE_CREATE_READER(SHORT, LongToShort);
          CASE_CREATE_READER(INT, LongToInt);
          CASE_CREATE_READER(FLOAT, LongToFloat);
          CASE_CREATE_READER(DOUBLE, LongToDouble);
          case LONG:
          case STRING:
          case BINARY:
          case TIMESTAMP:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DECIMAL:
          case DATE:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP_INSTANT:
            CASE_EXCEPTION
        }
      }
      case FLOAT: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, FloatToBoolean);
          CASE_CREATE_READER(BYTE, FloatToByte);
          CASE_CREATE_READER(SHORT, FloatToShort);
          CASE_CREATE_READER(INT, FloatToInt);
          CASE_CREATE_READER(LONG, FloatToLong);
          CASE_CREATE_READER(DOUBLE, FloatToDouble);
          case FLOAT:
          case STRING:
          case BINARY:
          case TIMESTAMP:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DECIMAL:
          case DATE:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP_INSTANT:
            CASE_EXCEPTION
        }
      }
      case DOUBLE: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, DoubleToBoolean);
          CASE_CREATE_READER(BYTE, DoubleToByte);
          CASE_CREATE_READER(SHORT, DoubleToShort);
          CASE_CREATE_READER(INT, DoubleToInt);
          CASE_CREATE_READER(LONG, DoubleToLong);
          CASE_CREATE_READER(FLOAT, DoubleToFloat);
          case DOUBLE:
          case STRING:
          case BINARY:
          case TIMESTAMP:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DECIMAL:
          case DATE:
          case VARCHAR:
          case CHAR:
          case TIMESTAMP_INSTANT:
            CASE_EXCEPTION
        }
      }
      case STRING:
      case BINARY:
      case TIMESTAMP:
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      case DECIMAL:
      case DATE:
      case VARCHAR:
      case CHAR:
      case TIMESTAMP_INSTANT:
        CASE_EXCEPTION
    }
  }

#undef DEFINE_NUMERIC_CONVERT_READER
#undef CASE_CREATE_READER
#undef CASE_EXCEPTION

}  // namespace orc
