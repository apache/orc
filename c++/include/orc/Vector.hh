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

#ifndef ORC_VECTOR_HH
#define ORC_VECTOR_HH

#include "orc/orc-config.hh"
#include "MemoryPool.hh"
#include "Int128.hh"

#include <list>
#include <memory>
#include <cstring>
#include <vector>
#include <stdexcept>
#include <cstdlib>
#include <iostream>

namespace orc {

  enum TypeKind {
    BOOLEAN = 0,
    BYTE = 1,
    SHORT = 2,
    INT = 3,
    LONG = 4,
    FLOAT = 5,
    DOUBLE = 6,
    STRING = 7,
    BINARY = 8,
    TIMESTAMP = 9,
    LIST = 10,
    MAP = 11,
    STRUCT = 12,
    UNION = 13,
    DECIMAL = 14,
    DATE = 15,
    VARCHAR = 16,
    CHAR = 17
  };

  std::string kind2String(TypeKind t);

  class Type {
  public:
    virtual ~Type();
    virtual int64_t assignIds(int64_t root) = 0;
    virtual int64_t getColumnId() const = 0;
    virtual TypeKind getKind() const = 0;
    virtual uint64_t getSubtypeCount() const = 0;
    virtual const Type& getSubtype(uint64_t typeId) const = 0;
    virtual const std::string& getFieldName(uint64_t fieldId) const = 0;
    virtual uint64_t getMaximumLength() const = 0;
    virtual uint64_t getPrecision() const = 0;
    virtual uint64_t getScale() const = 0;
    virtual std::string toString() const = 0;

    /**
     * Add a new field to a struct type.
     * @param fieldType the type of the new field
     * @param fieldName the name of the new field
     * @return a reference to the field's type
     */
    virtual Type& addStructField(ORC_UNIQUE_PTR<Type> fieldType,
                                 const std::string& fieldName) = 0;
  };

  const int64_t DEFAULT_DECIMAL_SCALE = 18;
  const int64_t DEFAULT_DECIMAL_PRECISION = 38;

  ORC_UNIQUE_PTR<Type> createPrimitiveType(TypeKind kind);
  ORC_UNIQUE_PTR<Type> createCharType(TypeKind kind,
				      uint64_t maxLength);
  ORC_UNIQUE_PTR<Type>
                createDecimalType(uint64_t precision=
                                    DEFAULT_DECIMAL_PRECISION,
                                  uint64_t scale=DEFAULT_DECIMAL_SCALE);

  ORC_UNIQUE_PTR<Type> createStructType();
  ORC_UNIQUE_PTR<Type>
    createStructType(std::vector<Type*> types,
                      std::vector<std::string> fieldNames);

#ifdef ORC_CXX_HAS_INITIALIZER_LIST
  std::unique_ptr<Type> createStructType(
      std::initializer_list<std::unique_ptr<Type> > types,
      std::initializer_list<std::string> fieldNames);
#endif

  ORC_UNIQUE_PTR<Type> createListType(ORC_UNIQUE_PTR<Type> elements);
  ORC_UNIQUE_PTR<Type> createMapType(ORC_UNIQUE_PTR<Type> key,
                                      ORC_UNIQUE_PTR<Type> value);
  ORC_UNIQUE_PTR<Type>
    createUnionType(std::vector<Type*> types);

  /**
   * The base class for each of the column vectors. This class handles
   * the generic attributes such as number of elements, capacity, and
   * notNull vector.
   */
  struct ColumnVectorBatch {
    ColumnVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~ColumnVectorBatch();

    // the number of slots available
    uint64_t capacity;
    // the number of current occupied slots
    uint64_t numElements;
    // an array of capacity length marking non-null values
    DataBuffer<char> notNull;
    // whether there are any null values
    bool hasNulls;

    // custom memory pool
    MemoryPool& memoryPool;

    /**
     * Generate a description of this vector as a string.
     */
    virtual std::string toString() const = 0;

    /**
     * Change the number of slots to at least the given capacity.
     * This function is not recursive into subtypes.
     */
    virtual void resize(uint64_t capacity);

  private:
    ColumnVectorBatch(const ColumnVectorBatch&);
    ColumnVectorBatch& operator=(const ColumnVectorBatch&);
  };

  struct LongVectorBatch: public ColumnVectorBatch {
    LongVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~LongVectorBatch();

    DataBuffer<int64_t> data;
    std::string toString() const;
    void resize(uint64_t capacity);
  };

  struct DoubleVectorBatch: public ColumnVectorBatch {
    DoubleVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~DoubleVectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    DataBuffer<double> data;
  };

  struct StringVectorBatch: public ColumnVectorBatch {
    StringVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~StringVectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    // pointers to the start of each string
    DataBuffer<char*> data;
    // the length of each string
    DataBuffer<int64_t> length;
  };

  struct StructVectorBatch: public ColumnVectorBatch {
    StructVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~StructVectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    std::vector<ColumnVectorBatch*> fields;
  };

  struct ListVectorBatch: public ColumnVectorBatch {
    ListVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~ListVectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    /**
     * The offset of the first element of each list.
     * The length of list i is startOffset[i+1] - startOffset[i].
     */
    DataBuffer<int64_t> offsets;

    // the concatenated elements
    ORC_UNIQUE_PTR<ColumnVectorBatch> elements;
  };

  struct MapVectorBatch: public ColumnVectorBatch {
    MapVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~MapVectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    /**
     * The offset of the first element of each list.
     * The length of list i is startOffset[i+1] - startOffset[i].
     */
    DataBuffer<int64_t> offsets;

    // the concatenated keys
    ORC_UNIQUE_PTR<ColumnVectorBatch> keys;
    // the concatenated elements
    ORC_UNIQUE_PTR<ColumnVectorBatch> elements;
  };

  struct UnionVectorBatch: public ColumnVectorBatch {
    UnionVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~UnionVectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    /**
     * For each value, which element of children has the value.
     */
    DataBuffer<unsigned char> tags;

    /**
     * For each value, the index inside of the child ColumnVectorBatch.
     */
    DataBuffer<uint64_t> offsets;

    // the sub-columns
    std::vector<ColumnVectorBatch*> children;
  };

  struct Decimal {
    Decimal(const Int128& value, int32_t scale);
    explicit Decimal(const std::string& value);

    std::string toString() const;
    Int128 value;
    int32_t scale;
  };

  struct Decimal64VectorBatch: public ColumnVectorBatch {
    Decimal64VectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~Decimal64VectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    // total number of digits
    int32_t precision;
    // the number of places after the decimal
    int32_t scale;

    // the numeric values
    DataBuffer<int64_t> values;

  protected:
    /**
     * Contains the scales that were read from the file. Should NOT be
     * used.
     */
    DataBuffer<int64_t> readScales;
    friend class Decimal64ColumnReader;
  };

  struct Decimal128VectorBatch: public ColumnVectorBatch {
    Decimal128VectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~Decimal128VectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    // total number of digits
    int32_t precision;
    // the number of places after the decimal
    int32_t scale;

    // the numeric values
    DataBuffer<Int128> values;

  protected:
    /**
     * Contains the scales that were read from the file. Should NOT be
     * used.
     */
    DataBuffer<int64_t> readScales;
    friend class Decimal128ColumnReader;
    friend class DecimalHive11ColumnReader;
  };

  /**
   * A column vector batch for storing timestamp values.
   * The timestamps are stored split into the time_t value (seconds since
   * 1 Jan 1970 00:00:00) and the nanoseconds within the time_t value.
   */
  struct TimestampVectorBatch: public ColumnVectorBatch {
    TimestampVectorBatch(uint64_t capacity, MemoryPool& pool);
    virtual ~TimestampVectorBatch();
    std::string toString() const;
    void resize(uint64_t capacity);

    // the number of seconds past 1 Jan 1970 00:00 UTC (aka time_t)
    DataBuffer<int64_t> data;

    // the nanoseconds of each value
    DataBuffer<int64_t> nanoseconds;
  };

}

#endif
