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

#include "orc/Vector.hh"

#include "Adaptor.hh"
#include "Exceptions.hh"

#include <iostream>
#include <sstream>
#include <cstdlib>

namespace orc {

  ColumnVectorBatch::ColumnVectorBatch(uint64_t cap,
                                       MemoryPool& pool
                                       ): capacity(cap),
                                          numElements(0),
                                          notNull(pool, cap, "ColumnBatch.notNull"),
                                          hasNulls(false),
                                          memoryPool(pool) {
    // PASS
  }

  ColumnVectorBatch::~ColumnVectorBatch() {
    // PASS
  }

  void ColumnVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      capacity = cap;
      notNull.resize(cap);
    }
  }

  uint64_t ColumnVectorBatch::memoryUse(std::string offset) {
    return notNull.capacity() * sizeof(char);
  }

  LongVectorBatch::LongVectorBatch(uint64_t capacity, MemoryPool& pool
                     ): ColumnVectorBatch(capacity, pool),
                        data(pool, capacity, "LongBatch.data") {
    // PASS
    std::cout << "Long created with capacity " << capacity << std::endl;
  }

  LongVectorBatch::~LongVectorBatch() {
    // PASS
  }

  std::string LongVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Long vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void LongVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
    }
    std::cout << "Long resized to " << cap << std::endl;
  }

  uint64_t LongVectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
          + data.capacity() * sizeof(int64_t);

    std::cout << offset << "Long memory " << memory << std::endl;
    return memory;
  }

  DoubleVectorBatch::DoubleVectorBatch(uint64_t capacity, MemoryPool& pool
                   ): ColumnVectorBatch(capacity, pool),
                      data(pool, capacity, "DoubleBatch.data") {
    // PASS
    std::cout << "Double created with capacity " << capacity << std::endl;
  }

  DoubleVectorBatch::~DoubleVectorBatch() {
    // PASS
  }

  std::string DoubleVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Double vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void DoubleVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
    }
  }

  uint64_t DoubleVectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
          + data.capacity() * sizeof(double);
    std::cout << offset << "Double memory " << memory << std::endl;
    return memory;
  }

  StringVectorBatch::StringVectorBatch(uint64_t capacity, MemoryPool& pool
               ): ColumnVectorBatch(capacity, pool),
                  data(pool, capacity, "StringBatch.data"),
                  length(pool, capacity, "StringBatch.length") {
    // PASS
    std::cout << "String created with capacity " << capacity << std::endl;
  }

  StringVectorBatch::~StringVectorBatch() {
    // PASS
  }

  std::string StringVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Byte vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void StringVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
      length.resize(cap);
    }
    std::cout << "String resized to " << cap << std::endl;
  }

  uint64_t StringVectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
          + data.capacity() * sizeof(char*)
          + length.capacity() * sizeof(int64_t);
    std::cout << offset << "String memory " << memory << std::endl;
    return memory;
  }

  StructVectorBatch::StructVectorBatch(uint64_t cap, MemoryPool& pool
                                        ): ColumnVectorBatch(cap, pool) {
    // PASS
    std::cout << "Struct created with capacity " << capacity << std::endl;
  }

  StructVectorBatch::~StructVectorBatch() {
    for (uint64_t i=0; i<this->fields.size(); i++) {
      delete this->fields[i];
    }
  }

  std::string StructVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Struct vector <" << numElements << " of " << capacity
           << "; ";
    for(std::vector<ColumnVectorBatch*>::const_iterator ptr=fields.begin();
        ptr != fields.end(); ++ptr) {
      buffer << (*ptr)->toString() << "; ";
    }
    buffer << ">";
    return buffer.str();
  }

  void StructVectorBatch::resize(uint64_t cap) {
    ColumnVectorBatch::resize(cap);
    std::cout << "Struct resized to " << cap << std::endl;
//    if (cap == 2)
//      throw NotImplementedYet("AAA!");
  }

  uint64_t StructVectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse();
    for (unsigned int i=0; i < fields.size(); i++) {
      memory += fields[i]->memoryUse(offset + "---");
    }

    std::cout << offset << "Struct memory " << memory << std::endl;
    return memory;
  }

  ListVectorBatch::ListVectorBatch(uint64_t cap, MemoryPool& pool
                   ): ColumnVectorBatch(cap, pool),
                      offsets(pool, cap+1, "ListBatch.offsets") {
    // PASS
    std::cout << "List created with capacity " << capacity << std::endl;
  }

  ListVectorBatch::~ListVectorBatch() {
    // PASS
  }

  std::string ListVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "List vector <" << elements->toString() << " with "
           << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void ListVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      offsets.resize(cap + 1);
    }
    std::cout << "List resized to " << cap << std::endl;
  }

  uint64_t ListVectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
          + offsets.capacity() * sizeof(int64_t)
          + elements->memoryUse(offset + "---");
    std::cout << offset << "List memory " << memory << std::endl;
    return memory;
  }

  MapVectorBatch::MapVectorBatch(uint64_t cap, MemoryPool& pool
                 ): ColumnVectorBatch(cap, pool),
                    offsets(pool, cap+1, "MapBatch.offsets") {
    // PASS
    std::cout << "Map created with capacity " << capacity << std::endl;
  }

  MapVectorBatch::~MapVectorBatch() {
    // PASS
  }

  std::string MapVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Map vector <" << keys->toString() << ", "
           << elements->toString() << " with "
           << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void MapVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      offsets.resize(cap + 1);
    }
  }

  uint64_t MapVectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
          + offsets.capacity() * sizeof(int64_t)
          + keys->memoryUse(offset + "---")
          + elements->memoryUse(offset + "---");
    std::cout << offset << "Map memory " << memory << std::endl;
    return memory;
  }

  UnionVectorBatch::UnionVectorBatch(uint64_t cap, MemoryPool& pool
                                     ): ColumnVectorBatch(cap, pool),
                                        tags(pool, cap, "UnionBatch.tags"),
                                        offsets(pool, cap, "UnionBatch.offsets") {
    // PASS
    std::cout << "Union created with capacity " << capacity << std::endl;
  }

  UnionVectorBatch::~UnionVectorBatch() {
    for (uint64_t i=0; i < children.size(); i++) {
      delete children[i];
    }
  }

  std::string UnionVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Union vector <";
    for(size_t i=0; i < children.size(); ++i) {
      if (i != 0) {
        buffer << ", ";
      }
      buffer << children[i]->toString();
    }
    buffer << "; with " << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void UnionVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      tags.resize(cap);
      offsets.resize(cap);
    }
  }

  uint64_t UnionVectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
                     + tags.capacity() * sizeof(unsigned char)
                     + offsets.capacity() * sizeof(uint64_t);
    for(size_t i=0; i < children.size(); ++i) {
      memory += children[i]->memoryUse(offset + "---");
    }

    std::cout << offset << "Union memory " << memory << std::endl;
    return memory;
  }

  Decimal64VectorBatch::Decimal64VectorBatch(uint64_t cap, MemoryPool& pool
                 ): ColumnVectorBatch(cap, pool),
                    values(pool, cap, "64Batch.values"),
                    readScales(pool, cap, "64Batch.readScales") {
    // PASS
    std::cout << "Dec64 created with capacity " << capacity << std::endl;
  }

  Decimal64VectorBatch::~Decimal64VectorBatch() {
    // PASS
  }

  std::string Decimal64VectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Decimal64 vector  with "
           << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void Decimal64VectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      values.resize(cap);
      readScales.resize(cap);
    }
  }

  uint64_t Decimal64VectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
          + (values.capacity() + readScales.capacity()) * sizeof(int64_t);
    std::cout << offset << "Dec64 memory " << memory << std::endl;
    return memory;
  }

  Decimal128VectorBatch::Decimal128VectorBatch(uint64_t cap, MemoryPool& pool
               ): ColumnVectorBatch(cap, pool),
                  values(pool, cap, "128Batch.values"),
                  readScales(pool, cap, "128Batch.readScales") {
    // PASS
    std::cout << "Dec128 created with capacity " << capacity << std::endl;
  }

  Decimal128VectorBatch::~Decimal128VectorBatch() {
    // PASS
  }

  std::string Decimal128VectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Decimal128 vector  with "
           << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void Decimal128VectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      values.resize(cap);
      readScales.resize(cap);
    }
  }

  uint64_t Decimal128VectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
          + values.capacity() * sizeof(Int128)
          + readScales.capacity() * sizeof(int64_t);

    std::cout << offset << "Dec128 memory " << memory << std::endl;
    return memory;
  }

  Decimal::Decimal(const Int128& _value,
                   int32_t _scale): value(_value), scale(_scale) {
    // PASS
  }

  Decimal::Decimal(const std::string& str) {
    std::size_t foundPoint = str.find(".");
    // no decimal point, it is int
    if(foundPoint == std::string::npos){
      value = Int128(str);
      scale = 0;
    }else{
      std::string copy(str);
      scale = static_cast<int32_t>(str.length() - foundPoint);
      value = Int128(copy.replace(foundPoint, 1, ""));
    }
  }

  std::string Decimal::toString() const {
    return value.toDecimalString(scale);
  }

  TimestampVectorBatch::TimestampVectorBatch(uint64_t capacity,
                                             MemoryPool& pool
                                             ): ColumnVectorBatch(capacity,
                                                                  pool),
                                                data(pool, capacity, "TimeBatch.data"),
                                                nanoseconds(pool, capacity, "TimeBatch.nanoseconds") {
    // PASS
    std::cout << "Time created with capacity " << capacity << std::endl;
  }

  TimestampVectorBatch::~TimestampVectorBatch() {
    // PASS
  }

  std::string TimestampVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Timestamp vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  void TimestampVectorBatch::resize(uint64_t cap) {
    if (capacity < cap) {
      ColumnVectorBatch::resize(cap);
      data.resize(cap);
      nanoseconds.resize(cap);
    }
  }

  uint64_t TimestampVectorBatch::memoryUse(std::string offset) {
    uint64_t memory = ColumnVectorBatch::memoryUse()
          + (data.capacity() + nanoseconds.capacity()) * sizeof(int64_t);

    std::cout << offset << "Time memory " << memory << std::endl;
    return memory;
  }
}
