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

#ifndef TYPE_IMPL_HH
#define TYPE_IMPL_HH

#include "orc/Adaptor.hh"
#include "orc/Vector.hh"

#include "wrap/orc-proto-wrapper.hh"

#include <vector>

namespace orc {

  class TypeImpl: public Type {
  private:
    int64_t columnId;
    TypeKind kind;
    std::vector<Type*> subTypes;
    std::vector<std::string> fieldNames;
    uint64_t subtypeCount;
    uint64_t maxLength;
    uint64_t precision;
    uint64_t scale;

  public:
    /**
     * Create most of the primitive types.
     */
    TypeImpl(TypeKind kind);

    /**
     * Create char and varchar type.
     */
    TypeImpl(TypeKind kind, uint64_t maxLength);

    /**
     * Create decimal type.
     */
    TypeImpl(TypeKind kind, uint64_t precision,
             uint64_t scale);

    /**
     * Create struct type.
     */
    TypeImpl(TypeKind kind,
             const std::vector<Type*>& types,
             const std::vector<std::string>& fieldNames);

    /**
     * Create list, map, and union type.
     */
    TypeImpl(TypeKind kind, const std::vector<Type*>& types);

    virtual ~TypeImpl();

    int64_t assignIds(int64_t root) override;

    int64_t getColumnId() const override;

    TypeKind getKind() const override;

    uint64_t getSubtypeCount() const override;

    const Type& getSubtype(uint64_t i) const override;

    const std::string& getFieldName(uint64_t i) const override;

    uint64_t getMaximumLength() const override;

    uint64_t getPrecision() const override;

    uint64_t getScale() const override;

    std::string toString() const override;

    Type& addStructField(std::unique_ptr<Type> fieldType,
                         const std::string& fieldName) override;
  };

  std::unique_ptr<Type> convertType(const proto::Type& type,
                                    const proto::Footer& footer);
}

#endif
