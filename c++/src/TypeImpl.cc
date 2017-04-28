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

#include "Adaptor.hh"
#include "Exceptions.hh"
#include "TypeImpl.hh"

#include <iostream>
#include <sstream>

namespace orc {

  Type::~Type() {
    // PASS
  }

  TypeImpl::TypeImpl(TypeKind _kind) {
    parent = nullptr;
    columnId = -1;
    maximumColumnId = -1;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = 0;
  }

  TypeImpl::TypeImpl(TypeKind _kind, uint64_t _maxLength) {
    parent = nullptr;
    columnId = -1;
    maximumColumnId = -1;
    kind = _kind;
    maxLength = _maxLength;
    precision = 0;
    scale = 0;
    subtypeCount = 0;
  }

  TypeImpl::TypeImpl(TypeKind _kind, uint64_t _precision,
                     uint64_t _scale) {
    parent = nullptr;
    columnId = -1;
    maximumColumnId = -1;
    kind = _kind;
    maxLength = 0;
    precision = _precision;
    scale = _scale;
    subtypeCount = 0;
  }

  uint64_t TypeImpl::assignIds(uint64_t root) const {
    columnId = static_cast<int64_t>(root);
    uint64_t current = root + 1;
    for(uint64_t i=0; i < subtypeCount; ++i) {
      current = dynamic_cast<TypeImpl*>(subTypes[i])->assignIds(current);
    }
    maximumColumnId = static_cast<int64_t>(current) - 1;
    return current;
  }

  TypeImpl::~TypeImpl() {
    for (std::vector<Type*>::iterator it = subTypes.begin();
        it != subTypes.end(); it++) {
      delete (*it) ;
    }
  }

  void TypeImpl::ensureIdAssigned() const {
    if (columnId == -1) {
      const TypeImpl* root = this;
      while (root->parent != nullptr) {
        root = root->parent;
      }
      root->assignIds(0);
    }
  }

  uint64_t TypeImpl::getColumnId() const {
    ensureIdAssigned();
    return static_cast<uint64_t>(columnId);
  }

  uint64_t TypeImpl::getMaximumColumnId() const {
    ensureIdAssigned();
    return static_cast<uint64_t>(maximumColumnId);
  }

  TypeKind TypeImpl::getKind() const {
    return kind;
  }

  uint64_t TypeImpl::getSubtypeCount() const {
    return subtypeCount;
  }

  const Type* TypeImpl::getSubtype(uint64_t i) const {
    return subTypes[i];
  }

  const std::string& TypeImpl::getFieldName(uint64_t i) const {
    return fieldNames[i];
  }

  uint64_t TypeImpl::getMaximumLength() const {
    return maxLength;
  }

  uint64_t TypeImpl::getPrecision() const {
    return precision;
  }

  uint64_t TypeImpl::getScale() const {
    return scale;
  }

  void TypeImpl::setIds(uint64_t _columnId, uint64_t _maxColumnId) {
    columnId = static_cast<int64_t>(_columnId);
    maximumColumnId = static_cast<int64_t>(_maxColumnId);
  }

  void TypeImpl::addChildType(std::unique_ptr<Type> childType) {
    TypeImpl* child = dynamic_cast<TypeImpl*>(childType.release());
    subTypes.push_back(child);
    if (child != nullptr) {
      child->parent = this;
    }
    subtypeCount += 1;
  }

  Type* TypeImpl::addStructField(const std::string& fieldName,
                                 std::unique_ptr<Type> fieldType) {
    addChildType(std::move(fieldType));
    fieldNames.push_back(fieldName);
    return this;
  }

  Type* TypeImpl::addUnionChild(std::unique_ptr<Type> fieldType) {
    addChildType(std::move(fieldType));
    return this;
  }

  std::string TypeImpl::toString() const {
    switch (static_cast<int64_t>(kind)) {
    case BOOLEAN:
      return "boolean";
    case BYTE:
      return "tinyint";
    case SHORT:
      return "smallint";
    case INT:
      return "int";
    case LONG:
      return "bigint";
    case FLOAT:
      return "float";
    case DOUBLE:
      return "double";
    case STRING:
      return "string";
    case BINARY:
      return "binary";
    case TIMESTAMP:
      return "timestamp";
    case LIST:
      return "array<" + (subTypes[0] ? subTypes[0]->toString() : "void") + ">";
    case MAP:
      return "map<" + (subTypes[0] ? subTypes[0]->toString() : "void") + "," +
        (subTypes[1] ? subTypes[1]->toString() : "void") +  ">";
    case STRUCT: {
      std::string result = "struct<";
      for(size_t i=0; i < subTypes.size(); ++i) {
        if (i != 0) {
          result += ",";
        }
        result += fieldNames[i];
        result += ":";
        result += subTypes[i]->toString();
      }
      result += ">";
      return result;
    }
    case UNION: {
      std::string result = "uniontype<";
      for(size_t i=0; i < subTypes.size(); ++i) {
        if (i != 0) {
          result += ",";
        }
        result += subTypes[i]->toString();
      }
      result += ">";
      return result;
    }
    case DECIMAL: {
      std::stringstream result;
      result << "decimal(" << precision << "," << scale << ")";
      return result.str();
    }
    case DATE:
      return "date";
    case VARCHAR: {
      std::stringstream result;
      result << "varchar(" << maxLength << ")";
      return result.str();
    }
    case CHAR: {
      std::stringstream result;
      result << "char(" << maxLength << ")";
      return result.str();
    }
    default:
      throw NotImplementedYet("Unknown type");
    }
  }

  std::unique_ptr<ColumnVectorBatch>
  TypeImpl::createRowBatch(uint64_t capacity,
                           MemoryPool& memoryPool) const {
    switch (static_cast<int64_t>(kind)) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case DATE:
      return std::unique_ptr<ColumnVectorBatch>
        (new LongVectorBatch(capacity, memoryPool));

    case FLOAT:
    case DOUBLE:
      return std::unique_ptr<ColumnVectorBatch>
        (new DoubleVectorBatch(capacity, memoryPool));

    case STRING:
    case BINARY:
    case CHAR:
    case VARCHAR:
      return std::unique_ptr<ColumnVectorBatch>
        (new StringVectorBatch(capacity, memoryPool));

    case TIMESTAMP:
      return std::unique_ptr<ColumnVectorBatch>
        (new TimestampVectorBatch(capacity, memoryPool));

    case STRUCT: {
      StructVectorBatch *result = new StructVectorBatch(capacity, memoryPool);
      for(uint64_t i=0; i < getSubtypeCount(); ++i) {
          result->fields.push_back(getSubtype(i)->
                                   createRowBatch(capacity,
                                                  memoryPool).release());
      }
      return std::unique_ptr<ColumnVectorBatch>(result);
    }

    case LIST: {
      ListVectorBatch* result = new ListVectorBatch(capacity, memoryPool);
      if (getSubtype(0) != nullptr) {
        result->elements = getSubtype(0)->createRowBatch(capacity, memoryPool);
      }
      return std::unique_ptr<ColumnVectorBatch>(result);
    }

    case MAP: {
      MapVectorBatch* result = new MapVectorBatch(capacity, memoryPool);
      if (getSubtype(0) != nullptr) {
        result->keys = getSubtype(0)->createRowBatch(capacity, memoryPool);
      }
      if (getSubtype(1) != nullptr) {
        result->elements = getSubtype(1)->createRowBatch(capacity, memoryPool);
      }
      return std::unique_ptr<ColumnVectorBatch>(result);
    }

    case DECIMAL: {
      if (getPrecision() == 0 || getPrecision() > 18) {
        return std::unique_ptr<ColumnVectorBatch>
          (new Decimal128VectorBatch(capacity, memoryPool));
      } else {
        return std::unique_ptr<ColumnVectorBatch>
          (new Decimal64VectorBatch(capacity, memoryPool));
      }
    }

    case UNION: {
      UnionVectorBatch *result = new UnionVectorBatch(capacity, memoryPool);
      for(uint64_t i=0; i < getSubtypeCount(); ++i) {
          result->children.push_back(getSubtype(i)->createRowBatch(capacity,
                                                                   memoryPool)
                                     .release());
      }
      return std::unique_ptr<ColumnVectorBatch>(result);
    }

    default:
      throw NotImplementedYet("not supported yet");
    }
  }

  std::unique_ptr<Type> createPrimitiveType(TypeKind kind) {
    return std::unique_ptr<Type>(new TypeImpl(kind));
  }

  std::unique_ptr<Type> createCharType(TypeKind kind,
                                       uint64_t maxLength) {
    return std::unique_ptr<Type>(new TypeImpl(kind, maxLength));
  }

  std::unique_ptr<Type> createDecimalType(uint64_t precision,
                                          uint64_t scale) {
    return std::unique_ptr<Type>(new TypeImpl(DECIMAL, precision, scale));
  }

  std::unique_ptr<Type> createStructType() {
    return std::unique_ptr<Type>(new TypeImpl(STRUCT));
  }

  std::unique_ptr<Type> createListType(std::unique_ptr<Type> elements) {
    TypeImpl* result = new TypeImpl(LIST);
    result->addChildType(std::move(elements));
    return std::unique_ptr<Type>(result);
  }

  std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key,
                                      std::unique_ptr<Type> value) {
    TypeImpl* result = new TypeImpl(MAP);
    result->addChildType(std::move(key));
    result->addChildType(std::move(value));
    return std::unique_ptr<Type>(result);
  }

  std::unique_ptr<Type> createUnionType() {
    return std::unique_ptr<Type>(new TypeImpl(UNION));
  }

  std::string printProtobufMessage(const google::protobuf::Message& message);
  std::unique_ptr<Type> convertType(const proto::Type& type,
                                    const proto::Footer& footer) {
    switch (static_cast<int64_t>(type.kind())) {

    case proto::Type_Kind_BOOLEAN:
    case proto::Type_Kind_BYTE:
    case proto::Type_Kind_SHORT:
    case proto::Type_Kind_INT:
    case proto::Type_Kind_LONG:
    case proto::Type_Kind_FLOAT:
    case proto::Type_Kind_DOUBLE:
    case proto::Type_Kind_STRING:
    case proto::Type_Kind_BINARY:
    case proto::Type_Kind_TIMESTAMP:
    case proto::Type_Kind_DATE:
      return std::unique_ptr<Type>
        (new TypeImpl(static_cast<TypeKind>(type.kind())));

    case proto::Type_Kind_CHAR:
    case proto::Type_Kind_VARCHAR:
      return std::unique_ptr<Type>
        (new TypeImpl(static_cast<TypeKind>(type.kind()),
                      type.maximumlength()));

    case proto::Type_Kind_DECIMAL:
      return std::unique_ptr<Type>
        (new TypeImpl(DECIMAL, type.precision(), type.scale()));

    case proto::Type_Kind_LIST:
    case proto::Type_Kind_MAP:
    case proto::Type_Kind_UNION: {
      TypeImpl* result = new TypeImpl(static_cast<TypeKind>(type.kind()));
      for(int i=0; i < type.subtypes_size(); ++i) {
        result->addUnionChild(convertType(footer.types(static_cast<int>
                                                       (type.subtypes(i))),
                                          footer));
      }
      return std::unique_ptr<Type>(result);
    }

    case proto::Type_Kind_STRUCT: {
      TypeImpl* result = new TypeImpl(STRUCT);
      uint64_t size = static_cast<uint64_t>(type.subtypes_size());
      std::vector<Type*> typeList(size);
      std::vector<std::string> fieldList(size);
      for(int i=0; i < type.subtypes_size(); ++i) {
        result->addStructField(type.fieldnames(i),
                               convertType(footer.types(static_cast<int>
                                                        (type.subtypes(i))),
                                           footer));
      }
      return std::unique_ptr<Type>(result);
    }
    default:
      throw NotImplementedYet("Unknown type kind");
    }
  }

  /**
   * Build a clone of the file type, projecting columns from the selected
   * vector. This routine assumes that the parent of any selected column
   * is also selected. The column ids are copied from the fileType.
   * @param fileType the type in the file
   * @param selected is each column by id selected
   * @return a clone of the fileType filtered by the selection array
   */
  std::unique_ptr<Type> buildSelectedType(const Type *fileType,
                                          const std::vector<bool>& selected) {
    if (fileType == nullptr || !selected[fileType->getColumnId()]) {
      return std::unique_ptr<Type>();
    }

    TypeImpl* result;
    switch (static_cast<int>(fileType->getKind())) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case BINARY:
    case TIMESTAMP:
    case DATE:
      result = new TypeImpl(fileType->getKind());
      break;

    case DECIMAL:
      result= new TypeImpl(fileType->getKind(),
                           fileType->getPrecision(), fileType->getScale());
      break;

    case VARCHAR:
    case CHAR:
      result = new TypeImpl(fileType->getKind(), fileType->getMaximumLength());
      break;

    case LIST:
      result = new TypeImpl(fileType->getKind());
      result->addChildType(buildSelectedType(fileType->getSubtype(0),
                                             selected));
      break;

    case MAP:
      result = new TypeImpl(fileType->getKind());
      result->addChildType(buildSelectedType(fileType->getSubtype(0),
                                             selected));
      result->addChildType(buildSelectedType(fileType->getSubtype(1),
                                             selected));
      break;

    case STRUCT: {
      result = new TypeImpl(fileType->getKind());
      for(uint64_t child=0; child < fileType->getSubtypeCount(); ++child) {
        std::unique_ptr<Type> childType =
          buildSelectedType(fileType->getSubtype(child), selected);
        if (childType.get() != nullptr) {
          result->addStructField(fileType->getFieldName(child),
                                 std::move(childType));
        }
      }
      break;
    }

    case UNION: {
      result = new TypeImpl(fileType->getKind());
      for(uint64_t child=0; child < fileType->getSubtypeCount(); ++child) {
        std::unique_ptr<Type> childType =
          buildSelectedType(fileType->getSubtype(child), selected);
        if (childType.get() != nullptr) {
          result->addUnionChild(std::move(childType));
        }
      }
      break;
    }

    default:
      throw NotImplementedYet("Unknown type kind");
    }
    result->setIds(fileType->getColumnId(), fileType->getMaximumColumnId());
    return std::unique_ptr<Type>(result);
  }

}
