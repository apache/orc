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

#include "orc/Adaptor.hh"
#include "Exceptions.hh"
#include "TypeImpl.hh"

#include <iostream>
#include <sstream>

namespace orc {

  Type::~Type() {
    // PASS
  }

  TypeImpl::TypeImpl(TypeKind _kind) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = 0;
  }

  TypeImpl::TypeImpl(TypeKind _kind, uint64_t _maxLength) {
    columnId = 0;
    kind = _kind;
    maxLength = _maxLength;
    precision = 0;
    scale = 0;
    subtypeCount = 0;
  }

  TypeImpl::TypeImpl(TypeKind _kind, uint64_t _precision,
                     uint64_t _scale) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = _precision;
    scale = _scale;
    subtypeCount = 0;
  }

  TypeImpl::TypeImpl(TypeKind _kind,
                     const std::vector<Type*>& types,
                     const std::vector<std::string>& _fieldNames) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = static_cast<uint64_t>(types.size());
    subTypes.assign(types.begin(), types.end());
    fieldNames.assign(_fieldNames.begin(), _fieldNames.end());
  }

  TypeImpl::TypeImpl(TypeKind _kind, const std::vector<Type*>& types) {
    columnId = 0;
    kind = _kind;
    maxLength = 0;
    precision = 0;
    scale = 0;
    subtypeCount = static_cast<uint64_t>(types.size());
    subTypes.assign(types.begin(), types.end());
  }

  int64_t TypeImpl::assignIds(int64_t root) {
    columnId = root;
    int64_t current = root + 1;
    for(uint64_t i=0; i < subtypeCount; ++i) {
      current = subTypes[i]->assignIds(current);
    }
    return current;
  }

  TypeImpl::~TypeImpl() {
    for (std::vector<Type*>::iterator it = subTypes.begin();
        it != subTypes.end(); it++) {
      delete (*it) ;
    }
  }

  int64_t TypeImpl::getColumnId() const {
    return columnId;
  }

  TypeKind TypeImpl::getKind() const {
    return kind;
  }

  uint64_t TypeImpl::getSubtypeCount() const {
    return subtypeCount;
  }

  const Type& TypeImpl::getSubtype(uint64_t i) const {
    return *(subTypes[i]);
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

  Type& TypeImpl::addStructField(std::unique_ptr<Type> fieldType,
                                 const std::string& fieldName) {
    Type* result = fieldType.release();
    subTypes.push_back(result);
    fieldNames.push_back(fieldName);
    subtypeCount += 1;
    return *result;
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
      return "array<" + subTypes[0]->toString() + ">";
    case MAP:
      return "map<" + subTypes[0]->toString() + "," +
        subTypes[1]->toString() +  ">";
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

  std::unique_ptr<Type>
      createStructType(std::vector<Type*> types,
                       std::vector<std::string> fieldNames) {
    std::vector<Type*> typeVector(types.begin(), types.end());
    std::vector<std::string> fieldVector(fieldNames.begin(), fieldNames.end());

    return std::unique_ptr<Type>(new TypeImpl(STRUCT, typeVector,
                                              fieldVector));
  }

#ifdef ORC_CXX_HAS_INITIALIZER_LIST
  std::unique_ptr<Type> createStructType(
      std::initializer_list<std::unique_ptr<Type> > types,
      std::initializer_list<std::string> fieldNames) {
    std::vector<Type*> typeVector(types.size());
    std::vector<std::string> fieldVector(types.size());
    auto currentType = types.begin();
    auto endType = types.end();
    size_t current = 0;
    while (currentType != endType) {
      typeVector[current++] =
          const_cast<std::unique_ptr<Type>*>(currentType)->release();
      ++currentType;
    }
    fieldVector.insert(fieldVector.end(), fieldNames.begin(),
        fieldNames.end());
    return std::unique_ptr<Type>(new TypeImpl(STRUCT, typeVector,
        fieldVector));
  }
#endif

  std::unique_ptr<Type> createListType(std::unique_ptr<Type> elements) {
    std::vector<Type*> subtypes(1);
    subtypes[0] = elements.release();
    return std::unique_ptr<Type>(new TypeImpl(LIST, subtypes));
  }

  std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key,
                                      std::unique_ptr<Type> value) {
    std::vector<Type*> subtypes(2);
    subtypes[0] = key.release();
    subtypes[1] = value.release();
    return std::unique_ptr<Type>(new TypeImpl(MAP, subtypes));
  }

  std::unique_ptr<Type>
      createUnionType(std::vector<Type*> types) {
    std::vector<Type*> typeVector(types.begin(), types.end());
    return std::unique_ptr<Type>(new TypeImpl(UNION, typeVector));
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
      uint64_t size = static_cast<uint64_t>(type.subtypes_size());
      std::vector<Type*> typeList(size);
      for(int i=0; i < type.subtypes_size(); ++i) {
        typeList[static_cast<uint64_t>(i)] =
          convertType(footer.types(static_cast<int>(type.subtypes(i))),
                      footer).release();
      }
      return std::unique_ptr<Type>
        (new TypeImpl(static_cast<TypeKind>(type.kind()), typeList));
    }

    case proto::Type_Kind_STRUCT: {
      uint64_t size = static_cast<uint64_t>(type.subtypes_size());
      std::vector<Type*> typeList(size);
      std::vector<std::string> fieldList(size);
      for(int i=0; i < type.subtypes_size(); ++i) {
        typeList[static_cast<uint64_t>(i)] =
          convertType(footer.types(static_cast<int>(type.subtypes(i))),
                      footer).release();
        fieldList[static_cast<uint64_t>(i)] = type.fieldnames(i);
      }
      return std::unique_ptr<Type>
        (new TypeImpl(STRUCT, typeList, fieldList));
    }
    default:
      throw NotImplementedYet("Unknown type kind");
    }
  }

  std::string kind2String(TypeKind t) {
      std::string name ;
      switch(static_cast<int64_t>(t)) {
        case BOOLEAN: { name = "BOOLEAN"; break; }
        case BYTE: { name = "TINYINT"; break; }
        case SHORT: { name = "SMALLINT"; break; }
        case INT: { name = "INT"; break; }
        case LONG: { name = "BIGINT"; break; }
        case FLOAT: { name = "FLOAT"; break; }
        case DOUBLE: { name = "DOUBLE"; break; }
        case STRING: { name = "STRING"; break; }
        case BINARY: { name = "BINARY"; break; }
        case TIMESTAMP: { name = "TIMESTAMP"; break; }
        case LIST: { name = "LIST"; break; }
        case MAP: { name = "MAP"; break; }
        case STRUCT: { name = "STRUCT"; break; }
        case UNION: { name = "UNION"; break; }
        case DECIMAL: { name = "DECIMAL"; break; }
        case DATE: { name = "DATE"; break; }
        case VARCHAR: { name = "VARCHAR"; break; }
        case CHAR: { name = "CHAR"; break; }
        default: { name = "UNKNOWN"; break; }
      }
      return name ;
    }

}
