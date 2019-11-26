/*
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

package org.apache.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.SchemaEvolution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This is the description of the types in an ORC file.
 */
public class TypeDescription
    implements Comparable<TypeDescription>, Serializable, Cloneable {
  private static final int MAX_PRECISION = 38;
  private static final int MAX_SCALE = 38;
  private static final int DEFAULT_PRECISION = 38;
  private static final int DEFAULT_SCALE = 10;
  public static final int MAX_DECIMAL64_PRECISION = 18;
  public static final long MAX_DECIMAL64 = 999_999_999_999_999_999L;
  public static final long MIN_DECIMAL64 = -MAX_DECIMAL64;
  private static final int DEFAULT_LENGTH = 256;
  static final Pattern UNQUOTED_NAMES = Pattern.compile("^[a-zA-Z0-9_]+$");

  @Override
  public int compareTo(TypeDescription other) {
    if (this == other) {
      return 0;
    } else if (other == null) {
      return -1;
    } else {
      int result = category.compareTo(other.category);
      if (result == 0) {
        switch (category) {
          case CHAR:
          case VARCHAR:
            return maxLength - other.maxLength;
          case DECIMAL:
            if (precision != other.precision) {
              return precision - other.precision;
            }
            return scale - other.scale;
          case UNION:
          case LIST:
          case MAP:
            if (children.size() != other.children.size()) {
              return children.size() - other.children.size();
            }
            for(int c=0; result == 0 && c < children.size(); ++c) {
              result = children.get(c).compareTo(other.children.get(c));
            }
            break;
          case STRUCT:
            if (children.size() != other.children.size()) {
              return children.size() - other.children.size();
            }
            for(int c=0; result == 0 && c < children.size(); ++c) {
              result = fieldNames.get(c).compareTo(other.fieldNames.get(c));
              if (result == 0) {
                result = children.get(c).compareTo(other.children.get(c));
              }
            }
            break;
          default:
            // PASS
        }
      }
      return result;
    }
  }

  public enum Category {
    BOOLEAN("boolean", true),
    BYTE("tinyint", true),
    SHORT("smallint", true),
    INT("int", true),
    LONG("bigint", true),
    FLOAT("float", true),
    DOUBLE("double", true),
    STRING("string", true),
    DATE("date", true),
    TIMESTAMP("timestamp", true),
    BINARY("binary", true),
    DECIMAL("decimal", true),
    VARCHAR("varchar", true),
    CHAR("char", true),
    LIST("array", false),
    MAP("map", false),
    STRUCT("struct", false),
    UNION("uniontype", false);

    Category(String name, boolean isPrimitive) {
      this.name = name;
      this.isPrimitive = isPrimitive;
    }

    final boolean isPrimitive;
    final String name;

    public boolean isPrimitive() {
      return isPrimitive;
    }

    public String getName() {
      return name;
    }
  }

  public static TypeDescription createBoolean() {
    return new TypeDescription(Category.BOOLEAN);
  }

  public static TypeDescription createByte() {
    return new TypeDescription(Category.BYTE);
  }

  public static TypeDescription createShort() {
    return new TypeDescription(Category.SHORT);
  }

  public static TypeDescription createInt() {
    return new TypeDescription(Category.INT);
  }

  public static TypeDescription createLong() {
    return new TypeDescription(Category.LONG);
  }

  public static TypeDescription createFloat() {
    return new TypeDescription(Category.FLOAT);
  }

  public static TypeDescription createDouble() {
    return new TypeDescription(Category.DOUBLE);
  }

  public static TypeDescription createString() {
    return new TypeDescription(Category.STRING);
  }

  public static TypeDescription createDate() {
    return new TypeDescription(Category.DATE);
  }

  public static TypeDescription createTimestamp() {
    return new TypeDescription(Category.TIMESTAMP);
  }

  public static TypeDescription createBinary() {
    return new TypeDescription(Category.BINARY);
  }

  public static TypeDescription createDecimal() {
    return new TypeDescription(Category.DECIMAL);
  }

  static class StringPosition {
    final String value;
    int position;
    final int length;

    StringPosition(String value) {
      this.value = value;
      position = 0;
      length = value.length();
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      buffer.append('\'');
      buffer.append(value.substring(0, position));
      buffer.append('^');
      buffer.append(value.substring(position));
      buffer.append('\'');
      return buffer.toString();
    }
  }

  static Category parseCategory(StringPosition source) {
    int start = source.position;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (!Character.isLetter(ch)) {
        break;
      }
      source.position += 1;
    }
    if (source.position != start) {
      String word = source.value.substring(start, source.position).toLowerCase();
      for (Category cat : Category.values()) {
        if (cat.getName().equals(word)) {
          return cat;
        }
      }
    }
    throw new IllegalArgumentException("Can't parse category at " + source);
  }

  static int parseInt(StringPosition source) {
    int start = source.position;
    int result = 0;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (!Character.isDigit(ch)) {
        break;
      }
      result = result * 10 + (ch - '0');
      source.position += 1;
    }
    if (source.position == start) {
      throw new IllegalArgumentException("Missing integer at " + source);
    }
    return result;
  }

  static String parseName(StringPosition source) {
    if (source.position == source.length) {
      throw new IllegalArgumentException("Missing name at " + source);
    }
    final int start = source.position;
    if (source.value.charAt(source.position) == '`') {
      source.position += 1;
      StringBuilder buffer = new StringBuilder();
      boolean closed = false;
      while (source.position < source.length) {
        char ch = source.value.charAt(source.position);
        source.position += 1;
        if (ch == '`') {
          if (source.position < source.length &&
              source.value.charAt(source.position) == '`') {
            source.position += 1;
            buffer.append('`');
          } else {
            closed = true;
            break;
          }
        } else {
          buffer.append(ch);
        }
      }
      if (!closed) {
        source.position = start;
        throw new IllegalArgumentException("Unmatched quote at " + source);
      } else if (buffer.length() == 0) {
        throw new IllegalArgumentException("Empty quoted field name at " + source);
      }
      return buffer.toString();
    } else {
      while (source.position < source.length) {
        char ch = source.value.charAt(source.position);
        if (!Character.isLetterOrDigit(ch) && ch != '_') {
          break;
        }
        source.position += 1;
      }
      if (source.position == start) {
        throw new IllegalArgumentException("Missing name at " + source);
      }
      return source.value.substring(start, source.position);
    }
  }

  static void requireChar(StringPosition source, char required) {
    if (source.position >= source.length ||
        source.value.charAt(source.position) != required) {
      throw new IllegalArgumentException("Missing required char '" +
          required + "' at " + source);
    }
    source.position += 1;
  }

  static boolean consumeChar(StringPosition source, char ch) {
    boolean result = source.position < source.length &&
        source.value.charAt(source.position) == ch;
    if (result) {
      source.position += 1;
    }
    return result;
  }

  static void parseUnion(TypeDescription type, StringPosition source) {
    requireChar(source, '<');
    do {
      type.addUnionChild(parseType(source));
    } while (consumeChar(source, ','));
    requireChar(source, '>');
  }

  static void parseStruct(TypeDescription type, StringPosition source) {
    requireChar(source, '<');
    boolean needComma = false;
    while (!consumeChar(source, '>')) {
      if (needComma) {
        requireChar(source, ',');
      } else {
        needComma = true;
      }
      String fieldName = parseName(source);
      requireChar(source, ':');
      type.addField(fieldName, parseType(source));
    }
  }

  static TypeDescription parseType(StringPosition source) {
    TypeDescription result = new TypeDescription(parseCategory(source));
    switch (result.getCategory()) {
      case BINARY:
      case BOOLEAN:
      case BYTE:
      case DATE:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case STRING:
      case TIMESTAMP:
        break;
      case CHAR:
      case VARCHAR:
        requireChar(source, '(');
        result.withMaxLength(parseInt(source));
        requireChar(source, ')');
        break;
      case DECIMAL: {
        requireChar(source, '(');
        int precision = parseInt(source);
        requireChar(source, ',');
        result.withScale(parseInt(source));
        result.withPrecision(precision);
        requireChar(source, ')');
        break;
      }
      case LIST: {
        requireChar(source, '<');
        TypeDescription child = parseType(source);
        result.children.add(child);
        child.parent = result;
        requireChar(source, '>');
        break;
      }
      case MAP: {
        requireChar(source, '<');
        TypeDescription keyType = parseType(source);
        result.children.add(keyType);
        keyType.parent = result;
        requireChar(source, ',');
        TypeDescription valueType = parseType(source);
        result.children.add(valueType);
        valueType.parent = result;
        requireChar(source, '>');
        break;
      }
      case UNION:
        parseUnion(result, source);
        break;
      case STRUCT:
        parseStruct(result, source);
        break;
      default:
        throw new IllegalArgumentException("Unknown type " +
            result.getCategory() + " at " + source);
    }
    return result;
  }

  /**
   * Parse TypeDescription from the Hive type names. This is the inverse
   * of TypeDescription.toString()
   * @param typeName the name of the type
   * @return a new TypeDescription or null if typeName was null
   * @throws IllegalArgumentException if the string is badly formed
   */
  public static TypeDescription fromString(String typeName) {
    if (typeName == null) {
      return null;
    }
    StringPosition source = new StringPosition(typeName);
    TypeDescription result = parseType(source);
    if (source.position != source.length) {
      throw new IllegalArgumentException("Extra characters at " + source);
    }
    return result;
  }

  /**
   * For decimal types, set the precision.
   * @param precision the new precision
   * @return this
   */
  public TypeDescription withPrecision(int precision) {
    if (category != Category.DECIMAL) {
      throw new IllegalArgumentException("precision is only allowed on decimal"+
         " and not " + category.name);
    } else if (precision < 1 || precision > MAX_PRECISION || scale > precision){
      throw new IllegalArgumentException("precision " + precision +
          " is out of range 1 .. " + scale);
    }
    this.precision = precision;
    return this;
  }

  /**
   * For decimal types, set the scale.
   * @param scale the new scale
   * @return this
   */
  public TypeDescription withScale(int scale) {
    if (category != Category.DECIMAL) {
      throw new IllegalArgumentException("scale is only allowed on decimal"+
          " and not " + category.name);
    } else if (scale < 0 || scale > MAX_SCALE || scale > precision) {
      throw new IllegalArgumentException("scale is out of range at " + scale);
    }
    this.scale = scale;
    return this;
  }

  public static TypeDescription createVarchar() {
    return new TypeDescription(Category.VARCHAR);
  }

  public static TypeDescription createChar() {
    return new TypeDescription(Category.CHAR);
  }

  /**
   * Set the maximum length for char and varchar types.
   * @param maxLength the maximum value
   * @return this
   */
  public TypeDescription withMaxLength(int maxLength) {
    if (category != Category.VARCHAR && category != Category.CHAR) {
      throw new IllegalArgumentException("maxLength is only allowed on char" +
                   " and varchar and not " + category.name);
    }
    this.maxLength = maxLength;
    return this;
  }

  public static TypeDescription createList(TypeDescription childType) {
    TypeDescription result = new TypeDescription(Category.LIST);
    result.children.add(childType);
    childType.parent = result;
    return result;
  }

  public static TypeDescription createMap(TypeDescription keyType,
                                          TypeDescription valueType) {
    TypeDescription result = new TypeDescription(Category.MAP);
    result.children.add(keyType);
    result.children.add(valueType);
    keyType.parent = result;
    valueType.parent = result;
    return result;
  }

  public static TypeDescription createUnion() {
    return new TypeDescription(Category.UNION);
  }

  public static TypeDescription createStruct() {
    return new TypeDescription(Category.STRUCT);
  }

  /**
   * Add a child to a union type.
   * @param child a new child type to add
   * @return the union type.
   */
  public TypeDescription addUnionChild(TypeDescription child) {
    if (category != Category.UNION) {
      throw new IllegalArgumentException("Can only add types to union type" +
          " and not " + category);
    }
    children.add(child);
    child.parent = this;
    return this;
  }

  /**
   * Add a field to a struct type as it is built.
   * @param field the field name
   * @param fieldType the type of the field
   * @return the struct type
   */
  public TypeDescription addField(String field, TypeDescription fieldType) {
    if (category != Category.STRUCT) {
      throw new IllegalArgumentException("Can only add fields to struct type" +
          " and not " + category);
    }
    fieldNames.add(field);
    children.add(fieldType);
    fieldType.parent = this;
    return this;
  }

  /**
   * Get the id for this type.
   * The first call will cause all of the the ids in tree to be assigned, so
   * it should not be called before the type is completely built.
   * @return the sequential id
   */
  public int getId() {
    // if the id hasn't been assigned, assign all of the ids from the root
    if (id == -1) {
      TypeDescription root = this;
      while (root.parent != null) {
        root = root.parent;
      }
      root.assignIds(0);
    }
    return id;
  }

  public TypeDescription clone() {
    TypeDescription result = new TypeDescription(category);
    result.maxLength = maxLength;
    result.precision = precision;
    result.scale = scale;
    if (fieldNames != null) {
      result.fieldNames.addAll(fieldNames);
    }
    if (children != null) {
      for(TypeDescription child: children) {
        TypeDescription clone = child.clone();
        clone.parent = result;
        result.children.add(clone);
      }
    }
    return result;
  }

  @Override
  public int hashCode() {
    long result = category.ordinal() * 4241 + maxLength + precision * 13 + scale;
    if (children != null) {
      for(TypeDescription child: children) {
        result = result * 6959 + child.hashCode();
      }
    }
    return (int) result;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof TypeDescription)) {
      return false;
    }
    if (other == this) {
      return true;
    }
    TypeDescription castOther = (TypeDescription) other;
    if (category != castOther.category ||
        maxLength != castOther.maxLength ||
        scale != castOther.scale ||
        precision != castOther.precision) {
      return false;
    }
    if (children != null) {
      if (children.size() != castOther.children.size()) {
        return false;
      }
      for (int i = 0; i < children.size(); ++i) {
        if (!children.get(i).equals(castOther.children.get(i))) {
          return false;
        }
      }
    }
    if (category == Category.STRUCT) {
      for(int i=0; i < fieldNames.size(); ++i) {
        if (!fieldNames.get(i).equals(castOther.fieldNames.get(i))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Get the maximum id assigned to this type or its children.
   * The first call will cause all of the the ids in tree to be assigned, so
   * it should not be called before the type is completely built.
   * @return the maximum id assigned under this type
   */
  public int getMaximumId() {
    // if the id hasn't been assigned, assign all of the ids from the root
    if (maxId == -1) {
      TypeDescription root = this;
      while (root.parent != null) {
        root = root.parent;
      }
      root.assignIds(0);
    }
    return maxId;
  }

  private ColumnVector createColumn(RowBatchVersion version, int maxSize) {
    switch (category) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new LongColumnVector(maxSize);
      case DATE:
        return new DateColumnVector(maxSize);
      case TIMESTAMP:
        return new TimestampColumnVector(maxSize);
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnVector(maxSize);
      case DECIMAL:
        if (version == RowBatchVersion.ORIGINAL ||
            precision > MAX_DECIMAL64_PRECISION) {
          return new DecimalColumnVector(maxSize, precision, scale);
        } else {
          return new Decimal64ColumnVector(maxSize, precision, scale);
        }
      case STRING:
      case BINARY:
      case CHAR:
      case VARCHAR:
        return new BytesColumnVector(maxSize);
      case STRUCT: {
        ColumnVector[] fieldVector = new ColumnVector[children.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = children.get(i).createColumn(version, maxSize);
        }
        return new StructColumnVector(maxSize,
                fieldVector);
      }
      case UNION: {
        ColumnVector[] fieldVector = new ColumnVector[children.size()];
        for(int i=0; i < fieldVector.length; ++i) {
          fieldVector[i] = children.get(i).createColumn(version, maxSize);
        }
        return new UnionColumnVector(maxSize,
            fieldVector);
      }
      case LIST:
        return new ListColumnVector(maxSize,
            children.get(0).createColumn(version, maxSize));
      case MAP:
        return new MapColumnVector(maxSize,
            children.get(0).createColumn(version, maxSize),
            children.get(1).createColumn(version, maxSize));
      default:
        throw new IllegalArgumentException("Unknown type " + category);
    }
  }

  /**
   * Specify the version of the VectorizedRowBatch that the user desires.
   */
  public enum RowBatchVersion {
    ORIGINAL,
    USE_DECIMAL64;
  }

  public VectorizedRowBatch createRowBatch(RowBatchVersion version, int size) {
    VectorizedRowBatch result;
    if (category == Category.STRUCT) {
      result = new VectorizedRowBatch(children.size(), size);
      for(int i=0; i < result.cols.length; ++i) {
        result.cols[i] = children.get(i).createColumn(version, size);
      }
    } else {
      result = new VectorizedRowBatch(1, size);
      result.cols[0] = createColumn(version, size);
    }
    result.reset();
    return result;
  }

  /**
   * Create a VectorizedRowBatch that uses Decimal64ColumnVector for
   * short (p &le; 18) decimals.
   * @return a new VectorizedRowBatch
   */
  public VectorizedRowBatch createRowBatchV2() {
    return createRowBatch(RowBatchVersion.USE_DECIMAL64,
        VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Create a VectorizedRowBatch with the original ColumnVector types
   * @param maxSize the maximum size of the batch
   * @return a new VectorizedRowBatch
   */
  public VectorizedRowBatch createRowBatch(int maxSize) {
    return createRowBatch(RowBatchVersion.ORIGINAL, maxSize);
  }

  /**
   * Create a VectorizedRowBatch with the original ColumnVector types
   * @return a new VectorizedRowBatch
   */
  public VectorizedRowBatch createRowBatch() {
    return createRowBatch(RowBatchVersion.ORIGINAL,
        VectorizedRowBatch.DEFAULT_SIZE);
  }

  /**
   * Get the kind of this type.
   * @return get the category for this type.
   */
  public Category getCategory() {
    return category;
  }

  /**
   * Get the maximum length of the type. Only used for char and varchar types.
   * @return the maximum length of the string type
   */
  public int getMaxLength() {
    return maxLength;
  }

  /**
   * Get the precision of the decimal type.
   * @return the number of digits for the precision.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Get the scale of the decimal type.
   * @return the number of digits for the scale.
   */
  public int getScale() {
    return scale;
  }

  /**
   * For struct types, get the list of field names.
   * @return the list of field names.
   */
  public List<String> getFieldNames() {
    return Collections.unmodifiableList(fieldNames);
  }

  /**
   * Get the subtypes of this type.
   * @return the list of children types
   */
  public List<TypeDescription> getChildren() {
    return children == null ? null : Collections.unmodifiableList(children);
  }

  /**
   * Assign ids to all of the nodes under this one.
   * @param startId the lowest id to assign
   * @return the next available id
   */
  private int assignIds(int startId) {
    id = startId++;
    if (children != null) {
      for (TypeDescription child : children) {
        startId = child.assignIds(startId);
      }
    }
    maxId = startId - 1;
    return startId;
  }

  public TypeDescription(Category category) {
    this.category = category;
    if (category.isPrimitive) {
      children = null;
    } else {
      children = new ArrayList<>();
    }
    if (category == Category.STRUCT) {
      fieldNames = new ArrayList<>();
    } else {
      fieldNames = null;
    }
  }

  private int id = -1;
  private int maxId = -1;
  private TypeDescription parent;
  private final Category category;
  private final List<TypeDescription> children;
  private final List<String> fieldNames;
  private int maxLength = DEFAULT_LENGTH;
  private int precision = DEFAULT_PRECISION;
  private int scale = DEFAULT_SCALE;

  static void printFieldName(StringBuilder buffer, String name) {
    if (UNQUOTED_NAMES.matcher(name).matches()) {
      buffer.append(name);
    } else {
      buffer.append('`');
      buffer.append(name.replace("`", "``"));
      buffer.append('`');
    }
  }

  public void printToBuffer(StringBuilder buffer) {
    buffer.append(category.name);
    switch (category) {
      case DECIMAL:
        buffer.append('(');
        buffer.append(precision);
        buffer.append(',');
        buffer.append(scale);
        buffer.append(')');
        break;
      case CHAR:
      case VARCHAR:
        buffer.append('(');
        buffer.append(maxLength);
        buffer.append(')');
        break;
      case LIST:
      case MAP:
      case UNION:
        buffer.append('<');
        for(int i=0; i < children.size(); ++i) {
          if (i != 0) {
            buffer.append(',');
          }
          children.get(i).printToBuffer(buffer);
        }
        buffer.append('>');
        break;
      case STRUCT:
        buffer.append('<');
        for(int i=0; i < children.size(); ++i) {
          if (i != 0) {
            buffer.append(',');
          }
          printFieldName(buffer, fieldNames.get(i));
          buffer.append(':');
          children.get(i).printToBuffer(buffer);
        }
        buffer.append('>');
        break;
      default:
        break;
    }
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder();
    printToBuffer(buffer);
    return buffer.toString();
  }

  private void printJsonToBuffer(String prefix, StringBuilder buffer,
                                 int indent) {
    for(int i=0; i < indent; ++i) {
      buffer.append(' ');
    }
    buffer.append(prefix);
    buffer.append("{\"category\": \"");
    buffer.append(category.name);
    buffer.append("\", \"id\": ");
    buffer.append(getId());
    buffer.append(", \"max\": ");
    buffer.append(maxId);
    switch (category) {
      case DECIMAL:
        buffer.append(", \"precision\": ");
        buffer.append(precision);
        buffer.append(", \"scale\": ");
        buffer.append(scale);
        break;
      case CHAR:
      case VARCHAR:
        buffer.append(", \"length\": ");
        buffer.append(maxLength);
        break;
      case LIST:
      case MAP:
      case UNION:
        buffer.append(", \"children\": [");
        for(int i=0; i < children.size(); ++i) {
          buffer.append('\n');
          children.get(i).printJsonToBuffer("", buffer, indent + 2);
          if (i != children.size() - 1) {
            buffer.append(',');
          }
        }
        buffer.append("]");
        break;
      case STRUCT:
        buffer.append(", \"fields\": [");
        for(int i=0; i < children.size(); ++i) {
          buffer.append('\n');
          children.get(i).printJsonToBuffer("\"" + fieldNames.get(i) + "\": ",
              buffer, indent + 2);
          if (i != children.size() - 1) {
            buffer.append(',');
          }
        }
        buffer.append(']');
        break;
      default:
        break;
    }
    buffer.append('}');
  }

  public String toJson() {
    StringBuilder buffer = new StringBuilder();
    printJsonToBuffer("", buffer, 0);
    return buffer.toString();
  }

  /**
   * Locate a subtype by its id.
   * @param goal the column id to look for
   * @return the subtype
   */
  public TypeDescription findSubtype(int goal) {
    // call getId method to make sure the ids are assigned
    int id = getId();
    if (goal < id || goal > maxId) {
      throw new IllegalArgumentException("Unknown type id " + id + " in " +
          toJson());
    }
    if (goal == id) {
      return this;
    } else {
      TypeDescription prev = null;
      for(TypeDescription next: children) {
        if (next.id > goal) {
          return prev.findSubtype(goal);
        }
        prev = next;
      }
      return prev.findSubtype(goal);
    }
  }

  /**
   * Split a compound name into parts separated by '.'.
   * @param source the string to parse into simple names
   * @return a list of simple names from the source
   */
  private static List<String> splitName(StringPosition source) {
    List<String> result = new ArrayList<>();
    do {
      result.add(parseName(source));
    } while (consumeChar(source, '.'));
    return result;
  }

  private static final Pattern INTEGER_PATTERN = Pattern.compile("^[0-9]+$");

  private TypeDescription findSubtype(StringPosition source) {
    List<String> names = splitName(source);
    if (names.size() == 1 && INTEGER_PATTERN.matcher(names.get(0)).matches()) {
      return findSubtype(Integer.parseInt(names.get(0)));
    }
    TypeDescription current = SchemaEvolution.checkAcidSchema(this)
        ? SchemaEvolution.getBaseRow(this) : this;
    while (names.size() > 0) {
      String first = names.remove(0);
      switch (current.category) {
        case STRUCT: {
          int posn = current.fieldNames.indexOf(first);
          if (posn == -1) {
            throw new IllegalArgumentException("Field " + first +
                " not found in " + current.toString());
          }
          current = current.children.get(posn);
          break;
        }
        case LIST:
          if (first.equals("_elem")) {
            current = current.getChildren().get(0);
          } else {
            throw new IllegalArgumentException("Field " + first +
                "not found in " + current.toString());
          }
          break;
        case MAP:
          if (first.equals("_key")) {
            current = current.getChildren().get(0);
          } else if (first.equals("_value")) {
            current = current.getChildren().get(1);
          } else {
            throw new IllegalArgumentException("Field " + first +
                "not found in " + current.toString());
          }
          break;
        case UNION: {
          try {
            int posn = Integer.parseInt(first);
            if (posn < 0 || posn >= current.getChildren().size()) {
              throw new NumberFormatException("off end of union");
            }
            current = current.getChildren().get(posn);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Field " + first +
                "not found in " + current.toString(), e);
          }
          break;
        }
        default:
          throw new IllegalArgumentException("Field " + first +
              "not found in " + current.toString());
      }
    }
    return current;
  }

  /**
   * Find a subtype of this schema by name.
   * If the name is a simple integer, it will be used as a column number.
   * Otherwise, this routine will recursively search for the name.
   * <ul>
   *   <li>Struct fields are selected by name.</li>
   *   <li>List children are selected by "_elem".</li>
   *   <li>Map children are selected by "_key" or "_value".</li>
   *   <li>Union children are selected by number starting at 0.</li>
   * </ul>
   * Names are separated by '.'.
   * @param columnName the name to search for
   * @return the subtype
   */
  public TypeDescription findSubtype(String columnName) {
    StringPosition source = new StringPosition(columnName);
    TypeDescription result = findSubtype(source);
    if (source.position != source.length) {
      throw new IllegalArgumentException("Remaining text in parsing field name "
          + source);
    }
    return result;
  }

  /**
   * Find a list of subtypes from a string, including the empty list.
   *
   * Each column name is separated by ','.
   * @param columnNameList the list of column names
   * @return the list of subtypes that correspond to the column names
   */
  public List<TypeDescription> findSubtypes(String columnNameList) {
    StringPosition source = new StringPosition(columnNameList);
    List<TypeDescription> result = new ArrayList<>();
    boolean needComma = false;
    while (source.position != source.length) {
      if (needComma) {
        if (!consumeChar(source, ',')) {
          throw new IllegalArgumentException("Comma expected in list of column"
              + " names at " + source);
        }
      } else {
        needComma = true;
      }
      result.add(findSubtype(source));
    }
    return result;
  }
}
