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

package org.apache.orc.tools.json;

import org.apache.orc.TypeDescription;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

/**
 * Model structs.
 */
class StructType extends HiveType {
  final Map<String, HiveType> fields = new TreeMap<String, HiveType>();

  StructType() {
    super(Kind.STRUCT);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder("struct<");
    boolean first = true;
    for (Map.Entry<String, HiveType> field : fields.entrySet()) {
      if (!first) {
        buf.append(',');
      } else {
        first = false;
      }
      buf.append(field.getKey());
      buf.append(':');
      buf.append(field.getValue().toString());
    }
    buf.append(">");
    return buf.toString();
  }

  public StructType addField(String name, HiveType fieldType) {
    fields.put(name, fieldType);
    return this;
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other) && fields.equals(((StructType) other).fields);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode() * 3;
    for (Map.Entry<String, HiveType> pair : fields.entrySet()) {
      result += pair.getKey().hashCode() * 17 + pair.getValue().hashCode();
    }
    return result;
  }

  @Override
  public boolean subsumes(HiveType other) {
    return other.kind == Kind.NULL || other.kind == Kind.STRUCT;
  }

  @Override
  public void merge(HiveType other) {
    if (other.getClass() == StructType.class) {
      StructType otherStruct = (StructType) other;
      for (Map.Entry<String, HiveType> pair : otherStruct.fields.entrySet()) {
        HiveType ourField = fields.get(pair.getKey());
        if (ourField == null) {
          fields.put(pair.getKey(), pair.getValue());
        } else if (ourField.subsumes(pair.getValue())) {
          ourField.merge(pair.getValue());
        } else if (pair.getValue().subsumes(ourField)) {
          pair.getValue().merge(ourField);
          fields.put(pair.getKey(), pair.getValue());
        } else {
          fields.put(pair.getKey(), new UnionType(ourField, pair.getValue()));
        }
      }
    }
  }

  public void printFlat(PrintStream out, String prefix) {
    prefix = prefix + ".";
    for (Map.Entry<String, HiveType> field : fields.entrySet()) {
      field.getValue().printFlat(out, prefix + field.getKey());
    }
  }

  @Override
  public TypeDescription getSchema() {
    TypeDescription result = TypeDescription.createStruct();
    for (Map.Entry<String, HiveType> child: fields.entrySet()) {
      result.addField(child.getKey(), child.getValue().getSchema());
    }
    return result;
  }
}
