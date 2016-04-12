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
package org.apache.orc.mapred;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.orc.TypeDescription;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class OrcStruct implements Writable {

  private Writable[] fields;
  private final TypeDescription schema;

  public OrcStruct(TypeDescription schema) {
    this.schema = schema;
    fields = new Writable[schema.getChildren().size()];
  }

  public Writable getFieldValue(int fieldIndex) {
    return fields[fieldIndex];
  }

  public void setFieldValue(int fieldIndex, Writable value) {
    fields[fieldIndex] = value;
  }

  public int getNumFields() {
    return fields.length;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    for(int f=0; f < fields.length; ++f) {
      output.writeBoolean(fields[f] != null);
      if (fields[f] != null) {
        fields[f].write(output);
      }
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    for(int f=0; f < fields.length; ++f) {
      if (input.readBoolean()) {
        if (fields[f] == null) {
          fields[f] = createValue(schema.getChildren().get(f));
        }
        fields[f].readFields(input);
      } else {
        fields[f] = null;
      }
    }
  }

  public void setFieldValue(String fieldName, Writable value) {
    int fieldIdx = schema.getFieldNames().indexOf(fieldName);
    if (fieldIdx == -1) {
      throw new IllegalArgumentException("Field " + fieldName +
          " not found in " + schema);
    }
    fields[fieldIdx] = value;
  }

  public Writable getFieldValue(String fieldName) {
    int fieldIdx = schema.getFieldNames().indexOf(fieldName);
    if (fieldIdx == -1) {
      throw new IllegalArgumentException("Field " + fieldName +
          " not found in " + schema);
    }
    return fields[fieldIdx];
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != OrcStruct.class) {
      return false;
    } else {
      OrcStruct oth = (OrcStruct) other;
      if (fields.length != oth.fields.length) {
        return false;
      }
      for(int i=0; i < fields.length; ++i) {
        if (fields[i] == null) {
          if (oth.fields[i] != null) {
            return false;
          }
        } else {
          if (!fields[i].equals(oth.fields[i])) {
            return false;
          }
        }
      }
      return true;
    }
  }

  @Override
  public int hashCode() {
    int result = fields.length;
    for(Object field: fields) {
      if (field != null) {
        result ^= field.hashCode();
      }
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{");
    for(int i=0; i < fields.length; ++i) {
      if (i != 0) {
        buffer.append(", ");
      }
      buffer.append(fields[i]);
    }
    buffer.append("}");
    return buffer.toString();
  }

  /* Routines for stubbing into Writables */

  public static Writable createValue(TypeDescription type) {
    switch (type.getCategory()) {
      case BOOLEAN: return new BooleanWritable();
      case BYTE: return new ByteWritable();
      case SHORT: return new ShortWritable();
      case INT: return new IntWritable();
      case LONG: return new LongWritable();
      case FLOAT: return new FloatWritable();
      case DOUBLE: return new DoubleWritable();
      case BINARY: return new BytesWritable();
      case CHAR:
      case VARCHAR:
      case STRING:
        return new Text();
      case DATE:
        return new DateWritable();
      case TIMESTAMP:
        return new OrcTimestamp();
      case DECIMAL:
        return new HiveDecimalWritable();
      case STRUCT: {
        OrcStruct result = new OrcStruct(type);
        int c = 0;
        for(TypeDescription child: type.getChildren()) {
          result.setFieldValue(c++, createValue(child));
        }
        return result;
      }
      case UNION: return new OrcUnion(type);
      case LIST: return new OrcList(type);
      case MAP: return new OrcMap(type);
      default:
        throw new IllegalArgumentException("Unknown type " + type);
    }
  }
}
