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

package org.apache.orc.bench;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

public class Utilities {

  public static TypeDescription loadSchema(String name) throws IOException {
    InputStream in = Utilities.class.getClassLoader().getResourceAsStream(name);
    byte[] buffer= new byte[1 * 1024];
    int len = in.read(buffer);
    StringBuilder string = new StringBuilder();
    while (len > 0) {
      for(int i=0; i < len; ++i) {
        // strip out
        if (buffer[i] != '\n' && buffer[i] != ' ') {
          string.append((char) buffer[i]);
        }
      }
      len = in.read(buffer);
    }
    return TypeDescription.fromString(string.toString());
  }

  public static org.apache.orc.CompressionKind getCodec(CompressionKind compression) {
    switch (compression) {
      case NONE:
        return org.apache.orc.CompressionKind.NONE;
      case ZLIB:
        return org.apache.orc.CompressionKind.ZLIB;
      case SNAPPY:
        return org.apache.orc.CompressionKind.SNAPPY;
      default:
        throw new IllegalArgumentException("Unknown compression " + compression);
    }
  }

  public static Iterable<String> sliceArray(final String[] array,
                                            final int start) {
    return new Iterable<String>() {
      String[] values = array;
      int posn = start;

      @Override
      public Iterator<String> iterator() {
        return new Iterator<String>() {
          @Override
          public boolean hasNext() {
            return posn < values.length;
          }

          @Override
          public String next() {
            if (posn >= values.length) {
              throw new NoSuchElementException("Index off end of array." +
                  " index = " + posn + " length = " + values.length);
            } else {
              return values[posn++];
            }
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("No remove");
          }
        };
      }
    };
  }

  public static Properties convertSchemaToHiveConfig(TypeDescription schema) {
    Properties result = new Properties();
    if (schema.getCategory() != TypeDescription.Category.STRUCT) {
      throw new IllegalArgumentException("Hive requires struct root types" +
          " instead of " + schema);
    }
    StringBuilder columns = new StringBuilder();
    StringBuilder types = new StringBuilder();
    List<String> columnNames = schema.getFieldNames();
    List<TypeDescription> columnTypes = schema.getChildren();
    for(int c=0; c < columnNames.size(); ++c) {
      if (c != 0) {
        columns.append(",");
        types.append(",");
      }
      columns.append(columnNames.get(c));
      types.append(columnTypes.get(c));
    }
    result.setProperty(serdeConstants.LIST_COLUMNS, columns.toString());
    result.setProperty(serdeConstants.LIST_COLUMN_TYPES, types.toString());
    return result;
  }

  public static Path getVariant(Path root,
                                String data,
                                String format,
                                String compress) {
    return new Path(root, "generated/" + data + "/" + format + "." + compress);
  }
}
