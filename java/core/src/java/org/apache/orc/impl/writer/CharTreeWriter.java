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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Under the covers, char is written to ORC the same way as string.
 */
public class CharTreeWriter extends StringBaseTreeWriter {
  private final int itemLength;
  private final byte[] padding;

  CharTreeWriter(int columnId,
                 TypeDescription schema,
                 WriterContext writer,
                 boolean nullable) throws IOException {
    super(columnId, schema, writer, nullable);
    itemLength = schema.getMaxLength();
    padding = new byte[itemLength];
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    BytesColumnVector vec = (BytesColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        byte[] ptr;
        int ptrOffset;
        if (vec.length[0] >= itemLength) {
          ptr = vec.vector[0];
          ptrOffset = vec.start[0];
        } else {
          ptr = padding;
          ptrOffset = 0;
          System.arraycopy(vec.vector[0], vec.start[0], ptr, 0,
              vec.length[0]);
          Arrays.fill(ptr, vec.length[0], itemLength, (byte) ' ');
        }
        if (useDictionaryEncoding) {
          int id = dictionary.add(ptr, ptrOffset, itemLength);
          for(int i=0; i < length; ++i) {
            rows.add(id);
          }
        } else {
          for(int i=0; i < length; ++i) {
            directStreamOutput.write(ptr, ptrOffset, itemLength);
            lengthOutput.write(itemLength);
          }
        }
        indexStatistics.updateString(ptr, ptrOffset, itemLength, length);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            // translate from UTF-8 to the default charset
            bloomFilter.addString(new String(vec.vector[0], vec.start[0],
                vec.length[0], StandardCharsets.UTF_8));
          }
          bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
        }
      }
    } else {
      for(int i=0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          byte[] ptr;
          int ptrOffset;
          if (vec.length[offset + i] >= itemLength) {
            ptr = vec.vector[offset + i];
            ptrOffset = vec.start[offset + i];
          } else {
            // it is the wrong length, so copy it
            ptr = padding;
            ptrOffset = 0;
            System.arraycopy(vec.vector[offset + i], vec.start[offset + i],
                ptr, 0, vec.length[offset + i]);
            Arrays.fill(ptr, vec.length[offset + i], itemLength, (byte) ' ');
          }
          if (useDictionaryEncoding) {
            rows.add(dictionary.add(ptr, ptrOffset, itemLength));
          } else {
            directStreamOutput.write(ptr, ptrOffset, itemLength);
            lengthOutput.write(itemLength);
          }
          indexStatistics.updateString(ptr, ptrOffset, itemLength, 1);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              // translate from UTF-8 to the default charset
              bloomFilter.addString(new String(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i],
                  StandardCharsets.UTF_8));
            }
            bloomFilterUtf8.addBytes(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]);
          }
        }
      }
    }
  }
}
