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
package org.apache.orc.pojowriter.writer;

import java.nio.charset.Charset;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

public class StringWriter implements Writer {
  private static final Charset UTF8 = Charset.forName("UTF-8");

  @Override
  public void write(VectorizedRowBatch batch, int colIndex, int row,
      Object value) {
    write(batch, colIndex, row, value, null);
  }

  public void write(VectorizedRowBatch batch, int colIndex, int row,
      Object value, Charset charset) {
    if (value != null) {
      BytesColumnVector x = (BytesColumnVector) batch.cols[colIndex];
      byte[] stringBytes = ((String) value)
          .getBytes((charset != null) ? charset : UTF8);
      x.setRef(row, stringBytes, 0, stringBytes.length);
    }
  }
}
