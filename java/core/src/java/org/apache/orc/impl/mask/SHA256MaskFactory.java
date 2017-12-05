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
package org.apache.orc.impl.mask;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.DataMask;
import org.apache.orc.TypeDescription;

public class SHA256MaskFactory extends MaskFactory {


  public SHA256MaskFactory(final String... params) {
    super();
  }

  class Stringmask implements DataMask {

    /* create an instance */
    public Stringmask() {
      super();
    }

    /**
     * Mask the given range of values
     *
     * @param original the original input data
     * @param masked   the masked output data
     * @param start    the first data element to mask
     * @param length   the number of data elements to mask
     */
    @Override
    public void maskData(final ColumnVector original, final ColumnVector masked, final int start,
        final int length) {

      final BytesColumnVector target = (BytesColumnVector) masked;
      final BytesColumnVector source = (BytesColumnVector) original;

      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;

      if (original.isRepeating) {
        target.isNull[0] = source.isNull[0];
        if (target.noNulls || !target.isNull[0]) {
          maskStringHelper(source, 0, target);
        }
      } else {
        for(int r = start; r < start + length; ++r) {
          target.isNull[r] = source.isNull[r];
          if (target.noNulls || !target.isNull[r]) {
            maskStringHelper(source, r, target);
          }
        }
      }

    }

  }

  /**
   * Mask a string by finding the character category of each character
   * and replacing it with the matching literal.
   * @param source the source column vector
   * @param row the value index
   * @param target the target column vector
   */
  void maskStringHelper(final BytesColumnVector source, final int row, final BytesColumnVector target) {

  }


  @Override
  protected DataMask buildBooleanMask(TypeDescription schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected DataMask buildLongMask(TypeDescription schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected DataMask buildDecimalMask(TypeDescription schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected DataMask buildDoubleMask(TypeDescription schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected DataMask buildStringMask(TypeDescription schema) {
    return new Stringmask();
  }

  @Override
  protected DataMask buildDateMask(TypeDescription schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected DataMask buildTimestampMask(TypeDescription schema) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected DataMask buildBinaryMask(TypeDescription schema) {
    throw new UnsupportedOperationException();
  }
}
