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

import javax.xml.bind.DatatypeConverter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Masking strategy that masks String, Varchar, Char and Binary types
 * as SHA 256 hash.
 * <p>
 * <b>For String type:</b><br/>
 * All string type of any length will be converted to 64 length SHA256 hash.<br/><br/>
 * <p>
 * <b>For Varchar type:</b><br/>
 * For Varchar type, max-length property will be honored i.e.
 * if the length is less than max-length then the SHA256 hash will be truncated
 * to max-length. If max-length is greater than 64 then the output is the sha256
 * length, which is 64.<br/><br/>
 * <p>
 * <b>For Char type:</b><br/>
 * For Char type, the length of mask will always be equal to specified max-length.
 * If the given length (max-length) is less than SHA256 hash length (64)
 * the mask will be truncated.
 * If the given length (max-length) is greater than SHA256 hash length (64)
 * then the mask will be padded by blank spaces.<br/><br/>
 * <p>
 * <b>For Binary type:</b><br/>
 * All Binary type of any length will be converted to 64 length SHA256 hash.<br/>
 */
public class SHA256MaskFactory extends MaskFactory {

  final MessageDigest md;

  public SHA256MaskFactory(final String... params) {
    super();
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Mask a string by finding the character category of each character
   * and replacing it with the matching literal.
   *
   * @param source the source column vector
   * @param row    the value index
   * @param target the target column vector
   * @param schema schema
   */
  void maskString(final BytesColumnVector source, final int row,
      final BytesColumnVector target, final TypeDescription schema) {
    final ByteBuffer sourceBytes = ByteBuffer
        .wrap(source.vector[row], source.start[row], source.length[row]);

    // take SHA-256 Hash and convert to HEX
    byte[] hash = DatatypeConverter
        .printHexBinary(md.digest(sourceBytes.array()))
        .getBytes(StandardCharsets.UTF_8);
    int targetLength = hash.length;

    /* For type varchar */
    if (schema.getCategory() == TypeDescription.Category.VARCHAR) {

      /* truncate the hash if max length for varchar is less than hash length
       * on the other hand if if the max length is more than hash length (64 bytes)
       * we use the hash length (64 bytes) always.
       */
      if (schema.getMaxLength() < hash.length) {
        targetLength = schema.getMaxLength();
      }

    }

    /* For type char */
    if (schema.getCategory() == TypeDescription.Category.CHAR) {
      /* for char the length is always constant */
      targetLength = schema.getMaxLength();
    }

    // ensure we have enough space, if the masked data is the same size
    target.ensureValPreallocated(targetLength);
    byte[] outputBuffer = target.getValPreallocatedBytes();
    int outputOffset = target.getValPreallocatedStart();

    if (targetLength > hash.length) {

      System.arraycopy(hash, 0, outputBuffer, 0, hash.length);
      Arrays.fill(outputBuffer, hash.length, targetLength - 1, (byte) ' ');

    } else {
      System.arraycopy(hash, 0, outputBuffer, outputOffset, targetLength);
    }

    target.setValPreallocated(row, targetLength);

  }

  /**
   * Helper function to mask binary data with it's SHA-256 hash.
   *
   * @param source
   * @param row
   * @param target
   */
  void maskBinary(final BytesColumnVector source, final int row,
      final BytesColumnVector target) {

    final ByteBuffer sourceBytes = ByteBuffer
        .wrap(source.vector[row], source.start[row], source.length[row]);

    // take SHA-256 Hash and convert to HEX
    byte[] hash = DatatypeConverter
        .printHexBinary(md.digest(sourceBytes.array()))
        .getBytes(StandardCharsets.UTF_8);
    int targetLength = hash.length;

    // ensure we have enough space, if the masked data is the same size
    target.ensureValPreallocated(targetLength);
    byte[] outputBuffer = target.getValPreallocatedBytes();
    int outputOffset = target.getValPreallocatedStart();

    System.arraycopy(hash, 0, outputBuffer, outputOffset, targetLength);

    target.setValPreallocated(row, targetLength);

  }

  @Override
  protected DataMask buildBinaryMask(TypeDescription schema) {
    return new BinaryMask();
  }

  @Override
  protected DataMask buildBooleanMask(TypeDescription schema) {
    return new NullifyMask();
  }

  @Override
  protected DataMask buildLongMask(TypeDescription schema) {
    return new NullifyMask();
  }

  @Override
  protected DataMask buildDecimalMask(TypeDescription schema) {
    return new NullifyMask();
  }

  @Override
  protected DataMask buildDoubleMask(TypeDescription schema) {
    return new NullifyMask();
  }

  @Override
  protected DataMask buildStringMask(final TypeDescription schema) {
    return new StringMask(schema);
  }

  @Override
  protected DataMask buildDateMask(TypeDescription schema) {
    return new NullifyMask();
  }

  @Override
  protected DataMask buildTimestampMask(TypeDescription schema) {
    return new NullifyMask();
  }

  /**
   * Data mask for String, Varchar and Char types.
   */
  class StringMask implements DataMask {

    final TypeDescription schema;

    /* create an instance */
    public StringMask(TypeDescription schema) {
      super();
      this.schema = schema;
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
    public void maskData(final ColumnVector original, final ColumnVector masked,
        final int start, final int length) {

      final BytesColumnVector target = (BytesColumnVector) masked;
      final BytesColumnVector source = (BytesColumnVector) original;

      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;

      if (original.isRepeating) {
        target.isNull[0] = source.isNull[0];
        if (target.noNulls || !target.isNull[0]) {
          maskString(source, 0, target, schema);
        }
      } else {
        for (int r = start; r < start + length; ++r) {
          target.isNull[r] = source.isNull[r];
          if (target.noNulls || !target.isNull[r]) {
            maskString(source, r, target, schema);
          }
        }
      }

    }

  }

  /**
   * Mask for binary data
   */
  class BinaryMask implements DataMask {

    /* create an instance */
    public BinaryMask() {
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
    public void maskData(final ColumnVector original, final ColumnVector masked,
        final int start, final int length) {

      final BytesColumnVector target = (BytesColumnVector) masked;
      final BytesColumnVector source = (BytesColumnVector) original;

      target.noNulls = original.noNulls;
      target.isRepeating = original.isRepeating;

      if (original.isRepeating) {
        target.isNull[0] = source.isNull[0];
        if (target.noNulls || !target.isNull[0]) {
          maskBinary(source, 0, target);
        }
      } else {
        for (int r = start; r < start + length; ++r) {
          target.isNull[r] = source.isNull[r];
          if (target.noNulls || !target.isNull[r]) {
            maskBinary(source, r, target);
          }
        }
      }

    }

  }

}
