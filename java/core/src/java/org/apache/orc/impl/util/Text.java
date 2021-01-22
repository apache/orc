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

package org.apache.orc.impl.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** This class stores text using standard UTF8 encoding.  It provides methods
 * to serialize, deserialize, and compare texts at byte level.  The type of
 * length is integer and is serialized using zero-compressed format.  <p>In
 * addition, it provides methods for string traversal without converting the
 * byte array to a string.  <p>Also includes utilities for
 * serializing/deserialing a string, coding/decoding a string, checking if a
 * byte array contains valid UTF8 code, calculating the length of an encoded
 * string.
 */
public class Text implements Comparable<Text> {

  private static final ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
      ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newEncoder().
             onMalformedInput(CodingErrorAction.REPORT).
             onUnmappableCharacter(CodingErrorAction.REPORT));

  private static final ThreadLocal<CharsetDecoder> DECODER_FACTORY =
      ThreadLocal.withInitial(() -> StandardCharsets.UTF_8.newDecoder().
             onMalformedInput(CodingErrorAction.REPORT).
             onUnmappableCharacter(CodingErrorAction.REPORT));

  private static final byte[] EMPTY_BYTES = new byte[0];

  private byte[] bytes = EMPTY_BYTES;
  private int length = 0;

  /**
   * Construct an empty text string.
   */
  public Text() {
  }

  /**
   * Construct from a string.
   */
  public Text(String string) {
    set(string);
  }

  /**
   * Construct from another text.
   */
  @SuppressWarnings("CopyConstructorMissesField")
  public Text(Text utf8) {
    set(utf8);
  }

  /**
   * Construct from a byte array.
   */
  public Text(byte[] utf8)  {
    set(utf8);
  }

  /**
   * Returns the raw bytes; however, only data up to {@link #getLength()} is
   * valid.
   */
  public byte[] getBytes() {
    return bytes;
  }

  /**
   * Returns the number of bytes in the byte array.
   */
  public int getLength() {
    return length;
  }

  /**
   * Set to contain the contents of a string.
   */
  public void set(String string) {
    try {
      ByteBuffer bb = encode(string, true);
      bytes = bb.array();
      length = bb.limit();
    } catch (CharacterCodingException e) {
      throw new RuntimeException("Should not have happened", e);
    }
  }

  /**
   * Set to a utf8 byte array.
   */
  public void set(byte[] utf8) {
    set(utf8, 0, utf8.length);
  }

  /**
   * Copy a text.
   */
  public void set(Text other) {
    set(other.getBytes(), 0, other.getLength());
  }

  /**
   * Set the Text to range of bytes.
   *
   * @param utf8 the data to copy from
   * @param start the first position of the new string
   * @param len the number of bytes of the new string
   */
  public void set(byte[] utf8, int start, int len) {
    ensureCapacity(len);
    System.arraycopy(utf8, start, bytes, 0, len);
    this.length = len;
  }

  /**
   * Append a range of bytes to the end of the given text.
   *
   * @param utf8 the data to copy from
   * @param start the first position to append from utf8
   * @param len the number of bytes to append
   */
  public void append(byte[] utf8, int start, int len) {
    byte[] original = bytes;
    int capacity = Math.max(length + len, length + (length >> 1));
    if (ensureCapacity(capacity)) {
      System.arraycopy(original, 0, bytes, 0, length);
    }
    System.arraycopy(utf8, start, bytes, length, len);
    length += len;
  }

  /**
   * Clear the string to empty.
   *
   * <em>Note</em>: For performance reasons, this call does not clear the
   * underlying byte array that is retrievable via {@link #getBytes()}.
   * In order to free the byte-array memory, call {@link #set(byte[])}
   * with an empty byte array (For example, <code>new byte[0]</code>).
   */
  public void clear() {
    length = 0;
  }

  /**
   * Sets the capacity of this Text object to <em>at least</em>
   * <code>capacity</code> bytes. If the current buffer is longer, then the
   * capacity and existing content of the buffer are unchanged. If
   * <code>capacity</code> is larger than the current capacity, the Text
   * object's capacity is increased to match and any existing data is lost.
   *
   * @param capacity the number of bytes we need
   */
  private boolean ensureCapacity(final int capacity) {
    if (bytes.length < capacity) {
      bytes = new byte[capacity];
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    try {
      return decode(bytes, 0, length);
    } catch (CharacterCodingException e) { 
      throw new RuntimeException("Should not have happened", e);
    }
  }

  /**
   * Returns true iff <code>o</code> is a Text with the same length and same
   * contents.
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof Text) {
      return compareTo((Text) other) == 0;
    }
    return false;
  }

  /**
   * Returns true iff <code>o</code> is a Text with the same length and same
   * contents.
   */
  @Override
  public int compareTo(Text other) {
    return FastByteComparisons.compareTo(bytes, 0, length,
        other.bytes, 0, other.length);
  }

  @Override
  public int hashCode() {
    return hashBytes(bytes, 0, length);
  }

  /// STATIC UTILITIES FROM HERE DOWN

  /**
   * Converts the provided byte array to a String using the
   * UTF-8 encoding. If the input is malformed,
   * replace by a default value.
   */
  public static String decode(byte[] utf8) throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8), true);
  }

  public static String decode(byte[] utf8, int start, int length) 
    throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), true);
  }

  /**
   * Converts the provided byte array to a String using the
   * UTF-8 encoding. If <code>replace</code> is true, then
   * malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the
   * method throws a MalformedInputException.
   */
  public static String decode(byte[] utf8, int start, int length, boolean replace) 
    throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), replace);
  }

  private static String decode(ByteBuffer utf8, boolean replace) 
    throws CharacterCodingException {
    CharsetDecoder decoder = DECODER_FACTORY.get();
    if (replace) {
      decoder.onMalformedInput(
          java.nio.charset.CodingErrorAction.REPLACE);
      decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    String str = decoder.decode(utf8).toString();
    // set decoder back to its default value: REPORT
    if (replace) {
      decoder.onMalformedInput(CodingErrorAction.REPORT);
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return str;
  }

  /**
   * Converts the provided String to bytes using the
   * UTF-8 encoding. If the input is malformed,
   * invalid chars are replaced by a default value.
   * @return ByteBuffer: bytes stores at ByteBuffer.array() 
   *                     and length is ByteBuffer.limit()
   */

  public static ByteBuffer encode(String string)
    throws CharacterCodingException {
    return encode(string, true);
  }

  /**
   * Converts the provided String to bytes using the
   * UTF-8 encoding. If <code>replace</code> is true, then
   * malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the
   * method throws a MalformedInputException.
   * @return ByteBuffer: bytes stores at ByteBuffer.array() 
   *                     and length is ByteBuffer.limit()
   */
  public static ByteBuffer encode(String string, boolean replace)
    throws CharacterCodingException {
    CharsetEncoder encoder = ENCODER_FACTORY.get();
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPLACE);
      encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    ByteBuffer bytes = 
      encoder.encode(CharBuffer.wrap(string.toCharArray()));
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPORT);
      encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return bytes;
  }

  /**
   * Returns the next code point at the current position in
   * the buffer. The buffer's position will be incremented.
   * Any mark set on this buffer will be changed by this method!
   */
  @SuppressWarnings("SwitchStatementWithoutDefaultBranch")
  public static int bytesToCodePoint(ByteBuffer bytes) {
    bytes.mark();
    byte b = bytes.get();
    bytes.reset();
    int extraBytesToRead = bytesFromUTF8[(b & 0xFF)];
    if (extraBytesToRead < 0) return -1; // trailing byte!
    int ch = 0;

    switch (extraBytesToRead) {
      case 5: ch += (bytes.get() & 0xFF); ch <<= 6; /* remember, illegal UTF-8 */
      case 4: ch += (bytes.get() & 0xFF); ch <<= 6; /* remember, illegal UTF-8 */
      case 3: ch += (bytes.get() & 0xFF); ch <<= 6;
      case 2: ch += (bytes.get() & 0xFF); ch <<= 6;
      case 1: ch += (bytes.get() & 0xFF); ch <<= 6;
      case 0: ch += (bytes.get() & 0xFF);
    }
    ch -= offsetsFromUTF8[extraBytesToRead];

    return ch;
  }

  /**
   * Magic numbers for UTF-8. These are the number of bytes
   * that <em>follow</em> a given lead byte. Trailing bytes
   * have the value -1. The values 4 and 5 are presented in
   * this table, even though valid UTF-8 cannot include the
   * five and six byte sequences.
   */
  private static final int[] bytesFromUTF8 =
      { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0,
          // trail bytes
          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1,
          1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
          1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3,
          3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5 };

  private static final int offsetsFromUTF8[] =
      { 0x00000000, 0x00003080,
          0x000E2080, 0x03C82080, 0xFA082080, 0x82082080 };

  public static int hashBytes(byte[] bytes, int offset, int length) {
    int hash = 1;

    for(int i = offset; i < offset + length; ++i) {
      hash = 31 * hash + bytes[i];
    }

    return hash;
  }
}
