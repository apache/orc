package org.apache.orc.impl;

import java.nio.charset.StandardCharsets;

public final class Utf8Utils {

  public static int charLength(byte[] data, int offset, int length) {
    int chars = 0;
    for (int i = 0; i < length; i++) {
      if (isUtfStartByte(data[offset +i ])) {
        chars++;
      }
    }
    return chars;
  }

  /**
   * Return the number of bytes required to read at most
   * maxLength characters in full from a utf-8 encoded byte array provided
   * by data[offset:offset+length]. This does not validate utf-8 data, but
   * operates correctly on already valid utf-8 data.
   *
   * @param maxCharLength
   * @param data
   * @param offset
   * @param length
   */
  public static int truncateBytesTo(int maxCharLength, byte[] data, int offset, int length) {
    int chars = 0;
    if (length <= maxCharLength) {
      return length;
    }
    for (int i = 0; i < length; i++) {
      if (isUtfStartByte(data[offset +i ])) {
        chars++;
      }
      if (chars > maxCharLength) {
        return i;
      }
    }
    // everything fits
    return length;
  }

  /**
   * Checks if b is the first byte of a UTF-8 character.
   *
   */
  public static boolean isUtfStartByte(byte b) {
    return (b & 0xC0) != 0x80;
  }

  /**
   * Find the start of the last character that ends in the current string.
   * @param text the bytes of the utf-8
   * @param from the first byte location
   * @param until the last byte location
   * @return the index of the last character
   */
  public static int findLastCharacter(byte[] text, int from, int until) {
    int posn = until;
    /* we don't expect characters more than 5 bytes */
    while (posn >= from) {
      if (isUtfStartByte(text[posn])) {
        return posn;
      }
      posn -= 1;
    }
    /* beginning of a valid char not found */
    throw new IllegalArgumentException(
        "Could not truncate string, beginning of a valid char not found");
  }

  /**
   * Get the code point at a given location in the byte array.
   * @param source the bytes of the string
   * @param from the offset to start at
   * @param len the number of bytes in the character
   * @return the code point
   */
  public static int getCodePoint(byte[] source, int from, int len) {
    return new String(source, from, len, StandardCharsets.UTF_8)
        .codePointAt(0);
  }

}
