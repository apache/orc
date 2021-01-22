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

package org.apache.orc.shims;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;


/**
 * A writable stream that tracks the current position in the file.
 */
public interface PositionOutputStream extends AutoCloseable, Closeable, Flushable {

  /**
   * Write the entire array.
   * @param b the data to write
   */
  default void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  /**
   * Writes len bytes from the specified byte array starting at offset off to this output stream.
   * @param b the data to write
   * @param off starting offset in the array
   * @param len the number of bytes to write
   */
  void write(byte[] b, int off, int len) throws IOException;

  /**
   * Writes the specified byte to this output stream.
   * @param b a byte of data
   */
  void write(int b) throws IOException;

  /**
   * Return the current position in the OutputStream.
   * @return the position in bytes
   */
  long getPos() throws IOException;

  /**
   * Get a standard java.io.OutputStream for protobuf, etc.
   * Writes to either stream are equivalent.
   * @return this stream as a child of java's OutputStream
   */
  OutputStream getOutputStream();
}
