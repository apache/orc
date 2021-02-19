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
import java.io.IOException;
import java.io.InputStream;

/**
 * A readable stream that tracks the current position in the file.
 */
public interface SeekableInputStream extends AutoCloseable, Closeable {

  /**
   * Read data from the stream.
   * @param position the position in bytes from the start of the file
   * @param buffer the buffer to read the data into
   * @param offset the offset in the buffer to start reading data into
   * @param length the number of bytes to read
   * @throws IOException
   */
  void readFully(long position, byte[] buffer, int offset, int length) throws IOException;

  /**
   * Moves the file pointer to the given offset.
   * @param offset
   * @throws IOException
   */
  void seek(long offset) throws IOException;

  /**
   * Get the current position of the stream.
   * @return the bytes
   * @throws IOException
   */
  long getPosition() throws IOException;

  /**
   * Get a standard java.io.OutputStream for protobuf, etc.
   * Writes to either stream are equivalent.
   * @return this stream as a child of java's OutputStream
   */
  InputStream getInputStream();
}
