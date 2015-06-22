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

#ifndef ORC_FILE_HH
#define ORC_FILE_HH

#include <string>

#include "orc/orc-config.hh"
#include "orc/Reader.hh"

/** /file orc/OrcFile.hh
    @brief The top level interface to ORC.
*/

namespace orc {

  /**
   * An abstract interface for a buffer provided by the input stream.
   */
  class Buffer {
  public:
    virtual ~Buffer();

    /**
     * Get the start of the buffer.
     */
    virtual char *getStart() const = 0;

    /**
     * Get the length of the buffer in bytes.
     */
    virtual uint64_t getLength() const = 0;
  };

  /**
   * An abstract interface for providing ORC readers a stream of bytes.
   */
  class InputStream {
  public:
    virtual ~InputStream();

    /**
     * Get the total length of the file in bytes.
     */
    virtual uint64_t getLength() const = 0;

    /**
     * Read length bytes from the file starting at offset into
     * the buffer.
     * @param offset the position in the file to read from
     * @param length the number of bytes to read
     * @param buffer a Buffer to reuse from a previous call to read. Ownership
     *    of this buffer passes to the InputStream object.
     * @return the buffer with the requested data. The client owns the Buffer.
     */
    virtual Buffer* read(uint64_t offset,
                         uint64_t length,
                         Buffer* buffer) = 0;

    /**
     * Get the name of the stream for error messages.
     */
    virtual const std::string& getName() const = 0;
  };

  /**
   * Create a stream to a local file.
   * @param path the name of the file in the local file system
   */
  ORC_UNIQUE_PTR<InputStream> readLocalFile(const std::string& path);

  /**
   * Create a reader to the for the ORC file.
   * @param stream the stream to read
   * @param options the options for reading the file
   */
  ORC_UNIQUE_PTR<Reader> createReader(ORC_UNIQUE_PTR<InputStream> stream,
                                      const ReaderOptions& options);
}

#endif
