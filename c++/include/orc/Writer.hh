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

#ifndef ORC_WRITER_HH
#define ORC_WRITER_HH

#include "orc/Common.hh"
#include "orc/orc-config.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"

#include <memory>
#include <string>
#include <vector>

namespace orc {

  // classes that hold data members so we can maintain binary compatibility
  struct WriterOptionsPrivate;

  enum EncodingStrategy {
    EncodingStrategy_SPEED = 0,
    EncodingStrategy_COMPRESSION
  };

  enum CompressionStrategy {
    CompressionStrategy_SPEED = 0,
    CompressionStrategy_COMPRESSION
  };

  enum RleVersion {
    RleVersion_1,
    RleVersion_2
  };

  class Timezone;

  /**
   * Options for creating a Writer.
   */
  class WriterOptions {
  private:
    ORC_UNIQUE_PTR<WriterOptionsPrivate> privateBits;

  public:
    WriterOptions();
    WriterOptions(const WriterOptions&);
    WriterOptions(WriterOptions&);
    WriterOptions& operator=(const WriterOptions&);
    virtual ~WriterOptions();

    /**
     * Set the strip size.
     */
    WriterOptions& setStripeSize(uint64_t size);

    /**
     * Get the strip size.
     * @return if not set, return default value.
     */
    uint64_t getStripeSize() const;

    /**
     * Set the block size.
     */
    WriterOptions& setBlockSize(uint64_t size);

    /**
     * Get the block size.
     * @return if not set, return default value.
     */
    uint64_t getBlockSize() const;

    /**
     * Set row index stride.
     */
    WriterOptions& setRowIndexStride(uint64_t stride);

    /**
     * Get the index stride size.
     * @return if not set, return default value.
     */
    uint64_t getRowIndexStride() const;

    /**
     * Set the buffer size.
     */
    WriterOptions& setBufferSize(uint64_t size);

    /**
     * Get the buffer size.
     * @return if not set, return default value.
     */
    uint64_t getBufferSize() const;

    /**
     * Set the dictionary key size threshold.
     * 0 to disable dictionary encoding.
     * 1 to always enable dictionary encoding.
     */
    WriterOptions& setDictionaryKeySizeThreshold(double val);

    /**
     * Get the dictionary key size threshold.
     */
    double getDictionaryKeySizeThreshold() const;

    /**
     * Set whether or not to have block padding.
     */
    WriterOptions& setBlockPadding(bool padding);

    /**
     * Get whether or not to have block padding.
     * @return if not set, return default value which is false.
     */
    bool getBlockPadding() const;

    /**
     * Set Run length encoding version
     */
    WriterOptions& setRleVersion(RleVersion version);

    /**
     * Get Run Length Encoding version
     */
    RleVersion getRleVersion() const;

    /**
     * Set compression kind.
     */
    WriterOptions& setCompression(CompressionKind comp);

    /**
     * Get the compression kind.
     * @return if not set, return default value which is ZLIB.
     */
    CompressionKind getCompression() const;

    /**
     * Set the encoding strategy.
     */
    WriterOptions& setEncodingStrategy(EncodingStrategy strategy);

    /**
     * Get the encoding strategy.
     * @return if not set, return default value which is SPEED.
     */
    EncodingStrategy getEncodingStrategy() const;

    /**
     * Set the compression strategy.
     */
    WriterOptions& setCompressionStrategy(CompressionStrategy strategy);

    /**
     * Get the compression strategy.
     * @return if not set, return default value which is speed.
     */
    CompressionStrategy getCompressionStrategy() const;

    /**
     * Set the writer version.
     */
    WriterOptions& setWriterVersion(WriterVersion version);

    /**
     * Get the strip size.
     * @return if not set, return default value which is WriterVersion_ORIGINAL
     */
    WriterVersion getWriterVersion() const;

    /**
     * Set the padding tolerance.
     */
    WriterOptions& setPaddingTolerance(double tolerance);

    /**
     * Get the padding tolerance.
     * @return if not set, return default value which is zero.
     */
    double getPaddingTolerance() const;

    /**
     * Set the memory pool.
     */
    WriterOptions& setMemoryPool(MemoryPool * memoryPool);

    /**
     * Get the strip size.
     * @return if not set, return default memory pool.
     */
    MemoryPool * getMemoryPool() const;

    /**
     * Set the error stream.
     */
    WriterOptions& setErrorStream(std::ostream& errStream);

    /**
     * Get the error stream.
     * @return if not set, return std::err.
     */
    std::ostream * getErrorStream() const;

    /**
     * Set whether or not to write statistics (file statistics,
     * stripe statistics, etc.)
     */
    WriterOptions& setEnableStats(bool enable);

    /**
     * Get whether or not to write statistics (file statistics,
     * stripe statistics, etc.)
     * @return if not set, the default is true
     */
     bool getEnableStats() const;

    /**
     * Set whether or not to update string min/max stats
     */
    WriterOptions& setEnableStrStatsCmp(bool enable);

    /**
     * Get whether or not to update string min/max stats
     * @return if not set, the default is false
     */
    bool getEnableStrStatsCmp() const;

    /**
     * Set whether or not to write row group index
     */
    WriterOptions& setEnableIndex(bool enable);

    /**
     * Get whether or not to write row group index
     * @return if not set, the default is false
     */
    bool getEnableIndex() const;

    /**
     * Get writer timezone
     * @return writer timezone
     */
    const Timezone* getTimezone() const;

    /**
     * Set writer timezone
     * @param zone writer timezone name
     */
    WriterOptions& setTimezone(const std::string& zone);
  };

  class Writer {
  public:
    virtual ~Writer();

    /**
     * Create a row batch for writing the columns into this file.
     * @param size the number of rows to read
     * @return a new ColumnVectorBatch to write into
     */
    virtual ORC_UNIQUE_PTR<ColumnVectorBatch> createRowBatch(uint64_t size
                                                             ) const = 0;

    /**
     * Add a row batch into current writer.
     * @param rowsToAdd the row batch data to write.
     */
    virtual void add(ColumnVectorBatch& rowsToAdd) = 0;

    /**
     * Close the write and flush any pending data to the output stream.
     */
    virtual void close() = 0;
  };
}

#endif
