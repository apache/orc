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

#include "Adaptor.hh"
#include "Compression.hh"
#include "Exceptions.hh"
#include "LzoDecompressor.hh"
#include "lz4.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "zlib.h"

#include "wrap/snappy-wrapper.h"

namespace orc {

  void printBuffer(std::ostream& out,
                   const char *buffer,
                   uint64_t length) {
    const uint64_t width = 24;
    out << std::hex;
    for(uint64_t line = 0; line < (length + width - 1) / width; ++line) {
      out << std::setfill('0') << std::setw(7) << (line * width);
      for(uint64_t byte = 0;
          byte < width && line * width + byte < length; ++byte) {
        out << " " << std::setfill('0') << std::setw(2)
                  << static_cast<uint64_t>(0xff & buffer[line * width +
                                                             byte]);
      }
      out << "\n";
    }
    out << std::dec;
  }

  PositionProvider::PositionProvider(const std::list<uint64_t>& posns) {
    position = posns.begin();
  }

  uint64_t PositionProvider::next() {
    uint64_t result = *position;
    ++position;
    return result;
  }

  SeekableInputStream::~SeekableInputStream() {
    // PASS
  }

  SeekableArrayInputStream::~SeekableArrayInputStream() {
    // PASS
  }

  SeekableArrayInputStream::SeekableArrayInputStream
               (const unsigned char* values,
                uint64_t size,
                uint64_t blkSize
                ): data(reinterpret_cast<const char*>(values)) {
    length = size;
    position = 0;
    blockSize = blkSize == 0 ? length : static_cast<uint64_t>(blkSize);
  }

  SeekableArrayInputStream::SeekableArrayInputStream(const char* values,
                                                     uint64_t size,
                                                     uint64_t blkSize
                                                     ): data(values) {
    length = size;
    position = 0;
    blockSize = blkSize == 0 ? length : static_cast<uint64_t>(blkSize);
  }

  bool SeekableArrayInputStream::Next(const void** buffer, int*size) {
    uint64_t currentSize = std::min(length - position, blockSize);
    if (currentSize > 0) {
      *buffer = data + position;
      *size = static_cast<int>(currentSize);
      position += currentSize;
      return true;
    }
    *size = 0;
    return false;
  }

  void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount <= blockSize && unsignedCount <= position) {
        position -= unsignedCount;
      } else {
        throw std::logic_error("Can't backup that much!");
      }
    }
  }

  bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount + position <= length) {
        position += unsignedCount;
        return true;
      } else {
        position = length;
      }
    }
    return false;
  }

  google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position);
  }

  void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
    position = seekPosition.next();
  }

  std::string SeekableArrayInputStream::getName() const {
    std::ostringstream result;
    result << "SeekableArrayInputStream " << position << " of " << length;
    return result.str();
  }

  static uint64_t computeBlock(uint64_t request, uint64_t length) {
    return std::min(length, request == 0 ? 256 * 1024 : request);
  }

  SeekableFileInputStream::SeekableFileInputStream(InputStream* stream,
                                                   uint64_t offset,
                                                   uint64_t byteCount,
                                                   MemoryPool& _pool,
                                                   uint64_t _blockSize
                                                   ):pool(_pool),
                                                     input(stream),
                                                     start(offset),
                                                     length(byteCount),
                                                     blockSize(computeBlock
                                                               (_blockSize,
                                                                length)) {

    position = 0;
    buffer.reset(new DataBuffer<char>(pool));
    pushBack = 0;
  }

  SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
  }

  bool SeekableFileInputStream::Next(const void** data, int*size) {
    uint64_t bytesRead;
    if (pushBack != 0) {
      *data = buffer->data() + (buffer->size() - pushBack);
      bytesRead = pushBack;
    } else {
      bytesRead = std::min(length - position, blockSize);
      buffer->resize(bytesRead);
      if (bytesRead > 0) {
        input->read(buffer->data(), bytesRead, start+position);
        *data = static_cast<void*>(buffer->data());
      }
    }
    position += bytesRead;
    pushBack = 0;
    *size = static_cast<int>(bytesRead);
    return bytesRead != 0;
  }

  void SeekableFileInputStream::BackUp(int signedCount) {
    if (signedCount < 0) {
      throw std::logic_error("can't backup negative distances");
    }
    uint64_t count = static_cast<uint64_t>(signedCount);
    if (pushBack > 0) {
      throw std::logic_error("can't backup unless we just called Next");
    }
    if (count > blockSize || count > position) {
      throw std::logic_error("can't backup that far");
    }
    pushBack = static_cast<uint64_t>(count);
    position -= pushBack;
  }

  bool SeekableFileInputStream::Skip(int signedCount) {
    if (signedCount < 0) {
      return false;
    }
    uint64_t count = static_cast<uint64_t>(signedCount);
    position = std::min(position + count, length);
    pushBack = 0;
    return position < length;
  }

  int64_t SeekableFileInputStream::ByteCount() const {
    return static_cast<int64_t>(position);
  }

  void SeekableFileInputStream::seek(PositionProvider& location) {
    position = location.next();
    if (position > length) {
      position = length;
      throw std::logic_error("seek too far");
    }
    pushBack = 0;
  }

  std::string SeekableFileInputStream::getName() const {
    std::ostringstream result;
    result << input->getName() << " from " << start << " for "
           << length;
    return result.str();
  }

  enum DecompressState { DECOMPRESS_HEADER,
                         DECOMPRESS_START,
                         DECOMPRESS_CONTINUE,
                         DECOMPRESS_ORIGINAL,
                         DECOMPRESS_EOF};

  class ZlibDecompressionStream: public SeekableInputStream {
  public:
    ZlibDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                            size_t blockSize,
                            MemoryPool& pool);
    virtual ~ZlibDecompressionStream();
    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override;

  private:
    void readBuffer(bool failOnEof) {
      int length;
      if (!input->Next(reinterpret_cast<const void**>(&inputBuffer),
                       &length)) {
        if (failOnEof) {
          throw ParseError("Read past EOF in "
                           "ZlibDecompressionStream::readBuffer");
        }
        state = DECOMPRESS_EOF;
        inputBuffer = nullptr;
        inputBufferEnd = nullptr;
      } else {
        inputBufferEnd = inputBuffer + length;
      }
    }

    uint32_t readByte(bool failOnEof) {
      if (inputBuffer == inputBufferEnd) {
        readBuffer(failOnEof);
        if (state == DECOMPRESS_EOF) {
          return 0;
        }
      }
      return static_cast<unsigned char>(*(inputBuffer++));
    }

    void readHeader() {
      uint32_t header = readByte(false);
      if (state != DECOMPRESS_EOF) {
        header |= readByte(true) << 8;
        header |= readByte(true) << 16;
        if (header & 1) {
          state = DECOMPRESS_ORIGINAL;
        } else {
          state = DECOMPRESS_START;
        }
        remainingLength = header >> 1;
      } else {
        remainingLength = 0;
      }
    }

    MemoryPool& pool;
    const size_t blockSize;
    std::unique_ptr<SeekableInputStream> input;
    z_stream zstream;
    DataBuffer<char> buffer;

    // the current state
    DecompressState state;

    // the start of the current buffer
    // This pointer is not owned by us. It is either owned by zstream or
    // the underlying stream.
    const char* outputBuffer;
    // the size of the current buffer
    size_t outputBufferLength;
    // the size of the current chunk
    size_t remainingLength;

    // the last buffer returned from the input
    const char *inputBuffer;
    const char *inputBufferEnd;

    // roughly the number of bytes returned
    off_t bytesReturned;
  };

DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wold-style-cast")

  ZlibDecompressionStream::ZlibDecompressionStream
                   (std::unique_ptr<SeekableInputStream> inStream,
                    size_t _blockSize,
                    MemoryPool& _pool
                    ): pool(_pool),
                       blockSize(_blockSize),
                       buffer(pool, _blockSize) {
    input.reset(inStream.release());
    zstream.next_in = Z_NULL;
    zstream.avail_in = 0;
    zstream.zalloc = Z_NULL;
    zstream.zfree = Z_NULL;
    zstream.opaque = Z_NULL;
    zstream.next_out = reinterpret_cast<Bytef*>(buffer.data());
    zstream.avail_out = static_cast<uInt>(blockSize);
    int64_t result = inflateInit2(&zstream, -15);
    switch (result) {
    case Z_OK:
      break;
    case Z_MEM_ERROR:
      throw std::logic_error("Memory error from inflateInit2");
    case Z_VERSION_ERROR:
      throw std::logic_error("Version error from inflateInit2");
    case Z_STREAM_ERROR:
      throw std::logic_error("Stream error from inflateInit2");
    default:
      throw std::logic_error("Unknown error from inflateInit2");
    }
    outputBuffer = nullptr;
    outputBufferLength = 0;
    remainingLength = 0;
    state = DECOMPRESS_HEADER;
    inputBuffer = nullptr;
    inputBufferEnd = nullptr;
    bytesReturned = 0;
  }

DIAGNOSTIC_POP

  ZlibDecompressionStream::~ZlibDecompressionStream() {
    int64_t result = inflateEnd(&zstream);
    if (result != Z_OK) {
      // really can't throw in destructors
      std::cout << "Error in ~ZlibDecompressionStream() " << result << "\n";
    }
  }

  bool ZlibDecompressionStream::Next(const void** data, int*size) {
    // if the user pushed back, return them the partial buffer
    if (outputBufferLength) {
      *data = outputBuffer;
      *size = static_cast<int>(outputBufferLength);
      outputBuffer += outputBufferLength;
      outputBufferLength = 0;
      return true;
    }
    if (state == DECOMPRESS_HEADER || remainingLength == 0) {
      readHeader();
    }
    if (state == DECOMPRESS_EOF) {
      return false;
    }
    if (inputBuffer == inputBufferEnd) {
      readBuffer(true);
    }
    size_t availSize =
      std::min(static_cast<size_t>(inputBufferEnd - inputBuffer),
               remainingLength);
    if (state == DECOMPRESS_ORIGINAL) {
      *data = inputBuffer;
      *size = static_cast<int>(availSize);
      outputBuffer = inputBuffer + availSize;
      outputBufferLength = 0;
    } else if (state == DECOMPRESS_START) {
      zstream.next_in =
        reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
      zstream.avail_in = static_cast<uInt>(availSize);
      outputBuffer = buffer.data();
      zstream.next_out =
        reinterpret_cast<Bytef*>(const_cast<char*>(outputBuffer));
      zstream.avail_out = static_cast<uInt>(blockSize);
      if (inflateReset(&zstream) != Z_OK) {
        throw std::logic_error("Bad inflateReset in "
                               "ZlibDecompressionStream::Next");
      }
      int64_t result;
      do {
        result = inflate(&zstream, availSize == remainingLength ? Z_FINISH :
                         Z_SYNC_FLUSH);
        switch (result) {
        case Z_OK:
          remainingLength -= availSize;
          inputBuffer += availSize;
          readBuffer(true);
          availSize =
            std::min(static_cast<size_t>(inputBufferEnd - inputBuffer),
                     remainingLength);
          zstream.next_in =
            reinterpret_cast<Bytef*>(const_cast<char*>(inputBuffer));
          zstream.avail_in = static_cast<uInt>(availSize);
          break;
        case Z_STREAM_END:
          break;
        case Z_BUF_ERROR:
          throw std::logic_error("Buffer error in "
                                 "ZlibDecompressionStream::Next");
        case Z_DATA_ERROR:
          throw std::logic_error("Data error in "
                                 "ZlibDecompressionStream::Next");
        case Z_STREAM_ERROR:
          throw std::logic_error("Stream error in "
                                 "ZlibDecompressionStream::Next");
        default:
          throw std::logic_error("Unknown error in "
                                 "ZlibDecompressionStream::Next");
        }
      } while (result != Z_STREAM_END);
      *size = static_cast<int>(blockSize - zstream.avail_out);
      *data = outputBuffer;
      outputBufferLength = 0;
      outputBuffer += *size;
    } else {
      throw std::logic_error("Unknown compression state in "
                             "ZlibDecompressionStream::Next");
    }
    inputBuffer += availSize;
    remainingLength -= availSize;
    bytesReturned += *size;
    return true;
  }

  void ZlibDecompressionStream::BackUp(int count) {
    if (outputBuffer == nullptr || outputBufferLength != 0) {
      throw std::logic_error("Backup without previous Next in "
                             "ZlibDecompressionStream");
    }
    outputBuffer -= static_cast<size_t>(count);
    outputBufferLength = static_cast<size_t>(count);
    bytesReturned -= count;
  }

  bool ZlibDecompressionStream::Skip(int count) {
    bytesReturned += count;
    // this is a stupid implementation for now.
    // should skip entire blocks without decompressing
    while (count > 0) {
      const void *ptr;
      int len;
      if (!Next(&ptr, &len)) {
        return false;
      }
      if (len > count) {
        BackUp(len - count);
        count = 0;
      } else {
        count -= len;
      }
    }
    return true;
  }

  int64_t ZlibDecompressionStream::ByteCount() const {
    return bytesReturned;
  }

  void ZlibDecompressionStream::seek(PositionProvider& position) {
    input->seek(position);
    bytesReturned = input->ByteCount();
    if (!Skip(static_cast<int>(position.next()))) {
      throw ParseError("Bad skip in ZlibDecompressionStream::seek");
    }
  }

  std::string ZlibDecompressionStream::getName() const {
    std::ostringstream result;
    result << "zlib(" << input->getName() << ")";
    return result.str();
  }

  class BlockDecompressionStream: public SeekableInputStream {
  public:
    BlockDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                             size_t blockSize,
                             MemoryPool& pool);

    virtual ~BlockDecompressionStream() {}
    virtual bool Next(const void** data, int*size) override;
    virtual void BackUp(int count) override;
    virtual bool Skip(int count) override;
    virtual int64_t ByteCount() const override;
    virtual void seek(PositionProvider& position) override;
    virtual std::string getName() const override = 0;

  protected:
    virtual uint64_t decompress(const char *input, uint64_t length,
                                char *output, size_t maxOutputLength) = 0;

    std::string getStreamName() const {
      return input->getName();
    }

  private:
    void readBuffer(bool failOnEof) {
      int length;
      if (!input->Next(reinterpret_cast<const void**>(&inputBufferPtr),
                       &length)) {
        if (failOnEof) {
          throw ParseError(getName() + "read past EOF");
        }
        state = DECOMPRESS_EOF;
        inputBufferPtr = nullptr;
        inputBufferPtrEnd = nullptr;
      } else {
        inputBufferPtrEnd = inputBufferPtr + length;
      }
    }

    uint32_t readByte(bool failOnEof) {
      if (inputBufferPtr == inputBufferPtrEnd) {
        readBuffer(failOnEof);
        if (state == DECOMPRESS_EOF) {
          return 0;
        }
      }
      return static_cast<unsigned char>(*(inputBufferPtr++));
    }

    void readHeader() {
      uint32_t header = readByte(false);
      if (state != DECOMPRESS_EOF) {
        header |= readByte(true) << 8;
        header |= readByte(true) << 16;
        if (header & 1) {
          state = DECOMPRESS_ORIGINAL;
        } else {
          state = DECOMPRESS_START;
        }
        remainingLength = header >> 1;
      } else {
        remainingLength = 0;
      }
    }

    std::unique_ptr<SeekableInputStream> input;
    MemoryPool& pool;

    // may need to stitch together multiple input buffers;
    // to give snappy a contiguous block
    DataBuffer<char> inputBuffer;

    // uncompressed output
    DataBuffer<char> outputBuffer;

    // the current state
    DecompressState state;

    // the start of the current output buffer
    const char* outputBufferPtr;
    // the size of the current output buffer
    size_t outputBufferLength;

    // the size of the current chunk
    size_t remainingLength;

    // the last buffer returned from the input
    const char *inputBufferPtr;
    const char *inputBufferPtrEnd;

    // bytes returned by this stream
    off_t bytesReturned;
  };

  BlockDecompressionStream::BlockDecompressionStream
                   (std::unique_ptr<SeekableInputStream> inStream,
                    size_t bufferSize,
                    MemoryPool& _pool
                    ) : pool(_pool),
                        inputBuffer(pool, bufferSize),
                        outputBuffer(pool, bufferSize),
                        state(DECOMPRESS_HEADER),
                        outputBufferPtr(0),
                        outputBufferLength(0),
                        remainingLength(0),
                        inputBufferPtr(0),
                        inputBufferPtrEnd(0),
                        bytesReturned(0) {
    input.reset(inStream.release());
  }

  bool BlockDecompressionStream::Next(const void** data, int*size) {
    // if the user pushed back, return them the partial buffer
    if (outputBufferLength) {
      *data = outputBufferPtr;
      *size = static_cast<int>(outputBufferLength);
      outputBufferPtr += outputBufferLength;
      bytesReturned += outputBufferLength;
      outputBufferLength = 0;
      return true;
    }
    if (state == DECOMPRESS_HEADER || remainingLength == 0) {
      readHeader();
    }
    if (state == DECOMPRESS_EOF) {
      return false;
    }
    if (inputBufferPtr == inputBufferPtrEnd) {
      readBuffer(true);
    }

    size_t availSize =
      std::min(static_cast<size_t>(inputBufferPtrEnd - inputBufferPtr),
               remainingLength);
    if (state == DECOMPRESS_ORIGINAL) {
      *data = inputBufferPtr;
      *size = static_cast<int>(availSize);
      outputBufferPtr = inputBufferPtr + availSize;
      outputBufferLength = 0;
      inputBufferPtr += availSize;
      remainingLength -= availSize;
    } else if (state == DECOMPRESS_START) {
      // Get contiguous bytes of compressed block.
      const char *compressed = inputBufferPtr;
      if (remainingLength == availSize) {
          inputBufferPtr += availSize;
      } else {
        // Did not read enough from input.
        if (inputBuffer.capacity() < remainingLength) {
          inputBuffer.resize(remainingLength);
        }
        ::memcpy(inputBuffer.data(), inputBufferPtr, availSize);
        inputBufferPtr += availSize;
        compressed = inputBuffer.data();

        for (size_t pos = availSize; pos < remainingLength; ) {
          readBuffer(true);
          size_t avail =
              std::min(static_cast<size_t>(inputBufferPtrEnd -
                                           inputBufferPtr),
                       remainingLength - pos);
          ::memcpy(inputBuffer.data() + pos, inputBufferPtr, avail);
          pos += avail;
          inputBufferPtr += avail;
        }
      }

      outputBufferLength = decompress(compressed, remainingLength,
                                      outputBuffer.data(),
                                      outputBuffer.capacity());

      remainingLength = 0;
      state = DECOMPRESS_HEADER;
      *data = outputBuffer.data();
      *size = static_cast<int>(outputBufferLength);
      outputBufferPtr = outputBuffer.data() + outputBufferLength;
      outputBufferLength = 0;
    }

    bytesReturned += *size;
    return true;
  }

  void BlockDecompressionStream::BackUp(int count) {
    if (outputBufferPtr == nullptr || outputBufferLength != 0) {
      throw std::logic_error("Backup without previous Next in "+getName());
    }
    outputBufferPtr -= static_cast<size_t>(count);
    outputBufferLength = static_cast<size_t>(count);
    bytesReturned -= count;
  }

  bool BlockDecompressionStream::Skip(int count) {
    bytesReturned += count;
    // this is a stupid implementation for now.
    // should skip entire blocks without decompressing
    while (count > 0) {
      const void *ptr;
      int len;
      if (!Next(&ptr, &len)) {
        return false;
      }
      if (len > count) {
        BackUp(len - count);
        count = 0;
      } else {
        count -= len;
      }
    }
    return true;
  }

  int64_t BlockDecompressionStream::ByteCount() const {
    return bytesReturned;
  }

  void BlockDecompressionStream::seek(PositionProvider& position) {
    input->seek(position);
    if (!Skip(static_cast<int>(position.next()))) {
      throw ParseError("Bad skip in " + getName());
    }
  }

  class SnappyDecompressionStream: public BlockDecompressionStream {
  public:
    SnappyDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                              size_t blockSize,
                              MemoryPool& pool
                              ): BlockDecompressionStream
                                 (std::move(inStream),
                                  blockSize,
                                  pool) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "snappy(" << getStreamName() << ")";
      return result.str();
    }

  protected:
    virtual uint64_t decompress(const char *input, uint64_t length,
                                char *output, size_t maxOutputLength
                                ) override;
  };

  uint64_t SnappyDecompressionStream::decompress(const char *input,
                                                 uint64_t length,
                                                 char *output,
                                                 size_t maxOutputLength) {
    size_t outLength;
    if (!snappy::GetUncompressedLength(input, length, &outLength)) {
      throw ParseError("SnappyDecompressionStream choked on corrupt input");
    }

    if (outLength > maxOutputLength) {
      throw std::logic_error("Snappy length exceeds block size");
    }

    if (!snappy::RawUncompress(input, length, output)) {
      throw ParseError("SnappyDecompressionStream choked on corrupt input");
    }
    return outLength;
  }

  class LzoDecompressionStream: public BlockDecompressionStream {
  public:
    LzoDecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                           size_t blockSize,
                           MemoryPool& pool
                           ): BlockDecompressionStream
                              (std::move(inStream),
                               blockSize,
                               pool) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "lzo(" << getStreamName() << ")";
      return result.str();
    }

  protected:
    virtual uint64_t decompress(const char *input, uint64_t length,
                                char *output, size_t maxOutputLength
                                ) override;
  };

  uint64_t LzoDecompressionStream::decompress(const char *input,
                                              uint64_t length,
                                              char *output,
                                              size_t maxOutputLength) {
    return lzoDecompress(input, input + length, output,
                         output + maxOutputLength);
  }

  class Lz4DecompressionStream: public BlockDecompressionStream {
  public:
    Lz4DecompressionStream(std::unique_ptr<SeekableInputStream> inStream,
                           size_t blockSize,
                           MemoryPool& pool
                           ): BlockDecompressionStream
                              (std::move(inStream),
                               blockSize,
                               pool) {
      // PASS
    }

    std::string getName() const override {
      std::ostringstream result;
      result << "lz4(" << getStreamName() << ")";
      return result.str();
    }

  protected:
    virtual uint64_t decompress(const char *input, uint64_t length,
                                char *output, size_t maxOutputLength
                                ) override;
  };

  uint64_t Lz4DecompressionStream::decompress(const char *input,
                                              uint64_t length,
                                              char *output,
                                              size_t maxOutputLength) {
    int result = LZ4_decompress_safe(input, output, static_cast<int>(length),
                                     static_cast<int>(maxOutputLength));
    if (result < 0) {
      throw new ParseError(getName() + " - failed to decompress");
    }
    return static_cast<uint64_t>(result);
  }

  std::unique_ptr<SeekableInputStream>
     createDecompressor(CompressionKind kind,
                        std::unique_ptr<SeekableInputStream> input,
                        uint64_t blockSize,
                        MemoryPool& pool) {
    switch (static_cast<int64_t>(kind)) {
    case CompressionKind_NONE:
      return REDUNDANT_MOVE(input);
    case CompressionKind_ZLIB:
      return std::unique_ptr<SeekableInputStream>
        (new ZlibDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_SNAPPY:
      return std::unique_ptr<SeekableInputStream>
        (new SnappyDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_LZO:
      return std::unique_ptr<SeekableInputStream>
        (new LzoDecompressionStream(std::move(input), blockSize, pool));
    case CompressionKind_LZ4:
      return std::unique_ptr<SeekableInputStream>
        (new Lz4DecompressionStream(std::move(input), blockSize, pool));
    default: {
      std::ostringstream buffer;
      buffer << "Unknown compression codec " << kind;
      throw NotImplementedYet(buffer.str());
    }
    }
  }

}
