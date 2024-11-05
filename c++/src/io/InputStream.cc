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

#include "InputStream.hh"
#include "orc/Exceptions.hh"

#include <algorithm>
#include <iomanip>

namespace orc {

  void printBuffer(std::ostream& out, const char* buffer, uint64_t length) {
    const uint64_t width = 24;
    out << std::hex;
    for (uint64_t line = 0; line < (length + width - 1) / width; ++line) {
      out << std::setfill('0') << std::setw(7) << (line * width);
      for (uint64_t byte = 0; byte < width && line * width + byte < length; ++byte) {
        out << " " << std::setfill('0') << std::setw(2)
            << static_cast<uint64_t>(0xff & buffer[line * width + byte]);
      }
      out << "\n";
    }
    out << std::dec;
  }

  PositionProvider::PositionProvider(const std::list<uint64_t>& posns) {
    position_ = posns.begin();
  }

  uint64_t PositionProvider::next() {
    uint64_t result = *position_;
    ++position_;
    return result;
  }

  uint64_t PositionProvider::current() {
    return *position_;
  }

  SeekableInputStream::~SeekableInputStream() {
    // PASS
  }

  SeekableArrayInputStream::~SeekableArrayInputStream() {
    // PASS
  }

  SeekableArrayInputStream::SeekableArrayInputStream(const unsigned char* values, uint64_t size,
                                                     uint64_t blkSize)
      : data_(reinterpret_cast<const char*>(values)) {
    length_ = size;
    position_ = 0;
    blockSize_ = blkSize == 0 ? length_ : static_cast<uint64_t>(blkSize);
  }

  SeekableArrayInputStream::SeekableArrayInputStream(const char* values, uint64_t size,
                                                     uint64_t blkSize)
      : data_(values) {
    length_ = size;
    position_ = 0;
    blockSize_ = blkSize == 0 ? length_ : static_cast<uint64_t>(blkSize);
  }

  bool SeekableArrayInputStream::Next(const void** buffer, int* size) {
    uint64_t currentSize = std::min(length_ - position_, blockSize_);
    if (currentSize > 0) {
      *buffer = data_ + position_;
      *size = static_cast<int>(currentSize);
      position_ += currentSize;
      return true;
    }
    *size = 0;
    return false;
  }

  void SeekableArrayInputStream::BackUp(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount <= blockSize_ && unsignedCount <= position_) {
        position_ -= unsignedCount;
      } else {
        throw std::logic_error("Can't backup that much!");
      }
    }
  }

  bool SeekableArrayInputStream::Skip(int count) {
    if (count >= 0) {
      uint64_t unsignedCount = static_cast<uint64_t>(count);
      if (unsignedCount + position_ <= length_) {
        position_ += unsignedCount;
        return true;
      } else {
        position_ = length_;
      }
    }
    return false;
  }

  google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
    return static_cast<google::protobuf::int64>(position_);
  }

  void SeekableArrayInputStream::seek(PositionProvider& seekPosition) {
    position_ = seekPosition.next();
  }

  std::string SeekableArrayInputStream::getName() const {
    std::ostringstream result;
    result << "SeekableArrayInputStream " << position_ << " of " << length_;
    return result.str();
  }

  static uint64_t computeBlock(uint64_t request, uint64_t length) {
    return std::min(length, request == 0 ? 256 * 1024 : request);
  }

  SeekableFileInputStream::SeekableFileInputStream(InputStream* stream, uint64_t offset,
                                                   uint64_t byteCount, MemoryPool& pool,
                                                   uint64_t blockSize)
      : pool_(pool),
        input_(stream),
        start_(offset),
        length_(byteCount),
        blockSize_(computeBlock(blockSize, length_)) {
    position_ = 0;
    buffer_.reset(new DataBuffer<char>(pool_));
    pushBack_ = 0;
  }

  SeekableFileInputStream::~SeekableFileInputStream() {
    // PASS
  }

  bool SeekableFileInputStream::Next(const void** data, int* size) {
    uint64_t bytesRead;
    if (pushBack_ != 0) {
      *data = buffer_->data() + (buffer_->size() - pushBack_);
      bytesRead = pushBack_;
    } else {
      bytesRead = std::min(length_ - position_, blockSize_);
      buffer_->resize(bytesRead);
      if (bytesRead > 0) {
        input_->read(buffer_->data(), bytesRead, start_ + position_);
        *data = static_cast<void*>(buffer_->data());
      }
    }
    position_ += bytesRead;
    pushBack_ = 0;
    *size = static_cast<int>(bytesRead);
    return bytesRead != 0;
  }

  void SeekableFileInputStream::BackUp(int signedCount) {
    if (signedCount < 0) {
      throw std::logic_error("can't backup negative distances");
    }
    uint64_t count = static_cast<uint64_t>(signedCount);
    if (pushBack_ > 0) {
      throw std::logic_error("can't backup unless we just called Next");
    }
    if (count > blockSize_ || count > position_) {
      throw std::logic_error("can't backup that far");
    }
    pushBack_ = static_cast<uint64_t>(count);
    position_ -= pushBack_;
  }

  bool SeekableFileInputStream::Skip(int signedCount) {
    if (signedCount < 0) {
      return false;
    }
    uint64_t count = static_cast<uint64_t>(signedCount);
    position_ = std::min(position_ + count, length_);
    pushBack_ = 0;
    return position_ < length_;
  }

  int64_t SeekableFileInputStream::ByteCount() const {
    return static_cast<int64_t>(position_);
  }

  void SeekableFileInputStream::seek(PositionProvider& location) {
    position_ = location.next();
    if (position_ > length_) {
      position_ = length_;
      throw std::logic_error("seek too far");
    }
    pushBack_ = 0;
  }

  std::string SeekableFileInputStream::getName() const {
    std::ostringstream result;
    result << input_->getName() << " from " << start_ << " for " << length_;
    return result.str();
  }
  DecryptionInputStream::DecryptionInputStream(std::unique_ptr<SeekableInputStream> input,
                                               std::vector<unsigned char> key,
                                               std::vector<unsigned char> iv,
                                               const EVP_CIPHER* cipher,MemoryPool& pool)
      : input_(std::move(input)),
        key_(key),
        iv_(iv),
        cipher(cipher),
        pool(pool){
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (ctx == nullptr) {
      throw std::runtime_error("Failed to create EVP cipher context");
    }
    int ret = EVP_DecryptInit_ex(ctx, cipher, NULL, key_.data(), iv_.data());
    if (ret != 1) {
      EVP_CIPHER_CTX_free(ctx);
      EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
      throw std::runtime_error("Failed to initialize EVP cipher context");
    }
    ctx_ = ctx;
    outputBuffer_.reset(new DataBuffer<unsigned char>(pool));
    inputBuffer_.reset(new DataBuffer<unsigned char>(pool));
  }

  DecryptionInputStream::~DecryptionInputStream() {
    EVP_CIPHER_CTX_free(ctx_);
    EVP_CIPHER_free(const_cast<evp_cipher_st*>(cipher));
  }

  bool DecryptionInputStream::Next(const void** data, int* size) {
    int bytesRead = 0;
    //const void* ptr;
    const void* inptr = static_cast<void*>(inputBuffer_->data());
    input_->Next(&inptr, &bytesRead);
    if (bytesRead == 0) {
      return false;
    }
    // decrypt data
    const unsigned char* result = static_cast<const unsigned char*>(inptr);
    int outlen = 0;
    //int blockSize = EVP_CIPHER_block_size(this->cipher);
    outputBuffer_->resize(bytesRead);
    int ret = EVP_DecryptUpdate(ctx_, outputBuffer_->data(), &outlen, result, bytesRead);
    if (ret != 1) {
      throw std::runtime_error("Failed to decrypt data");
    }
    outputBuffer_->resize(outlen);
    *data = outputBuffer_->data();
    *size = outputBuffer_->size();
    return true;
  }
  void DecryptionInputStream::BackUp(int count) {
    this->input_->BackUp(count);
  }

  bool DecryptionInputStream::Skip(int count) {
    return this->input_->Skip(count);
  }

  google::protobuf::int64 DecryptionInputStream::ByteCount() const {
    return input_->ByteCount();
  }

  void DecryptionInputStream::seek(PositionProvider& position) {
    //std::cout<<"PPP:DecryptionInputStream::seek:"<<position.current()<<std::endl;
    changeIv(position.current());
    input_->seek(position);
  }
  void DecryptionInputStream::changeIv(long offset) {
    int blockSize = EVP_CIPHER_key_length(cipher);
    long encryptionBlocks = offset / blockSize;
    long extra = offset % blockSize;
    std::fill(iv_.end() - 8, iv_.end(), 0);
    if (encryptionBlocks != 0) {
      // Add the encryption blocks into the initial iv, to compensate for
      // skipping over decrypting those bytes.
      int posn = iv_.size() - 1;
      while (encryptionBlocks > 0) {
        long sum = (iv_[posn] & 0xff) + encryptionBlocks;
        iv_[posn--] = (unsigned char) sum;
        encryptionBlocks = sum / 0x100;
      }
    }
    EVP_DecryptInit_ex(ctx_, cipher, NULL, key_.data(), iv_.data());
    // If the range starts at an offset that doesn't match the encryption
    // block, we need to advance some bytes within an encryption block.
    if (extra > 0) {
      std::vector<unsigned char> decrypted(extra);
      int decrypted_len;
      EVP_DecryptUpdate(ctx_, decrypted.data(), &decrypted_len, decrypted.data(), extra);
    }
  }
  std::string DecryptionInputStream::getName() const {
    return "DecryptionInputStream("+input_->getName()+")";
  }
}// namespace orc
