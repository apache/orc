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

#include "StripeStream.hh"
#include "RLE.hh"
#include "Reader.hh"
#include "orc/Exceptions.hh"

#include "wrap/coded-stream-wrapper.h"

namespace orc {

  StripeStreamsImpl::StripeStreamsImpl(const RowReaderImpl& _reader, uint64_t _index,long originalStripeId,
                                       const proto::StripeInformation& _stripeInfo,
                                       const proto::StripeFooter& _footer, uint64_t _stripeStart,
                                       InputStream& _input, const Timezone& _writerTimezone,
                                       const Timezone& _readerTimezone)
      : reader_(_reader),
        stripeInfo_(_stripeInfo),
        footer_(_footer),
        stripeIndex_(_index),
        originalStripeId(originalStripeId),
        stripeStart_(_stripeStart),
        input_(_input),
        writerTimezone_(_writerTimezone),
        readerTimezone_(_readerTimezone) {
    // +-----------------+---------------+-----------------+---------------+
    // |                 |               |                 |               |
    // |   unencrypted   |   encrypted   |   unencrypted   |   encrypted   |
    // |      index      |     index     |      data       |     data      |
    // |                 |               |                 |               |
    // +-----------------+---------------+-----------------+---------------+
    // The above refers to the storage layout of indexes and data, hence we need to follow this order to seek the stream.
    // Look for the index stream, first encrypted stream and then unencrypted stream.
    long currentOffset = _stripeStart;
    currentOffset = StripeStreamsImpl::findStreamsByArea(const_cast<proto::StripeFooter&>(footer_),currentOffset, Area::INDEX,reader_.getReaderEncryption(),streams);
    //Look for the data stream, first the encrypted stream and then the unencrypted stream.
    findStreamsByArea(const_cast<proto::StripeFooter&>(footer_),currentOffset, Area::DATA,reader_.getReaderEncryption(),streams);

    for (size_t i = 0; i < streams.size(); i++) {
      std::shared_ptr<StreamInformation> stream = streams.at(i);
      std::string key =
          std::to_string(stream->getColumnId()) + ":" + std::to_string(stream->getKind());
      streamMap.emplace(key, std::shared_ptr<StreamInformation>(stream));
    }
  }

  StripeStreamsImpl::~StripeStreamsImpl() {
    // PASS
  }

  StreamInformation::~StreamInformation() {
    // PASS
  }

  StripeInformation::~StripeInformation() {
    // PASS
  }

  StreamInformationImpl::~StreamInformationImpl() {
    // PASS
  }

  const std::vector<bool> StripeStreamsImpl::getSelectedColumns() const {
    return reader_.getSelectedColumns();
  }

  proto::ColumnEncoding StripeStreamsImpl::getEncoding(uint64_t columnId) const {
    // The encoding of encrypted columns needs to be obtained in a special way.
    ReaderEncryptionVariant* variant = this->reader_.getFileContents().encryption->getVariant(columnId);
    if(variant != nullptr){
      int subColumn = columnId - variant->getRoot()->getColumnId();
      return footer_.encryption().Get(variant->getVariantId()).encoding(subColumn);
    }else{
      return footer_.columns(static_cast<int>(columnId));
    }
  }

  const Timezone& StripeStreamsImpl::getWriterTimezone() const {
    return writerTimezone_;
  }

  const Timezone& StripeStreamsImpl::getReaderTimezone() const {
    return readerTimezone_;
  }

  std::ostream* StripeStreamsImpl::getErrorStream() const {
    return reader_.getFileContents().errorStream;
  }

  std::unique_ptr<SeekableInputStream> StripeStreamsImpl::getStream(uint64_t columnId,
                                                                    proto::Stream_Kind kind,
                                                                    bool shouldStream) const {    MemoryPool* pool = reader_.getFileContents().pool;
    const std::string skey = std::to_string(columnId) + ":" + std::to_string(kind);
    StreamInformation* streamInformation = streamMap[skey].get();
    if(streamInformation == nullptr){
      return nullptr;
    }
    uint64_t myBlock = shouldStream ? input_.getNaturalReadSize() : streamInformation->getLength();
    auto inputStream = std::make_unique<SeekableFileInputStream>(
        &input_, streamInformation->getOffset(), streamInformation->getLength(), *pool, myBlock);
    ReaderEncryptionVariant* variant = reader_.getReaderEncryption()->getVariant(columnId);
    if (variant != nullptr) {
      ReaderEncryptionKey* encryptionKey = variant->getKeyDescription();
      const int ivLength = encryptionKey->getAlgorithm()->getIvLength();
      std::vector<unsigned char> iv(ivLength);
      orc::CryptoUtil::modifyIvForStream(columnId, kind, originalStripeId, iv.data(), ivLength);
      const EVP_CIPHER* cipher = encryptionKey->getAlgorithm()->createCipher();
      std::vector<unsigned char> key = variant->getStripeKey(stripeIndex_)->getEncoded();
      std::unique_ptr<SeekableInputStream> decompressStream = createDecompressorAndDecryption(
          reader_.getCompression(), std::move(inputStream), reader_.getCompressionSize(), *pool,
          reader_.getFileContents().readerMetrics, key,
          iv, const_cast<EVP_CIPHER*>(cipher));
      return decompressStream;
    } else {
      return createDecompressor(reader_.getCompression(),
                                std::move(inputStream),
                                reader_.getCompressionSize(), *pool,reader_.getFileContents().readerMetrics);
    }
  }

  MemoryPool& StripeStreamsImpl::getMemoryPool() const {
    return *reader_.getFileContents().pool;
  }

  ReaderMetrics* StripeStreamsImpl::getReaderMetrics() const {
    return reader_.getFileContents().readerMetrics;
  }

  bool StripeStreamsImpl::getThrowOnHive11DecimalOverflow() const {
    return reader_.getThrowOnHive11DecimalOverflow();
  }

  bool StripeStreamsImpl::isDecimalAsLong() const {
    return reader_.getIsDecimalAsLong();
  }

  int32_t StripeStreamsImpl::getForcedScaleOnHive11Decimal() const {
    return reader_.getForcedScaleOnHive11Decimal();
  }

  const SchemaEvolution* StripeStreamsImpl::getSchemaEvolution() const {
    return reader_.getSchemaEvolution();
  }

  void StripeInformationImpl::ensureStripeFooterLoaded() const {
    if (stripeFooter_.get() == nullptr) {
      std::unique_ptr<SeekableInputStream> pbStream = createDecompressor(
          compression_,
          std::make_unique<SeekableFileInputStream>(stream_, offset_ + indexLength_ + dataLength_,
                                                    footerLength_, memory_),
          blockSize_, memory_, metrics_);
      stripeFooter_ = std::make_unique<proto::StripeFooter>();
      if (!stripeFooter_->ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError("Failed to parse the stripe footer");
      }
    }
  }

  std::unique_ptr<StreamInformation> StripeInformationImpl::getStreamInformation(
      uint64_t streamId) const {
    ensureStripeFooterLoaded();
    uint64_t streamOffset = offset_;
    for (uint64_t s = 0; s < streamId; ++s) {
      streamOffset += stripeFooter_->streams(static_cast<int>(s)).length();
    }
    return std::make_unique<StreamInformationImpl>(
        streamOffset, stripeFooter_->streams(static_cast<int>(streamId)));
  }

}  // namespace orc
