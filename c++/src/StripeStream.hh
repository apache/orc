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

#ifndef ORC_STRIPE_STREAM_HH
#define ORC_STRIPE_STREAM_HH

#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "ColumnReader.hh"
#include "Timezone.hh"
#include "TypeImpl.hh"
#include "security/ReaderEncryption.hh"

namespace orc {

  class RowReaderImpl;
  /**
   * StreamInformation Implementation
  */
  class StreamInformationImpl : public StreamInformation {
   private:
    StreamKind kind;
    uint64_t column;
    uint64_t offset;
    uint64_t length;
    proto::Stream_Kind originalKind;

   public:
    StreamInformationImpl(uint64_t _offset, const proto::Stream& stream)
        : kind(static_cast<StreamKind>(stream.kind())),
          column(stream.column()),
          offset(_offset),
          length(stream.length()) {
      // PASS
    }
    StreamInformationImpl(proto::Stream_Kind originalKind, uint64_t column, uint64_t _offset,
                          uint64_t length)
        : kind(static_cast<StreamKind>(originalKind)),
          column(column),
          offset(_offset),
          length(length),
          originalKind(originalKind) {
      // PASS
    }
    ~StreamInformationImpl() override;

    StreamKind getKind() const override {
      return kind;
    }

    uint64_t getColumnId() const override {
      return column;
    }

    uint64_t getOffset() const override {
      return offset;
    }

    uint64_t getLength() const override {
      return length;
    }
  };

  enum class Area { DATA, INDEX, FOOTER };

  /**
   * StripeStream Implementation
   */

  class StripeStreamsImpl : public StripeStreams {
   private:
    const RowReaderImpl& reader_;
    const proto::StripeInformation& stripeInfo_;
    const proto::StripeFooter& footer_;
    const uint64_t stripeIndex_;
    long originalStripeId = 0;
    const uint64_t stripeStart_;
    InputStream& input_;
    const Timezone& writerTimezone_;
    const Timezone& readerTimezone_;
    mutable std::map<std::string, std::shared_ptr<StreamInformation>> streamMap;
    std::vector<std::shared_ptr<StreamInformation>> streams;
    static long handleStream(long offset, const proto::Stream& stream, Area area,
                                   ReaderEncryptionVariant* variant, ReaderEncryption* encryption,
                                   std::vector<std::shared_ptr<StreamInformation>>& streams) {
      int column = stream.column();
      if (stream.has_kind()) {
        proto::Stream_Kind kind = stream.kind();
        // If there are no encrypted columns
        /*            if (encryption->getKeys().empty()) {
                        StreamInformationImpl* info = new StreamInformationImpl(kind, column, offset, stream.length());
                        streams.push_back(std::shared_ptr<StreamInformation>(info));
                        return stream.length();
                    }*/
        if (getArea(kind) != area || kind == proto::Stream_Kind::Stream_Kind_ENCRYPTED_INDEX ||
            kind == proto::Stream_Kind::Stream_Kind_ENCRYPTED_DATA) {
          //Ignore the placeholder that should not be included in the offset calculation.
          return 0;
        }
        if (encryption->getVariant(column) == variant) {
          StreamInformationImpl* info = new StreamInformationImpl(kind, column, offset, stream.length());
          streams.push_back(std::shared_ptr<StreamInformation>(info));
        }
      }
      return stream.length();
    }
   public:
    StripeStreamsImpl(const RowReaderImpl& reader, uint64_t index, long originalStripeId,
                      const proto::StripeInformation& stripeInfo, const proto::StripeFooter& footer,
                      uint64_t stripeStart, InputStream& input, const Timezone& writerTimezone,
                      const Timezone& readerTimezone);

    virtual ~StripeStreamsImpl() override;

    virtual const std::vector<bool> getSelectedColumns() const override;

    virtual proto::ColumnEncoding getEncoding(uint64_t columnId) const override;

    virtual std::unique_ptr<SeekableInputStream> getStream(uint64_t columnId,
                                                           proto::Stream_Kind kind,
                                                           bool shouldStream) const override;

    MemoryPool& getMemoryPool() const override;

    ReaderMetrics* getReaderMetrics() const override;

    const Timezone& getWriterTimezone() const override;

    const Timezone& getReaderTimezone() const override;

    std::ostream* getErrorStream() const override;

    bool getThrowOnHive11DecimalOverflow() const override;

    bool isDecimalAsLong() const override;

    int32_t getForcedScaleOnHive11Decimal() const override;

    const SchemaEvolution* getSchemaEvolution() const override;
    //
    static Area getArea(proto::Stream_Kind kind) {
      switch (kind) {
        case proto::Stream_Kind::Stream_Kind_FILE_STATISTICS:
        case proto::Stream_Kind::Stream_Kind_STRIPE_STATISTICS:
          return Area::FOOTER;
        case proto::Stream_Kind::Stream_Kind_ROW_INDEX:
        case proto::Stream_Kind::Stream_Kind_DICTIONARY_COUNT:
        case proto::Stream_Kind::Stream_Kind_BLOOM_FILTER:
        case proto::Stream_Kind::Stream_Kind_BLOOM_FILTER_UTF8:
        case proto::Stream_Kind::Stream_Kind_ENCRYPTED_INDEX:
          return Area::INDEX;
        default:
          return Area::DATA;
      }
    }
    static long findStreamsByArea(proto::StripeFooter& footer, long currentOffset, Area area,
                                  ReaderEncryption* encryption,
                                  std::vector<std::shared_ptr<StreamInformation>>& streams) {
      // Look for the unencrypted stream.
      for (const proto::Stream& stream : footer.streams()) {
        currentOffset += handleStream(currentOffset, stream, area, nullptr, encryption, streams);
      }
      //If there are encrypted columns
      if (!encryption->getKeys().empty()) {
        std::vector<std::shared_ptr<ReaderEncryptionVariant>>& vList = encryption->getVariants();
        for (std::vector<orc::ReaderEncryptionVariant*>::size_type i = 0; i < vList.size(); i++) {
          ReaderEncryptionVariant* variant = vList.at(i).get();
          int variantId = variant->getVariantId();
          const proto::StripeEncryptionVariant& stripeVariant = footer.encryption(variantId);
          for (const proto::Stream& stream : stripeVariant.streams()) {
            currentOffset += handleStream(currentOffset, stream, area, variant, encryption, streams);
          }
        }
      }
      return currentOffset;
    }
  };

  /**
   * StripeInformation Implementation
   */

  class StripeInformationImpl : public StripeInformation {
    uint64_t offset_;
    uint64_t indexLength_;
    uint64_t dataLength_;
    uint64_t footerLength_;
    uint64_t numRows_;
    InputStream* stream_;
    MemoryPool& memory_;
    CompressionKind compression_;
    uint64_t blockSize_;
    mutable std::unique_ptr<proto::StripeFooter> stripeFooter_;
    ReaderMetrics* metrics_;
    void ensureStripeFooterLoaded() const;
    std::shared_ptr<std::vector<std::vector<unsigned char>>> encryptedKeys;
    long originalStripeId = 0;
    ReaderEncryption* encryption;

   public:
    StripeInformationImpl(uint64_t offset, uint64_t indexLength, uint64_t dataLength,
                          uint64_t footerLength, uint64_t numRows, InputStream* stream,
                          MemoryPool& memory, CompressionKind compression, uint64_t blockSize,
                          ReaderMetrics* metrics)
        : offset_(offset),
          indexLength_(indexLength),
          dataLength_(dataLength),
          footerLength_(footerLength),
          numRows_(numRows),
          stream_(stream),
          memory_(memory),
          compression_(compression),
          blockSize_(blockSize),
          metrics_(metrics) {
      // PASS
    }
    StripeInformationImpl(proto::StripeInformation* stripeInfo, ReaderEncryption* encryption,
                          long previousOriginalStripeId,
                          std::shared_ptr<std::vector<std::vector<unsigned char>>> previousKeys, InputStream* _stream,
                          MemoryPool& _memory, CompressionKind _compression, uint64_t _blockSize,
                          ReaderMetrics* _metrics)
        : offset_(stripeInfo->offset()),
          indexLength_(stripeInfo->index_length()),
          dataLength_(stripeInfo->data_length()),
          footerLength_(stripeInfo->footer_length()),
          numRows_(stripeInfo->number_of_rows()),
          stream_(_stream),
          memory_(_memory),
          compression_(_compression),
          blockSize_(_blockSize),
          metrics_(_metrics),
          encryption(encryption) {
      // It is usually the first strip that has this value.
      if (stripeInfo->has_encrypt_stripe_id()) {
        originalStripeId = stripeInfo->encrypt_stripe_id();
      } else {
        originalStripeId = previousOriginalStripeId + 1;
      }
      // The value is generally present in the first strip.
      // Each encrypted column corresponds to a key.
      if (stripeInfo->encrypted_local_keys_size() != 0) {
        encryptedKeys = std::shared_ptr<std::vector<std::vector<unsigned char>>>(
            new std::vector<std::vector<unsigned char>>());
        for (int i = 0; i < static_cast<int>(stripeInfo->encrypted_local_keys_size()); i++) {
          std::string str = stripeInfo->encrypted_local_keys(i);
          std::vector<unsigned char> chars(str.begin(), str.end());
          encryptedKeys->push_back(chars);
        }
      } else {
        encryptedKeys = std::shared_ptr<std::vector<std::vector<unsigned char>>>(previousKeys);
      }
    }
    virtual ~StripeInformationImpl() override {
      // PASS
    }

    uint64_t getOffset() const override {
      return offset_;
    }

    uint64_t getLength() const override {
      return indexLength_ + dataLength_ + footerLength_;
    }
    uint64_t getIndexLength() const override {
      return indexLength_;
    }

    uint64_t getDataLength() const override {
      return dataLength_;
    }

    uint64_t getFooterLength() const override {
      return footerLength_;
    }

    uint64_t getNumberOfRows() const override {
      return numRows_;
    }

    uint64_t getNumberOfStreams() const override {
      ensureStripeFooterLoaded();
      return static_cast<uint64_t>(stripeFooter_->streams_size());
    }

    std::unique_ptr<StreamInformation> getStreamInformation(uint64_t streamId) const override;

    ColumnEncodingKind getColumnEncoding(uint64_t colId) const override {
      ensureStripeFooterLoaded();
      ReaderEncryptionVariant* variant = this->encryption->getVariant(colId);
      if (variant != nullptr) {
        int subColumn = colId - variant->getRoot()->getColumnId();
        return static_cast<ColumnEncodingKind>(
            stripeFooter_->encryption().Get(variant->getVariantId()).encoding(subColumn).kind());
      } else {
        return static_cast<ColumnEncodingKind>(stripeFooter_->columns(static_cast<int>(colId)).kind());
      }
    }

    uint64_t getDictionarySize(uint64_t colId) const override {
      ensureStripeFooterLoaded();
      ReaderEncryptionVariant* variant = this->encryption->getVariant(colId);
      if (variant != nullptr) {
        int subColumn = colId - variant->getRoot()->getColumnId();
        return static_cast<ColumnEncodingKind>(
            stripeFooter_->encryption().Get(variant->getVariantId()).encoding(subColumn).dictionary_size());
      } else {
        return static_cast<ColumnEncodingKind>(stripeFooter_->columns(static_cast<int>(colId)).dictionary_size());
      }
    }

    const std::string& getWriterTimezone() const override {
      ensureStripeFooterLoaded();
      return stripeFooter_->writer_timezone();
    }
    std::shared_ptr<std::vector<std::vector<unsigned char>>> getEncryptedLocalKeys() const override {
      return this->encryptedKeys;
    }
    std::vector<unsigned char>& getEncryptedLocalKeyByVariantId(int col) const override {
      return getEncryptedLocalKeys()->at(col);
    }
    long getOriginalStripeId() const override { return originalStripeId; }
  };

}  // namespace orc

#endif
