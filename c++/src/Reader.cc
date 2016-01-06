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

#include "orc/Int128.hh"
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"

#include "Adaptor.hh"
#include "ColumnReader.hh"
#include "Exceptions.hh"
#include "RLE.hh"
#include "TypeImpl.hh"

#include "wrap/coded-stream-wrapper.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace orc {

  struct ReaderOptionsPrivate {
    bool setIndexes;
    bool setNames;
    std::list<uint64_t> includedColumnIndexes;
    std::list<std::string> includedColumnNames;
    uint64_t dataStart;
    uint64_t dataLength;
    uint64_t tailLocation;
    bool throwOnHive11DecimalOverflow;
    int32_t forcedScaleOnHive11Decimal;
    std::ostream* errorStream;
    MemoryPool* memoryPool;
    std::string serializedTail;

    ReaderOptionsPrivate() {
      setIndexes = false;
      setNames = false;
      dataStart = 0;
      dataLength = std::numeric_limits<uint64_t>::max();
      tailLocation = std::numeric_limits<uint64_t>::max();
      throwOnHive11DecimalOverflow = true;
      forcedScaleOnHive11Decimal = 6;
      errorStream = &std::cerr;
      memoryPool = getDefaultPool();
    }
  };

  ReaderOptions::ReaderOptions():
    privateBits(std::unique_ptr<ReaderOptionsPrivate>
                (new ReaderOptionsPrivate())) {
    // PASS
  }

  ReaderOptions::ReaderOptions(const ReaderOptions& rhs):
    privateBits(std::unique_ptr<ReaderOptionsPrivate>
                (new ReaderOptionsPrivate(*(rhs.privateBits.get())))) {
    // PASS
  }

  ReaderOptions::ReaderOptions(ReaderOptions& rhs) {
    // swap privateBits with rhs
    ReaderOptionsPrivate* l = privateBits.release();
    privateBits.reset(rhs.privateBits.release());
    rhs.privateBits.reset(l);
  }

  ReaderOptions& ReaderOptions::operator=(const ReaderOptions& rhs) {
    if (this != &rhs) {
      privateBits.reset(new ReaderOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
  }

  ReaderOptions::~ReaderOptions() {
    // PASS
  }

  ReaderOptions& ReaderOptions::include(const std::list<uint64_t>& include) {
    privateBits->setIndexes = true;
    privateBits->includedColumnIndexes.assign(include.begin(), include.end());
    privateBits->setNames = false;
    privateBits->includedColumnNames.clear();
    return *this;
  }

  ReaderOptions& ReaderOptions::include
       (const std::list<std::string>& include) {
    privateBits->setNames = true;
    privateBits->includedColumnNames.assign(include.begin(), include.end());
    privateBits->setIndexes = false;
    privateBits->includedColumnIndexes.clear();
    return *this;
  }

  ReaderOptions& ReaderOptions::range(uint64_t offset,
                                      uint64_t length) {
    privateBits->dataStart = offset;
    privateBits->dataLength = length;
    return *this;
  }

  ReaderOptions& ReaderOptions::setTailLocation(uint64_t offset) {
    privateBits->tailLocation = offset;
    return *this;
  }

  ReaderOptions& ReaderOptions::setMemoryPool(MemoryPool& pool) {
    privateBits->memoryPool = &pool;
    return *this;
  }

  ReaderOptions& ReaderOptions::setSerializedFileTail(const std::string& value
                                                      ) {
    privateBits->serializedTail = value;
    return *this;
  }

  MemoryPool* ReaderOptions::getMemoryPool() const{
    return privateBits->memoryPool;
  }

  bool ReaderOptions::getIndexesSet() const {
    return privateBits->setIndexes;
  }

  const std::list<uint64_t>& ReaderOptions::getInclude() const {
    return privateBits->includedColumnIndexes;
  }

  bool ReaderOptions::getNamesSet() const {
    return privateBits->setNames;
  }

  const std::list<std::string>& ReaderOptions::getIncludeNames() const {
    return privateBits->includedColumnNames;
  }

  uint64_t ReaderOptions::getOffset() const {
    return privateBits->dataStart;
  }

  uint64_t ReaderOptions::getLength() const {
    return privateBits->dataLength;
  }

  uint64_t ReaderOptions::getTailLocation() const {
    return privateBits->tailLocation;
  }

  ReaderOptions& ReaderOptions::throwOnHive11DecimalOverflow(bool shouldThrow){
    privateBits->throwOnHive11DecimalOverflow = shouldThrow;
    return *this;
  }

  bool ReaderOptions::getThrowOnHive11DecimalOverflow() const {
    return privateBits->throwOnHive11DecimalOverflow;
  }

  ReaderOptions& ReaderOptions::forcedScaleOnHive11Decimal(int32_t forcedScale
                                                           ) {
    privateBits->forcedScaleOnHive11Decimal = forcedScale;
    return *this;
  }

  int32_t ReaderOptions::getForcedScaleOnHive11Decimal() const {
    return privateBits->forcedScaleOnHive11Decimal;
  }

  ReaderOptions& ReaderOptions::setErrorStream(std::ostream& stream) {
    privateBits->errorStream = &stream;
    return *this;
  }

  std::ostream* ReaderOptions::getErrorStream() const {
    return privateBits->errorStream;
  }

  std::string ReaderOptions::getSerializedFileTail() const {
    return privateBits->serializedTail;
  }

  StripeInformation::~StripeInformation() {

  }

  class ColumnStatisticsImpl: public ColumnStatistics {
  private:
    uint64_t valueCount;

  public:
    ColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~ColumnStatisticsImpl();

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Column has " << valueCount << " values" << std::endl;
      return buffer.str();
    }
  };

  class BinaryColumnStatisticsImpl: public BinaryColumnStatistics {
  private:
    bool _hasTotalLength;
    uint64_t valueCount;
    uint64_t totalLength;

  public:
    BinaryColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                               bool correctStats);
    virtual ~BinaryColumnStatisticsImpl();

    bool hasTotalLength() const override {
      return _hasTotalLength;
    }
    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    uint64_t getTotalLength() const override {
      if(_hasTotalLength){
        return totalLength;
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Binary" << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasTotalLength){
        buffer << "Total length: " << totalLength << std::endl;
      }else{
        buffer << "Total length: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class BooleanColumnStatisticsImpl: public BooleanColumnStatistics {
  private:
    bool _hasCount;
    uint64_t valueCount;
    uint64_t trueCount;

  public:
    BooleanColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~BooleanColumnStatisticsImpl();

    bool hasCount() const override {
      return _hasCount;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    uint64_t getFalseCount() const override {
      if(_hasCount){
        return valueCount - trueCount;
      }else{
        throw ParseError("False count is not defined.");
      }
    }

    uint64_t getTrueCount() const override {
      if(_hasCount){
        return trueCount;
      }else{
        throw ParseError("True count is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Boolean" << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasCount){
        buffer << "(true: " << trueCount << "; false: "
               << valueCount - trueCount << ")" << std::endl;
      } else {
        buffer << "(true: not defined; false: not defined)" << std::endl;
        buffer << "True and false count are not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DateColumnStatisticsImpl: public DateColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    uint64_t valueCount;
    int32_t minimum;
    int32_t maximum;

  public:
    DateColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~DateColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    int32_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int32_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Date" << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DecimalColumnStatisticsImpl: public DecimalColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    std::string minimum;
    std::string maximum;
    std::string sum;

  public:
    DecimalColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~DecimalColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    Decimal getMinimum() const override {
      if(_hasMinimum){
        return Decimal(minimum);
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    Decimal getMaximum() const override {
      if(_hasMaximum){
        return Decimal(maximum);
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    Decimal getSum() const override {
      if(_hasSum){
        return Decimal(sum);
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Decimal" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }

      return buffer.str();
    }
  };

  class DoubleColumnStatisticsImpl: public DoubleColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    double minimum;
    double maximum;
    double sum;

  public:
    DoubleColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~DoubleColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    double getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    double getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    double getSum() const override {
      if(_hasSum){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Double" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class IntegerColumnStatisticsImpl: public IntegerColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    int64_t minimum;
    int64_t maximum;
    int64_t sum;

  public:
    IntegerColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~IntegerColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    int64_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    int64_t getSum() const override {
      if(_hasSum){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Integer" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class StringColumnStatisticsImpl: public StringColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasTotalLength;
    uint64_t valueCount;
    std::string minimum;
    std::string maximum;
    uint64_t totalLength;

  public:
    StringColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~StringColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasTotalLength() const override {
      return _hasTotalLength;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    std::string getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    std::string getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    uint64_t getTotalLength() const override {
      if(_hasTotalLength){
        return totalLength;
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: String" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }

      if(_hasTotalLength){
        buffer << "Total length: " << totalLength << std::endl;
      }else{
        buffer << "Total length is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class TimestampColumnStatisticsImpl: public TimestampColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    uint64_t valueCount;
    int64_t minimum;
    int64_t maximum;

  public:
    TimestampColumnStatisticsImpl(const proto::ColumnStatistics& stats,
                                  bool correctStats);
    virtual ~TimestampColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    int64_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Timestamp" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class StripeInformationImpl : public StripeInformation {
    uint64_t offset;
    uint64_t indexLength;
    uint64_t dataLength;
    uint64_t footerLength;
    uint64_t numRows;

  public:

    StripeInformationImpl(uint64_t _offset,
                          uint64_t _indexLength,
                          uint64_t _dataLength,
                          uint64_t _footerLength,
                          uint64_t _numRows) :
      offset(_offset),
      indexLength(_indexLength),
      dataLength(_dataLength),
      footerLength(_footerLength),
      numRows(_numRows)
    {}

    virtual ~StripeInformationImpl();

    uint64_t getOffset() const override {
      return offset;
    }

    uint64_t getLength() const override {
      return indexLength + dataLength + footerLength;
    }
    uint64_t getIndexLength() const override {
      return indexLength;
    }

    uint64_t getDataLength()const override {
      return dataLength;
    }

    uint64_t getFooterLength() const override {
      return footerLength;
    }

    uint64_t getNumberOfRows() const override {
      return numRows;
    }
  };

  ColumnStatistics* convertColumnStatistics(const proto::ColumnStatistics& s,
                                            bool correctStats) {
    if (s.has_intstatistics()) {
      return new IntegerColumnStatisticsImpl(s);
    } else if (s.has_doublestatistics()) {
      return new DoubleColumnStatisticsImpl(s);
    } else if (s.has_stringstatistics()) {
      return new StringColumnStatisticsImpl(s, correctStats);
    } else if (s.has_bucketstatistics()) {
      return new BooleanColumnStatisticsImpl(s, correctStats);
    } else if (s.has_decimalstatistics()) {
      return new DecimalColumnStatisticsImpl(s, correctStats);
    } else if (s.has_timestampstatistics()) {
      return new TimestampColumnStatisticsImpl(s, correctStats);
    } else if (s.has_datestatistics()) {
      return new DateColumnStatisticsImpl(s, correctStats);
    } else if (s.has_binarystatistics()) {
      return new BinaryColumnStatisticsImpl(s, correctStats);
    } else {
      return new ColumnStatisticsImpl(s);
    }
  }

  Statistics::~Statistics() {
    // PASS
  }

  class StatisticsImpl: public Statistics {
  private:
    std::list<ColumnStatistics*> colStats;

    // DELIBERATELY NOT IMPLEMENTED
    StatisticsImpl(const StatisticsImpl&);
    StatisticsImpl& operator=(const StatisticsImpl&);

  public:
    StatisticsImpl(const proto::StripeStatistics& stripeStats, bool correctStats) {
      for(int i = 0; i < stripeStats.colstats_size(); i++) {
        colStats.push_back(convertColumnStatistics
                           (stripeStats.colstats(i), correctStats));
      }
    }

    StatisticsImpl(const proto::Footer& footer, bool correctStats) {
      for(int i = 0; i < footer.statistics_size(); i++) {
        colStats.push_back(convertColumnStatistics
                           (footer.statistics(i), correctStats));
      }
    }

    virtual const ColumnStatistics* getColumnStatistics(uint32_t columnId
                                                        ) const override {
      std::list<ColumnStatistics*>::const_iterator it = colStats.begin();
      std::advance(it, static_cast<int64_t>(columnId));
      return *it;
    }

    virtual ~StatisticsImpl();

    uint32_t getNumberOfColumns() const override {
      return static_cast<uint32_t>(colStats.size());
    }
  };

  StatisticsImpl::~StatisticsImpl() {
    for(std::list<ColumnStatistics*>::iterator ptr = colStats.begin();
        ptr != colStats.end();
        ++ptr) {
      delete *ptr;
    }
  }

  Reader::~Reader() {
    // PASS
  }

  StripeInformationImpl::~StripeInformationImpl() {
    // PASS
  }

  static const uint64_t DIRECTORY_SIZE_GUESS = 16 * 1024;

  class ReaderImpl : public Reader {
  private:
    const int64_t epochOffset;

    // inputs
    std::unique_ptr<InputStream> stream;
    ReaderOptions options;
    const uint64_t footerStart;
    std::vector<bool> selectedColumns;

    // custom memory pool
    MemoryPool& memoryPool;

    // postscript
    std::unique_ptr<proto::PostScript> postscript;
    const uint64_t blockSize;
    const CompressionKind compression;

    // footer
    std::unique_ptr<proto::Footer> footer;
    DataBuffer<uint64_t> firstRowOfStripe;
    uint64_t numberOfStripes;
    std::unique_ptr<Type> schema;
    mutable std::unique_ptr<Type> selectedSchema;

    // metadata
    mutable std::unique_ptr<proto::Metadata> metadata;
    mutable bool isMetadataLoaded;

    // reading state
    uint64_t previousRow;
    uint64_t firstStripe;
    uint64_t currentStripe;
    uint64_t lastStripe; // the stripe AFTER the last one
    uint64_t currentRowInStripe;
    uint64_t rowsInCurrentStripe;
    proto::StripeInformation currentStripeInfo;
    proto::StripeFooter currentStripeFooter;
    std::unique_ptr<ColumnReader> reader;

    // internal methods
    proto::StripeFooter getStripeFooter(const proto::StripeInformation& info);
    void startNextStripe();
    void checkOrcVersion();
    void selectType(const Type& type);
    void readMetadata() const;
    void updateSelected(const std::list<uint64_t>& fieldIds);
    void updateSelected(const std::list<std::string>& fieldNames);

  public:
    /**
     * Constructor that lets the user specify additional options.
     * @param stream the stream to read from
     * @param options options for reading
     * @param postscript the postscript for the file
     * @param footer the footer for the file
     * @param footerStart the byte offset of the start of the footer
     */
    ReaderImpl(std::unique_ptr<InputStream> stream,
               const ReaderOptions& options,
               std::unique_ptr<proto::PostScript> postscript,
               std::unique_ptr<proto::Footer> footer,
               uint64_t footerStart);

    const ReaderOptions& getReaderOptions() const;

    CompressionKind getCompression() const override;

    std::string getFormatVersion() const override;

    uint64_t getNumberOfRows() const override;

    uint64_t getRowIndexStride() const override;

    const std::string& getStreamName() const override;

    std::list<std::string> getMetadataKeys() const override;

    std::string getMetadataValue(const std::string& key) const override;

    bool hasMetadataValue(const std::string& key) const override;

    uint64_t getCompressionSize() const override;

    uint64_t getNumberOfStripes() const override;

    std::unique_ptr<StripeInformation> getStripe(uint64_t
                                                 ) const override;

    uint64_t getNumberOfStripeStatistics() const override;

    std::unique_ptr<Statistics>
    getStripeStatistics(uint64_t stripeIndex) const override;


    uint64_t getContentLength() const override;

    std::unique_ptr<Statistics> getStatistics() const override;

    std::unique_ptr<ColumnStatistics> getColumnStatistics(uint32_t columnId
                                                          ) const override;

    const Type& getType() const override;

    const Type& getSelectedType() const override;

    const std::vector<bool> getSelectedColumns() const override;

    std::unique_ptr<ColumnVectorBatch> createRowBatch(uint64_t size
                                                      ) const override;

    bool next(ColumnVectorBatch& data) override;

    uint64_t getRowNumber() const override;

    void seekToRow(uint64_t rowNumber) override;

    MemoryPool* getMemoryPool() const ;

    bool hasCorrectStatistics() const override;

    std::string getSerializedFileTail() const override;

    uint64_t getMemoryUse(int stripeIx = -1) override;
  };

  InputStream::~InputStream() {
    // PASS
  };

  uint64_t getCompressionBlockSize(const proto::PostScript& ps) {
    if (ps.has_compressionblocksize()) {
      return ps.compressionblocksize();
    } else {
      return 256 * 1024;
    }
  }

  CompressionKind convertCompressionKind(const proto::PostScript& ps) {
    if (ps.has_compression()) {
      return static_cast<CompressionKind>(ps.compression());
    } else {
      throw ParseError("Unknown compression type");
    }
  }

  int64_t getEpochOffset() {
    // Build the literal for the ORC epoch
    // 2015 Jan 1 00:00:00
    struct tm epoch;
    epoch.tm_sec = 0;
    epoch.tm_min = 0;
    epoch.tm_hour = 0;
    epoch.tm_mday = 1;
    epoch.tm_mon = 0;
    epoch.tm_year = 2015 - 1900;
    epoch.tm_isdst = 0;
    return static_cast<int64_t>(mktime(&epoch));
  }

  ReaderImpl::ReaderImpl(std::unique_ptr<InputStream> input,
                         const ReaderOptions& opts,
                         std::unique_ptr<proto::PostScript> _postscript,
                         std::unique_ptr<proto::Footer> _footer,
                         uint64_t _footerStart
                         ): epochOffset(getEpochOffset()),
                            stream(std::move(input)),
                            options(opts),
                            footerStart(_footerStart),
                            memoryPool(*opts.getMemoryPool()),
                            postscript(std::move(_postscript)),
                            blockSize(getCompressionBlockSize(*postscript)),
                            compression(convertCompressionKind(*postscript)),
                            footer(std::move(_footer)),
                            firstRowOfStripe(memoryPool, 0) {
    isMetadataLoaded = false;
    checkOrcVersion();
    numberOfStripes = static_cast<uint64_t>(footer->stripes_size());
    currentStripe = static_cast<uint64_t>(footer->stripes_size());
    lastStripe = 0;
    currentRowInStripe = 0;
    uint64_t rowTotal = 0;

    firstRowOfStripe.resize(static_cast<uint64_t>(footer->stripes_size()));
    for(size_t i=0; i < static_cast<size_t>(footer->stripes_size()); ++i) {
      firstRowOfStripe[i] = rowTotal;
      proto::StripeInformation stripeInfo =
        footer->stripes(static_cast<int>(i));
      rowTotal += stripeInfo.numberofrows();
      bool isStripeInRange = stripeInfo.offset() >= opts.getOffset() &&
        stripeInfo.offset() < opts.getOffset() + opts.getLength();
      if (isStripeInRange) {
        if (i < currentStripe) {
          currentStripe = i;
        }
        if (i >= lastStripe) {
          lastStripe = i + 1;
        }
      }
    }
    firstStripe = currentStripe;

    if (currentStripe == 0) {
      previousRow = (std::numeric_limits<uint64_t>::max)();
    } else if (currentStripe ==
               static_cast<uint64_t>(footer->stripes_size())) {
      previousRow = footer->numberofrows();
    } else {
      previousRow = firstRowOfStripe[firstStripe]-1;
    }

    schema = convertType(footer->types(0), *footer);

    selectedColumns.assign(static_cast<size_t>(footer->types_size()), false);
    if (schema->getKind() == STRUCT && options.getIndexesSet()) {
      updateSelected(options.getInclude());
    } else if (schema->getKind() == STRUCT && options.getNamesSet()) {
      updateSelected(options.getIncludeNames());
    } else {
      std::fill(selectedColumns.begin(), selectedColumns.end(), true);
    }
    selectedColumns[0] = true;
  }

  void ReaderImpl::selectType(const Type& type) {
    if (!selectedColumns[static_cast<size_t>(type.getColumnId())]) {
      selectedColumns[static_cast<size_t>(type.getColumnId())] = true;
      for (uint64_t i=0; i < type.getSubtypeCount(); i++) {
        selectType(*type.getSubtype(i));
      }
    }
  }

  std::string ReaderImpl::getSerializedFileTail() const {
    proto::FileTail tail;
    proto::PostScript *mutable_ps = tail.mutable_postscript();
    mutable_ps->CopyFrom(*postscript);
    proto::Footer *mutableFooter = tail.mutable_footer();
    mutableFooter->CopyFrom(*footer);
    tail.set_footerstart(footerStart);
    std::string result;
    if (!tail.SerializeToString(&result)) {
      throw ParseError("Failed to serialize file tail");
    }
    return result;
  }

  const ReaderOptions& ReaderImpl::getReaderOptions() const {
    return options;
  }

  CompressionKind ReaderImpl::getCompression() const {
    return compression;
  }

  uint64_t ReaderImpl::getCompressionSize() const {
    return blockSize;
  }

  uint64_t ReaderImpl::getNumberOfStripes() const {
    return numberOfStripes;
  }

  uint64_t ReaderImpl::getNumberOfStripeStatistics() const {
    if (!isMetadataLoaded) {
      readMetadata();
    }
    return metadata.get() == nullptr ? 0 :
      static_cast<uint64_t>(metadata->stripestats_size());
  }

  std::unique_ptr<StripeInformation>
  ReaderImpl::getStripe(uint64_t stripeIndex) const {
    if (stripeIndex > getNumberOfStripes()) {
      throw std::logic_error("stripe index out of range");
    }
    proto::StripeInformation stripeInfo =
      footer->stripes(static_cast<int>(stripeIndex));

    return std::unique_ptr<StripeInformation>
      (new StripeInformationImpl
       (stripeInfo.offset(),
        stripeInfo.indexlength(),
        stripeInfo.datalength(),
        stripeInfo.footerlength(),
        stripeInfo.numberofrows()));
  }

  std::string ReaderImpl::getFormatVersion() const {
    std::stringstream result;
    for(int i=0; i < postscript->version_size(); ++i) {
      if (i != 0) {
        result << ".";
      }
      result << postscript->version(i);
    }
    return result.str();
  }

  uint64_t ReaderImpl::getNumberOfRows() const {
    return footer->numberofrows();
  }

  uint64_t ReaderImpl::getContentLength() const {
    return footer->contentlength();
  }

  uint64_t ReaderImpl::getRowIndexStride() const {
    return footer->rowindexstride();
  }

  const std::string& ReaderImpl::getStreamName() const {
    return stream->getName();
  }

  std::list<std::string> ReaderImpl::getMetadataKeys() const {
    std::list<std::string> result;
    for(int i=0; i < footer->metadata_size(); ++i) {
      result.push_back(footer->metadata(i).name());
    }
    return result;
  }

  std::string ReaderImpl::getMetadataValue(const std::string& key) const {
    for(int i=0; i < footer->metadata_size(); ++i) {
      if (footer->metadata(i).name() == key) {
        return footer->metadata(i).value();
      }
    }
    throw std::range_error("key not found");
  }

  bool ReaderImpl::hasMetadataValue(const std::string& key) const {
    for(int i=0; i < footer->metadata_size(); ++i) {
      if (footer->metadata(i).name() == key) {
        return true;
      }
    }
    return false;
  }

  const std::vector<bool> ReaderImpl::getSelectedColumns() const {
    return selectedColumns;
  }

  const Type& ReaderImpl::getType() const {
    return *(schema.get());
  }

  const Type& ReaderImpl::getSelectedType() const {
    if (selectedSchema.get() == nullptr) {
      selectedSchema = buildSelectedType(schema.get(),
                                         selectedColumns);
    }
    return *(selectedSchema.get());
  }

  uint64_t ReaderImpl::getRowNumber() const {
    return previousRow;
  }

  std::unique_ptr<Statistics> ReaderImpl::getStatistics() const {
    return std::unique_ptr<Statistics>
      (new StatisticsImpl(*footer,
                          hasCorrectStatistics()));
  }

  std::unique_ptr<ColumnStatistics>
  ReaderImpl::getColumnStatistics(uint32_t index) const {
    if (index >= static_cast<uint64_t>(footer->statistics_size())) {
      throw std::logic_error("column index out of range");
    }
    proto::ColumnStatistics col =
      footer->statistics(static_cast<int32_t>(index));
    return std::unique_ptr<ColumnStatistics> (convertColumnStatistics
                                              (col, hasCorrectStatistics()));
  }

  void ReaderImpl::readMetadata() const {
    uint64_t metadataSize = postscript->metadatalength();
    uint64_t metadataStart = footerStart - metadataSize;
    if (metadataSize != 0) {
      std::unique_ptr<SeekableInputStream> pbStream =
        createDecompressor(compression,
                           std::unique_ptr<SeekableInputStream>
                             (new SeekableFileInputStream(stream.get(),
                                                          metadataStart,
                                                          metadataSize,
                                                          memoryPool)),
                           blockSize,
                           memoryPool);
      metadata.reset(new proto::Metadata());
      if (!metadata->ParseFromZeroCopyStream(pbStream.get())) {
        throw ParseError("Failed to parse the metadata");
      }
    }
    isMetadataLoaded = true;
  }

  std::unique_ptr<Statistics>
  ReaderImpl::getStripeStatistics(uint64_t stripeIndex) const {
    if (!isMetadataLoaded) {
      readMetadata();
    }
    if (metadata.get() == nullptr) {
      throw std::logic_error("No stripe statistics in file");
    }
    return std::unique_ptr<Statistics>
      (new StatisticsImpl(metadata->stripestats
                          (static_cast<int>(stripeIndex)),
                          hasCorrectStatistics()));
  }


  void ReaderImpl::seekToRow(uint64_t rowNumber) {
    // Empty file
    if (lastStripe == 0) {
      return;
    }

    // If we are reading only a portion of the file
    // (bounded by firstStripe and lastStripe),
    // seeking before or after the portion of interest should return no data.
    // Implement this by setting previousRow to the number of rows in the file.

    // seeking past lastStripe
    if ( (lastStripe == static_cast<uint64_t>(footer->stripes_size())
            && rowNumber >= footer->numberofrows())  ||
         (lastStripe < static_cast<uint64_t>(footer->stripes_size())
            && rowNumber >= firstRowOfStripe[lastStripe])   ) {
      currentStripe = static_cast<uint64_t>(footer->stripes_size());
      previousRow = footer->numberofrows();
      return;
    }

    uint64_t seekToStripe = 0;
    while (seekToStripe+1 < lastStripe &&
                  firstRowOfStripe[seekToStripe+1] <= rowNumber) {
      seekToStripe++;
    }

    // seeking before the first stripe
    if (seekToStripe < firstStripe) {
      currentStripe = static_cast<uint64_t>(footer->stripes_size());
      previousRow = footer->numberofrows();
      return;
    }

    currentStripe = seekToStripe;
    currentRowInStripe = rowNumber - firstRowOfStripe[currentStripe];
    previousRow = rowNumber;
    startNextStripe();
    reader->skip(currentRowInStripe);
  }

  bool ReaderImpl::hasCorrectStatistics() const {
    return postscript->has_writerversion() && postscript->writerversion();
  }

  proto::StripeFooter ReaderImpl::getStripeFooter
       (const proto::StripeInformation& info) {
    uint64_t stripeFooterStart = info.offset() + info.indexlength() +
      info.datalength();
    uint64_t stripeFooterLength = info.footerlength();
    std::unique_ptr<SeekableInputStream> pbStream =
      createDecompressor(compression,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableFileInputStream(stream.get(),
                                                      stripeFooterStart,
                                                      stripeFooterLength,
                                                      memoryPool)),
                         blockSize,
                         memoryPool);
    proto::StripeFooter result;
    if (!result.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError(std::string("bad StripeFooter from ") +
                       pbStream->getName());
    }
    return result;
  }

  class StripeStreamsImpl: public StripeStreams {
  private:
    const ReaderImpl& reader;
    const proto::StripeFooter& footer;
    const uint64_t stripeStart;
    InputStream& input;
    MemoryPool& memoryPool;
    const int64_t epochOffset;

  public:
    StripeStreamsImpl(const ReaderImpl& reader,
                      const proto::StripeFooter& footer,
                      uint64_t stripeStart,
                      InputStream& input,
                      MemoryPool& memoryPool,
                      int64_t epochOffset);

    virtual ~StripeStreamsImpl();

    virtual const ReaderOptions& getReaderOptions() const override;

    virtual const std::vector<bool> getSelectedColumns() const override;

    virtual proto::ColumnEncoding getEncoding(uint64_t columnId
                                              ) const override;

    virtual std::unique_ptr<SeekableInputStream>
    getStream(uint64_t columnId,
              proto::Stream_Kind kind,
              bool shouldStream) const override;

    MemoryPool& getMemoryPool() const override;

    int64_t getEpochOffset() const override;
  };

  uint64_t maxStreamsForType(const proto::Type& type) {
    switch (static_cast<int64_t>(type.kind())) {
      case proto::Type_Kind_STRUCT:
        return 1;
      case proto::Type_Kind_INT:
      case proto::Type_Kind_LONG:
      case proto::Type_Kind_SHORT:
      case proto::Type_Kind_FLOAT:
      case proto::Type_Kind_DOUBLE:
      case proto::Type_Kind_BOOLEAN:
      case proto::Type_Kind_BYTE:
      case proto::Type_Kind_DATE:
      case proto::Type_Kind_LIST:
      case proto::Type_Kind_MAP:
      case proto::Type_Kind_UNION:
        return 2;
      case proto::Type_Kind_BINARY:
      case proto::Type_Kind_DECIMAL:
      case proto::Type_Kind_TIMESTAMP:
        return 3;
      case proto::Type_Kind_CHAR:
      case proto::Type_Kind_STRING:
      case proto::Type_Kind_VARCHAR:
        return 4;
      default:
          return 0;
      }
  }

  uint64_t ReaderImpl::getMemoryUse(int stripeIx) {
    uint64_t maxDataLength = 0;

    if (stripeIx >= 0 && stripeIx < footer->stripes_size()) {
      uint64_t stripe = footer->stripes(stripeIx).datalength();
      if (maxDataLength < stripe) {
        maxDataLength = stripe;
      }
    } else {
      for (int i=0; i < footer->stripes_size(); i++) {
        uint64_t stripe = footer->stripes(i).datalength();
        if (maxDataLength < stripe) {
          maxDataLength = stripe;
        }
      }
    }

    bool hasStringColumn = false;
    uint64_t nSelectedStreams = 0;
    for (int i=0; !hasStringColumn && i < footer->types_size(); i++) {
      if (selectedColumns[static_cast<size_t>(i)]) {
        const proto::Type& type = footer->types(i);
        nSelectedStreams += maxStreamsForType(type) ;
        switch (static_cast<int64_t>(type.kind())) {
          case proto::Type_Kind_CHAR:
          case proto::Type_Kind_STRING:
          case proto::Type_Kind_VARCHAR:
          case proto::Type_Kind_BINARY: {
            hasStringColumn = true;
            break;
          }
          default: {
            break;
          }
        }
      }
    }

    /* If a string column is read, use stripe datalength as a memory estimate
     * because we don't know the dictionary size. Multiply by 2 because
     * a string column requires two buffers:
     * in the input stream and in the seekable input stream.
     * If no string column is read, estimate from the number of streams.
     */
    uint64_t memory = hasStringColumn ? 2 * maxDataLength :
        std::min(uint64_t(maxDataLength),
                 nSelectedStreams * stream->getNaturalReadSize());

    // Do we need even more memory to read the footer or the metadata?
    if (memory < postscript->footerlength() + DIRECTORY_SIZE_GUESS) {
      memory =  postscript->footerlength() + DIRECTORY_SIZE_GUESS;
    }
    if (memory < postscript->metadatalength()) {
      memory =  postscript->metadatalength();
    }

    // Account for firstRowOfStripe.
    memory += firstRowOfStripe.capacity() * sizeof(uint64_t);

    // Decompressors need buffers for each stream
    uint64_t decompressorMemory = 0;
    if (compression != CompressionKind_NONE) {
      for (int i=0; i < footer->types_size(); i++) {
        if (selectedColumns[static_cast<size_t>(i)]) {
          const proto::Type& type = footer->types(i);
          decompressorMemory += maxStreamsForType(type) * blockSize;
        }
      }
      if (compression == CompressionKind_SNAPPY) {
        decompressorMemory *= 2;  // Snappy decompressor uses a second buffer
      }
    }

    return memory + decompressorMemory ;
  }

  StripeStreamsImpl::StripeStreamsImpl(const ReaderImpl& _reader,
                                       const proto::StripeFooter& _footer,
                                       uint64_t _stripeStart,
                                       InputStream& _input,
                                       MemoryPool& _memoryPool,
                                       int64_t _epochOffset
                                       ): reader(_reader),
                                          footer(_footer),
                                          stripeStart(_stripeStart),
                                          input(_input),
                                          memoryPool(_memoryPool),
                                          epochOffset(_epochOffset) {
    // PASS
  }

  StripeStreamsImpl::~StripeStreamsImpl() {
    // PASS
  }

  const ReaderOptions& StripeStreamsImpl::getReaderOptions() const {
    return reader.getReaderOptions();
  }

  const std::vector<bool> StripeStreamsImpl::getSelectedColumns() const {
    return reader.getSelectedColumns();
  }

  proto::ColumnEncoding StripeStreamsImpl::getEncoding(uint64_t columnId
                                                       ) const {
    return footer.columns(static_cast<int>(columnId));
  }

  int64_t StripeStreamsImpl::getEpochOffset() const {
    return epochOffset;
  }

  std::unique_ptr<SeekableInputStream>
  StripeStreamsImpl::getStream(uint64_t columnId,
                               proto::Stream_Kind kind,
                               bool shouldStream) const {
    uint64_t offset = stripeStart;
    for(int i = 0; i < footer.streams_size(); ++i) {
      const proto::Stream& stream = footer.streams(i);
      if (stream.has_kind() &&
          stream.kind() == kind &&
          stream.column() == static_cast<uint64_t>(columnId)) {
        uint64_t myBlock = shouldStream ? input.getNaturalReadSize():
          stream.length();
        return createDecompressor(reader.getCompression(),
                                  std::unique_ptr<SeekableInputStream>
                                  (new SeekableFileInputStream
                                   (&input,
                                    offset,
                                    stream.length(),
                                    memoryPool,
                                    myBlock)),
                                  reader.getCompressionSize(),
                                  memoryPool);
      }
      offset += stream.length();
    }
    return std::unique_ptr<SeekableInputStream>();
  }

  MemoryPool& StripeStreamsImpl::getMemoryPool() const {
    return memoryPool;
  }

  void ReaderImpl::startNextStripe() {
    reader.reset(); // ColumnReaders use lots of memory; free old memory first
    currentStripeInfo = footer->stripes(static_cast<int>(currentStripe));
    currentStripeFooter = getStripeFooter(currentStripeInfo);
    rowsInCurrentStripe = currentStripeInfo.numberofrows();
    StripeStreamsImpl stripeStreams(*this, currentStripeFooter,
                                    currentStripeInfo.offset(),
                                    *(stream.get()),
                                    memoryPool,
                                    epochOffset);
    reader = buildReader(*(schema.get()), stripeStreams);
  }

  void ReaderImpl::checkOrcVersion() {
    std::string version = getFormatVersion();
    if (version != "0.11" && version != "0.12") {
      *(options.getErrorStream())
        << "Warning: ORC file " << stream->getName()
        << " was written in an unknown format version "
        << version << "\n";
    }
  }

  bool ReaderImpl::next(ColumnVectorBatch& data) {
    if (currentStripe >= lastStripe) {
      data.numElements = 0;
      if (lastStripe > 0) {
        previousRow = firstRowOfStripe[lastStripe - 1] +
          footer->stripes(static_cast<int>(lastStripe - 1)).numberofrows();
      } else {
        previousRow = 0;
      }
      return false;
    }
    if (currentRowInStripe == 0) {
      startNextStripe();
    }
    uint64_t rowsToRead =
      std::min(static_cast<uint64_t>(data.capacity),
               rowsInCurrentStripe - currentRowInStripe);
    data.numElements = rowsToRead;
    reader->next(data, rowsToRead, 0);
    // update row number
    previousRow = firstRowOfStripe[currentStripe] + currentRowInStripe;
    currentRowInStripe += rowsToRead;
    if (currentRowInStripe >= rowsInCurrentStripe) {
      currentStripe += 1;
      currentRowInStripe = 0;
    }
    return rowsToRead != 0;
  }

  std::unique_ptr<ColumnVectorBatch> ReaderImpl::createRowBatch
                                              (uint64_t capacity) const {
    return getSelectedType().createRowBatch(capacity, memoryPool);
  }

  void ensureOrcFooter(InputStream* stream,
                       DataBuffer<char> *buffer,
                       uint64_t postscriptLength) {

    const std::string MAGIC("ORC");
    const uint64_t magicLength = MAGIC.length();
    const char * const bufferStart = buffer->data();
    const uint64_t bufferLength = buffer->size();

    if (postscriptLength < magicLength || bufferLength < magicLength) {
      throw ParseError("Invalid ORC postscript length");
    }
    const char* magicStart = bufferStart + bufferLength - 1 - magicLength;

    // Look for the magic string at the end of the postscript.
    if (memcmp(magicStart, MAGIC.c_str(), magicLength) != 0) {
      // If there is no magic string at the end, check the beginning.
      // Only files written by Hive 0.11.0 don't have the tail ORC string.
      char *frontBuffer = new char[magicLength];
      stream->read(frontBuffer, magicLength, 0);
      bool foundMatch = memcmp(frontBuffer, MAGIC.c_str(), magicLength) == 0;
      delete[] frontBuffer;
      if (!foundMatch) {
        throw ParseError("Not an ORC file");
      }
    }
  }

  /**
   * Read the file's postscript from the given buffer.
   * @param stream the file stream
   * @param buffer the buffer with the tail of the file.
   * @param postscriptSize the length of postscript in bytes
   */
  std::unique_ptr<proto::PostScript> readPostscript(InputStream *stream,
                                                    DataBuffer<char> *buffer,
                                                    uint64_t postscriptSize) {
    char *ptr = buffer->data();
    uint64_t readSize = buffer->size();

    ensureOrcFooter(stream, buffer, postscriptSize);

    std::unique_ptr<proto::PostScript> postscript =
      std::unique_ptr<proto::PostScript>(new proto::PostScript());
    if (!postscript->ParseFromArray(ptr + readSize - 1 - postscriptSize,
                                   static_cast<int>(postscriptSize))) {
      throw ParseError("Failed to parse the postscript from " +
                       stream->getName());
    }
    return std::move(postscript);
  }

  /**
   * Parse the footer from the given buffer.
   * @param stream the file's stream
   * @param buffer the buffer to parse the footer from
   * @param footerOffset the offset within the buffer that contains the footer
   * @param ps the file's postscript
   * @param memoryPool the memory pool to use
   */
  std::unique_ptr<proto::Footer> readFooter(InputStream* stream,
                                            DataBuffer<char> *&buffer,
                                            uint64_t footerOffset,
                                            const proto::PostScript& ps,
                                            MemoryPool& memoryPool) {
    char *footerPtr = buffer->data() + footerOffset;

    std::unique_ptr<SeekableInputStream> pbStream =
      createDecompressor(convertCompressionKind(ps),
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableArrayInputStream(footerPtr,
                                                       ps.footerlength())),
                         getCompressionBlockSize(ps),
                         memoryPool);

    std::unique_ptr<proto::Footer> footer =
      std::unique_ptr<proto::Footer>(new proto::Footer());
    if (!footer->ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError("Failed to parse the footer from " +
                       stream->getName());
    }
    return std::move(footer);
  }

  std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream,
                                       const ReaderOptions& options) {
    MemoryPool *memoryPool = options.getMemoryPool();
    std::unique_ptr<proto::PostScript> ps;
    std::unique_ptr<proto::Footer> footer;
    uint64_t footerStart;
    std::string serializedFooter = options.getSerializedFileTail();
    if (serializedFooter.length() != 0) {
      // Parse the file tail from the serialized one.
      proto::FileTail tail;
      if (!tail.ParseFromString(serializedFooter)) {
        throw ParseError("Failed to parse the file tail from string");
      }
      ps.reset(new proto::PostScript(tail.postscript()));
      footer.reset(new proto::Footer(tail.footer()));
      footerStart = tail.footerstart();
    } else {
      // figure out the size of the file using the option or filesystem
      uint64_t size = std::min(options.getTailLocation(),
                               static_cast<uint64_t>(stream->getLength()));

      //read last bytes into buffer to get PostScript
      uint64_t readSize = std::min(size, DIRECTORY_SIZE_GUESS);
      if (readSize < 4) {
        throw ParseError("File size too small");
      }
      DataBuffer<char> *buffer = new DataBuffer<char>(*memoryPool, readSize);
      stream->read(buffer->data(), readSize, size - readSize);

      uint64_t postscriptSize = buffer->data()[readSize - 1] & 0xff;
      ps = readPostscript(stream.get(), buffer, postscriptSize);
      uint64_t footerSize = ps->footerlength();
      uint64_t tailSize = 1 + postscriptSize + footerSize;
      footerStart = size - tailSize;
      uint64_t footerOffset;

      if (tailSize > readSize) {
        buffer->resize(footerSize);
        stream->read(buffer->data(), footerSize, size - tailSize);
        footerOffset = 0;
      } else {
        footerOffset = readSize - tailSize;
      }

      footer = readFooter(stream.get(), buffer, footerOffset, *ps,
                          *memoryPool);
      delete buffer;
    }
    return std::unique_ptr<Reader>(new ReaderImpl(std::move(stream),
                                                  options,
                                                  std::move(ps),
                                                  std::move(footer),
                                                  footerStart));
  }

  ColumnStatistics::~ColumnStatistics() {
    // PASS
  }

  BinaryColumnStatistics::~BinaryColumnStatistics() {
    // PASS
  }

  BooleanColumnStatistics::~BooleanColumnStatistics() {
    // PASS
  }

  DateColumnStatistics::~DateColumnStatistics() {
    // PASS
  }

  DecimalColumnStatistics::~DecimalColumnStatistics() {
    // PASS
  }

  DoubleColumnStatistics::~DoubleColumnStatistics() {
    // PASS
  }

  IntegerColumnStatistics::~IntegerColumnStatistics() {
    // PASS
  }

  StringColumnStatistics::~StringColumnStatistics() {
    // PASS
  }

  TimestampColumnStatistics::~TimestampColumnStatistics() {
    // PASS
  }

  ColumnStatisticsImpl::~ColumnStatisticsImpl() {
    // PASS
  }

  BinaryColumnStatisticsImpl::~BinaryColumnStatisticsImpl() {
    // PASS
  }

  BooleanColumnStatisticsImpl::~BooleanColumnStatisticsImpl() {
    // PASS
  }

  DateColumnStatisticsImpl::~DateColumnStatisticsImpl() {
    // PASS
  }

  DecimalColumnStatisticsImpl::~DecimalColumnStatisticsImpl() {
    // PASS
  }

  DoubleColumnStatisticsImpl::~DoubleColumnStatisticsImpl() {
    // PASS
  }

  IntegerColumnStatisticsImpl::~IntegerColumnStatisticsImpl() {
    // PASS
  }

  StringColumnStatisticsImpl::~StringColumnStatisticsImpl() {
    // PASS
  }

  TimestampColumnStatisticsImpl::~TimestampColumnStatisticsImpl() {
    // PASS
  }

  ColumnStatisticsImpl::ColumnStatisticsImpl
  (const proto::ColumnStatistics& pb) {
    valueCount = pb.numberofvalues();
  }

  BinaryColumnStatisticsImpl::BinaryColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_binarystatistics() || !correctStats) {
      _hasTotalLength = false;

      totalLength = 0;
    }else{
      _hasTotalLength = pb.binarystatistics().has_sum();
      totalLength = static_cast<uint64_t>(pb.binarystatistics().sum());
    }
  }

  BooleanColumnStatisticsImpl::BooleanColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_bucketstatistics() || !correctStats) {
      _hasCount = false;
      trueCount = 0;
    }else{
      _hasCount = true;
      trueCount = pb.bucketstatistics().count(0);
    }
  }

  DateColumnStatisticsImpl::DateColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_datestatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;

      minimum = 0;
      maximum = 0;
    } else {
      _hasMinimum = pb.datestatistics().has_minimum();
      _hasMaximum = pb.datestatistics().has_maximum();
      minimum = pb.datestatistics().minimum();
      maximum = pb.datestatistics().maximum();
    }
  }

  DecimalColumnStatisticsImpl::DecimalColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_decimalstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;
    }else{
      const proto::DecimalStatistics& stats = pb.decimalstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  DoubleColumnStatisticsImpl::DoubleColumnStatisticsImpl
  (const proto::ColumnStatistics& pb){
    valueCount = pb.numberofvalues();
    if (!pb.has_doublestatistics()) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;

      minimum = 0;
      maximum = 0;
      sum = 0;
    }else{
      const proto::DoubleStatistics& stats = pb.doublestatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  IntegerColumnStatisticsImpl::IntegerColumnStatisticsImpl
  (const proto::ColumnStatistics& pb){
    valueCount = pb.numberofvalues();
    if (!pb.has_intstatistics()) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;

      minimum = 0;
      maximum = 0;
      sum = 0;
    }else{
      const proto::IntegerStatistics& stats = pb.intstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  StringColumnStatisticsImpl::StringColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_stringstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasTotalLength = false;
      
      totalLength = 0;
    }else{
      const proto::StringStatistics& stats = pb.stringstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasTotalLength = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      totalLength = static_cast<uint64_t>(stats.sum());
    }
  }

  TimestampColumnStatisticsImpl::TimestampColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats) {
    valueCount = pb.numberofvalues();
    if (!pb.has_timestampstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
      minimum = 0;
      maximum = 0;
    }else{
      const proto::TimestampStatistics& stats = pb.timestampstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();

      minimum = stats.minimum();
      maximum = stats.maximum();
    }
  }

  void ReaderImpl::updateSelected(const std::list<uint64_t>& fieldIds) {
    uint64_t childCount = schema->getSubtypeCount();
    for(std::list<uint64_t>::const_iterator i = fieldIds.begin();
        i != fieldIds.end(); ++i) {
      if (*i >= childCount) {
        std::stringstream buffer;
        buffer << "Invalid column selected " << *i << " out of "
               << childCount;
        throw ParseError(buffer.str());
      }
      const Type& child = *schema->getSubtype(*i);
      for(size_t c = child.getColumnId();
          c <= child.getMaximumColumnId(); ++c){
        selectedColumns[c] = true;
      }
    }
  }

  void ReaderImpl::updateSelected(const std::list<std::string>& fieldNames) {
    uint64_t childCount = schema->getSubtypeCount();
    for(std::list<std::string>::const_iterator i = fieldNames.begin();
        i != fieldNames.end(); ++i) {
      bool foundMatch = false;
      for(size_t field=0; field < childCount; ++field) {
        if (schema->getFieldName(field) == *i) {
          const Type& child = *schema->getSubtype(field);
          for(size_t c = child.getColumnId();
              c <= child.getMaximumColumnId(); ++c){
            selectedColumns[c] = true;
          }
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        throw ParseError("Invalid column selected " + *i);
      }
    }
  }
}// namespace
