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

#include "orc/Common.hh"
#include "orc/OrcFile.hh"

#include "ColumnWriter.hh"
#include "Timezone.hh"

#include <memory>

namespace orc {

  struct WriterOptionsPrivate {
    uint64_t stripeSize;
    uint64_t compressionBlockSize;
    uint64_t rowIndexStride;
    CompressionKind compression;
    CompressionStrategy compressionStrategy;
    MemoryPool* memoryPool;
    double paddingTolerance;
    std::ostream* errorStream;
    FileVersion fileVersion;
    double dictionaryKeySizeThreshold;
    bool enableIndex;

    WriterOptionsPrivate() :
                            fileVersion(0, 11) { // default to Hive_0_11
      stripeSize = 64 * 1024 * 1024; // 64M
      compressionBlockSize = 64 * 1024; // 64K
      rowIndexStride = 10000;
      compression = CompressionKind_ZLIB;
      compressionStrategy = CompressionStrategy_SPEED;
      memoryPool = getDefaultPool();
      paddingTolerance = 0.0;
      errorStream = &std::cerr;
      dictionaryKeySizeThreshold = 0.0;
      enableIndex = true;
    }
  };

  WriterOptions::WriterOptions():
    privateBits(std::unique_ptr<WriterOptionsPrivate>
                (new WriterOptionsPrivate())) {
    // PASS
  }

  WriterOptions::WriterOptions(const WriterOptions& rhs):
    privateBits(std::unique_ptr<WriterOptionsPrivate>
                (new WriterOptionsPrivate(*(rhs.privateBits.get())))) {
    // PASS
  }

  WriterOptions::WriterOptions(WriterOptions& rhs) {
    // swap privateBits with rhs
    WriterOptionsPrivate* l = privateBits.release();
    privateBits.reset(rhs.privateBits.release());
    rhs.privateBits.reset(l);
  }

  WriterOptions& WriterOptions::operator=(const WriterOptions& rhs) {
    if (this != &rhs) {
      privateBits.reset(new WriterOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
  }

  WriterOptions::~WriterOptions() {
    // PASS
  }

  WriterOptions& WriterOptions::setStripeSize(uint64_t size) {
    privateBits->stripeSize = size;
    return *this;
  }

  uint64_t WriterOptions::getStripeSize() const {
    return privateBits->stripeSize;
  }

  WriterOptions& WriterOptions::setCompressionBlockSize(uint64_t size) {
    privateBits->compressionBlockSize = size;
    return *this;
  }

  uint64_t WriterOptions::getCompressionBlockSize() const {
    return privateBits->compressionBlockSize;
  }

  WriterOptions& WriterOptions::setRowIndexStride(uint64_t stride) {
    privateBits->rowIndexStride = stride;
    privateBits->enableIndex = (stride != 0);
    return *this;
  }

  uint64_t WriterOptions::getRowIndexStride() const {
    return privateBits->rowIndexStride;
  }

  WriterOptions& WriterOptions::setDictionaryKeySizeThreshold(double val) {
    privateBits->dictionaryKeySizeThreshold = val;
    return *this;
  }

  double WriterOptions::getDictionaryKeySizeThreshold() const {
    return privateBits->dictionaryKeySizeThreshold;
  }

  WriterOptions& WriterOptions::setFileVersion(const FileVersion& version) {
    // Only Hive_0_11 version is supported currently
    if (version.getMajor() == 0 && version.getMinor() == 11) {
      privateBits->fileVersion = version;
      return *this;
    }
    throw std::logic_error("Unpoorted file version specified.");
  }

  FileVersion WriterOptions::getFileVersion() const {
    return privateBits->fileVersion;
  }

  WriterOptions& WriterOptions::setCompression(CompressionKind comp) {
    privateBits->compression = comp;
    return *this;
  }

  CompressionKind WriterOptions::getCompression() const {
    return privateBits->compression;
  }

  WriterOptions& WriterOptions::setCompressionStrategy(
    CompressionStrategy strategy) {
    privateBits->compressionStrategy = strategy;
    return *this;
  }

  CompressionStrategy WriterOptions::getCompressionStrategy() const {
    return privateBits->compressionStrategy;
  }

  WriterOptions& WriterOptions::setPaddingTolerance(double tolerance) {
    privateBits->paddingTolerance = tolerance;
    return *this;
  }

  double WriterOptions::getPaddingTolerance() const {
    return privateBits->paddingTolerance;
  }

  WriterOptions& WriterOptions::setMemoryPool(MemoryPool* memoryPool) {
    privateBits->memoryPool = memoryPool;
    return *this;
  }

  MemoryPool* WriterOptions::getMemoryPool() const {
    return privateBits->memoryPool;
  }

  WriterOptions& WriterOptions::setErrorStream(std::ostream& errStream) {
    privateBits->errorStream = &errStream;
    return *this;
  }

  std::ostream* WriterOptions::getErrorStream() const {
    return privateBits->errorStream;
  }

  bool WriterOptions::getEnableIndex() const {
    return privateBits->enableIndex;
  }

  Writer::~Writer() {
    // PASS
  }

  class WriterImpl : public Writer {
  private:
    std::unique_ptr<ColumnWriter> columnWriter;
    std::unique_ptr<BufferedOutputStream> compressionStream;
    std::unique_ptr<BufferedOutputStream> bufferedStream;
    std::unique_ptr<StreamsFactory> streamsFactory;
    OutputStream* outStream;
    WriterOptions options;
    const Type& type;
    uint64_t stripeRows, totalRows, indexRows;
    uint64_t currentOffset;
    proto::Footer fileFooter;
    proto::PostScript postScript;
    proto::StripeInformation stripeInfo;
    proto::Metadata metadata;

    static const char* magicId;
    static const WriterId writerId;

  public:
    WriterImpl(
               const Type& type,
               OutputStream* stream,
               const WriterOptions& options);

    std::unique_ptr<ColumnVectorBatch> createRowBatch(uint64_t size)
                                                            const override;

    void add(ColumnVectorBatch& rowsToAdd) override;

    void close() override;

  private:
    void init();
    void initStripe();
    void writeStripe();
    void writeMetadata();
    void writeFileFooter();
    void writePostscript();
    void buildFooterType(const Type& t, proto::Footer& footer, uint32_t& index);
    static proto::CompressionKind convertCompressionKind(
                                                  const CompressionKind& kind);
  };

  const char * WriterImpl::magicId = "ORC";

  const WriterId WriterImpl::writerId = WriterId::ORC_CPP_WRITER;

  WriterImpl::WriterImpl(
                         const Type& t,
                         OutputStream* stream,
                         const WriterOptions& opts) :
                         outStream(stream),
                         options(opts),
                         type(t) {
    streamsFactory = createStreamsFactory(options, outStream);
    columnWriter = buildWriter(type, *streamsFactory, options);
    stripeRows = totalRows = indexRows = 0;
    currentOffset = 0;

    // compression stream for stripe footer, file footer and metadata
    compressionStream = createCompressor(
                                  options.getCompression(),
                                  outStream,
                                  options.getCompressionStrategy(),
                                  1 * 1024 * 1024, // buffer capacity: 1M
                                  options.getCompressionBlockSize(),
                                  *options.getMemoryPool());

    // uncompressed stream for post script
    bufferedStream.reset(new BufferedOutputStream(
                                            *options.getMemoryPool(),
                                            outStream,
                                            1024, // buffer capacity: 1024 bytes
                                            options.getCompressionBlockSize()));

    init();
  }

  std::unique_ptr<ColumnVectorBatch> WriterImpl::createRowBatch(uint64_t size)
                                                                         const {
    return type.createRowBatch(size, *options.getMemoryPool());
  }

  void WriterImpl::add(ColumnVectorBatch& rowsToAdd) {
    if (options.getEnableIndex()) {
      uint64_t pos = 0;
      uint64_t chunkSize = 0;
      uint64_t rowIndexStride = options.getRowIndexStride();
      while (pos < rowsToAdd.numElements) {
        chunkSize = std::min(rowsToAdd.numElements - pos,
                             rowIndexStride - indexRows);
        columnWriter->add(rowsToAdd, pos, chunkSize);

        pos += chunkSize;
        indexRows += chunkSize;
        stripeRows += chunkSize;

        if (indexRows >= rowIndexStride) {
          columnWriter->createRowIndexEntry();
          indexRows = 0;
        }
      }
    } else {
      stripeRows += rowsToAdd.numElements;
      columnWriter->add(rowsToAdd, 0, rowsToAdd.numElements);
    }

    if (columnWriter->getEstimatedSize() >= options.getStripeSize()) {
      writeStripe();
    }
  }

  void WriterImpl::close() {
    if (stripeRows > 0) {
      writeStripe();
    }
    writeMetadata();
    writeFileFooter();
    writePostscript();
    outStream->close();
  }

  void WriterImpl::init() {
    // Write file header
    outStream->write(WriterImpl::magicId, strlen(WriterImpl::magicId));
    currentOffset += strlen(WriterImpl::magicId);

    // Initialize file footer
    fileFooter.set_headerlength(currentOffset);
    fileFooter.set_contentlength(0);
    fileFooter.set_numberofrows(0);
    fileFooter.set_rowindexstride(
                          static_cast<uint32_t>(options.getRowIndexStride()));
    fileFooter.set_writer(writerId);

    uint32_t index = 0;
    buildFooterType(type, fileFooter, index);

    // Initialize post script
    postScript.set_footerlength(0);
    postScript.set_compression(
                  WriterImpl::convertCompressionKind(options.getCompression()));
    postScript.set_compressionblocksize(options.getCompressionBlockSize());

    postScript.add_version(options.getFileVersion().getMajor());
    postScript.add_version(options.getFileVersion().getMinor());

    postScript.set_writerversion(WriterVersion_ORC_135);
    postScript.set_magic("ORC");

    // Initialize first stripe
    initStripe();
  }

  void WriterImpl::initStripe() {
    stripeInfo.set_offset(currentOffset);
    stripeInfo.set_indexlength(0);
    stripeInfo.set_datalength(0);
    stripeInfo.set_footerlength(0);
    stripeInfo.set_numberofrows(0);

    stripeRows = indexRows = 0;
  }

  void WriterImpl::writeStripe() {
    if (options.getEnableIndex() && indexRows != 0) {
      columnWriter->createRowIndexEntry();
      indexRows = 0;
    } else {
      columnWriter->mergeRowGroupStatsIntoStripeStats();
    }

    std::vector<proto::Stream> streams;
    // write ROW_INDEX streams
    if (options.getEnableIndex()) {
      columnWriter->writeIndex(streams);
    }
    // write streams like PRESENT, DATA, etc.
    columnWriter->flush(streams);

    // generate and write stripe footer
    proto::StripeFooter stripeFooter;
    for (uint32_t i = 0; i < streams.size(); ++i) {
      *stripeFooter.add_streams() = streams[i];
    }

    std::vector<proto::ColumnEncoding> encodings;
    columnWriter->getColumnEncoding(encodings);

    for (uint32_t i = 0; i < encodings.size(); ++i) {
      *stripeFooter.add_columns() = encodings[i];
    }

    // use GMT to guarantee TimestampVectorBatch from reader can write
    // same wall clock time
    stripeFooter.set_writertimezone("GMT");

    // add stripe statistics to metadata
    proto::StripeStatistics* stripeStats = metadata.add_stripestats();
    std::vector<proto::ColumnStatistics> colStats;
    columnWriter->getStripeStatistics(colStats);
    for (uint32_t i = 0; i != colStats.size(); ++i) {
      *stripeStats->add_colstats() = colStats[i];
    }
    // merge stripe stats into file stats and clear stripe stats
    columnWriter->mergeStripeStatsIntoFileStats();

    if (!stripeFooter.SerializeToZeroCopyStream(compressionStream.get())) {
      throw std::logic_error("Failed to write stripe footer.");
    }
    uint64_t footerLength = compressionStream->flush();

    // calculate data length and index length
    uint64_t dataLength = 0;
    uint64_t indexLength = 0;
    for (uint32_t i = 0; i < streams.size(); ++i) {
      if (streams[i].kind() == proto::Stream_Kind_ROW_INDEX) {
        indexLength += streams[i].length();
      } else {
        dataLength += streams[i].length();
      }
    }

    // update stripe info
    stripeInfo.set_indexlength(indexLength);
    stripeInfo.set_datalength(dataLength);
    stripeInfo.set_footerlength(footerLength);
    stripeInfo.set_numberofrows(stripeRows);

    *fileFooter.add_stripes() = stripeInfo;

    currentOffset = currentOffset + indexLength + dataLength + footerLength;
    totalRows += stripeRows;

    columnWriter->reset();

    initStripe();
  }

  void WriterImpl::writeMetadata() {
    if (!metadata.SerializeToZeroCopyStream(compressionStream.get())) {
      throw std::logic_error("Failed to write metadata.");
    }
    postScript.set_metadatalength(compressionStream.get()->flush());
  }

  void WriterImpl::writeFileFooter() {
    fileFooter.set_contentlength(currentOffset - fileFooter.headerlength());
    fileFooter.set_numberofrows(totalRows);

    // update file statistics
    std::vector<proto::ColumnStatistics> colStats;
    columnWriter->getFileStatistics(colStats);
    for (uint32_t i = 0; i != colStats.size(); ++i) {
      *fileFooter.add_statistics() = colStats[i];
    }

    if (!fileFooter.SerializeToZeroCopyStream(compressionStream.get())) {
      throw std::logic_error("Failed to write file footer.");
    }
    postScript.set_footerlength(compressionStream->flush());
  }

  void WriterImpl::writePostscript() {
    if (!postScript.SerializeToZeroCopyStream(bufferedStream.get())) {
      throw std::logic_error("Failed to write post script.");
    }
    unsigned char psLength =
                      static_cast<unsigned char>(bufferedStream->flush());
    outStream->write(&psLength, sizeof(unsigned char));
  }

  void WriterImpl::buildFooterType(
                                   const Type& t,
                                   proto::Footer& footer,
                                   uint32_t & index) {
    proto::Type protoType;
    protoType.set_maximumlength(static_cast<uint32_t>(t.getMaximumLength()));
    protoType.set_precision(static_cast<uint32_t>(t.getPrecision()));
    protoType.set_scale(static_cast<uint32_t>(t.getScale()));

    switch (t.getKind()) {
    case BOOLEAN: {
      protoType.set_kind(proto::Type_Kind_BOOLEAN);
      break;
    }
    case BYTE: {
      protoType.set_kind(proto::Type_Kind_BYTE);
      break;
    }
    case SHORT: {
      protoType.set_kind(proto::Type_Kind_SHORT);
      break;
    }
    case INT: {
      protoType.set_kind(proto::Type_Kind_INT);
      break;
    }
    case LONG: {
      protoType.set_kind(proto::Type_Kind_LONG);
      break;
    }
    case FLOAT: {
      protoType.set_kind(proto::Type_Kind_FLOAT);
      break;
    }
    case DOUBLE: {
      protoType.set_kind(proto::Type_Kind_DOUBLE);
      break;
    }
    case STRING: {
      protoType.set_kind(proto::Type_Kind_STRING);
      break;
    }
    case BINARY: {
      protoType.set_kind(proto::Type_Kind_BINARY);
      break;
    }
    case TIMESTAMP: {
      protoType.set_kind(proto::Type_Kind_TIMESTAMP);
      break;
    }
    case LIST: {
      protoType.set_kind(proto::Type_Kind_LIST);
      break;
    }
    case MAP: {
      protoType.set_kind(proto::Type_Kind_MAP);
      break;
    }
    case STRUCT: {
      protoType.set_kind(proto::Type_Kind_STRUCT);
      break;
    }
    case UNION: {
      protoType.set_kind(proto::Type_Kind_UNION);
      break;
    }
    case DECIMAL: {
      protoType.set_kind(proto::Type_Kind_DECIMAL);
      break;
    }
    case DATE: {
      protoType.set_kind(proto::Type_Kind_DATE);
      break;
    }
    case VARCHAR: {
      protoType.set_kind(proto::Type_Kind_VARCHAR);
      break;
    }
    case CHAR: {
      protoType.set_kind(proto::Type_Kind_CHAR);
      break;
    }
    default:
      throw std::logic_error("Unknown type.");
    }

    int pos = static_cast<int>(index);
    *footer.add_types() = protoType;

    for (uint64_t i = 0; i < t.getSubtypeCount(); ++i) {
      if (t.getKind() != LIST && t.getKind() != MAP && t.getKind() != UNION) {
        footer.mutable_types(pos)->add_fieldnames(t.getFieldName(i));
      }
      footer.mutable_types(pos)->add_subtypes(++index);
      buildFooterType(*t.getSubtype(i), footer, index);
    }
  }

  proto::CompressionKind WriterImpl::convertCompressionKind(
                                      const CompressionKind& kind) {
    return static_cast<proto::CompressionKind>(kind);
  }

  std::unique_ptr<Writer> createWriter(
                                       const Type& type,
                                       OutputStream* stream,
                                       const WriterOptions& options) {
    return std::unique_ptr<Writer>(
                                   new WriterImpl(
                                            type,
                                            stream,
                                            options));
  }

}

