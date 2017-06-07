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
#include "orc/Writer.hh"

#include "ByteRLE.hh"
#include "ColumnWriter.hh"
#include "RLE.hh"
#include "Statistics.hh"
#include "Timezone.hh"

namespace orc {
  StreamsFactory::~StreamsFactory() {
    //PASS
  }

  class StreamsFactoryImpl : public StreamsFactory {
  public:
    StreamsFactoryImpl(
                       const WriterOptions& writerOptions,
                       OutputStream* outputStream) :
                       options(writerOptions),
                       outStream(outputStream) {
                       }

    virtual std::unique_ptr<BufferedOutputStream>
                    createStream(proto::Stream_Kind kind) override;
  private:
    const WriterOptions& options;
    OutputStream* outStream;
  };

  std::unique_ptr<BufferedOutputStream> StreamsFactoryImpl::createStream(
                                                      proto::Stream_Kind) {
    // In the future, we can decide compression strategy and modifier
    // based on stream kind. But for now we just use the setting from
    // WriterOption
    return createCompressor(
                            options.getCompression(),
                            outStream,
                            options.getCompressionStrategy(),
                            options.getBufferSize(),
                            options.getBlockSize(),
                            *options.getMemoryPool());
  }

  std::unique_ptr<StreamsFactory> createStreamsFactory(
                                        const WriterOptions& options,
                                        OutputStream* outStream) {
    return std::unique_ptr<StreamsFactory>(
                                   new StreamsFactoryImpl(options, outStream));
  }

  RowIndexPositionRecorder::~RowIndexPositionRecorder() {
    // PASS
  }

  ColumnWriter::ColumnWriter(
                             const Type& type,
                             StreamsFactory& factory,
                             const WriterOptions& options) :
                                columnId(type.getColumnId()),
                                streamsFactory(factory),
                                colIndexStatistics(),
                                colStripeStatistics(),
                                colFileStatistics(),
                                enableIndex(options.getEnableIndex()),
                                enableStats(options.getEnableStats()),
                                rowIndex(),
                                rowIndexEntry(),
                                rowIndexPosition(),
                                memPool(*options.getMemoryPool()),
                                indexStream() {

    std::unique_ptr<BufferedOutputStream> presentStream =
        factory.createStream(proto::Stream_Kind_PRESENT);
    notNullEncoder = createBooleanRleEncoder(std::move(presentStream));

    if (enableIndex || enableStats) {
      bool enableStrCmp = options.getEnableStrStatsCmp();
      colIndexStatistics = createColumnStatistics(type, enableStrCmp);
      if (enableStats) {
        colStripeStatistics = createColumnStatistics(type, enableStrCmp);
        colFileStatistics = createColumnStatistics(type, enableStrCmp);
      }
    }

    if (enableIndex) {
      rowIndex = std::unique_ptr<proto::RowIndex>(new proto::RowIndex());
      rowIndexEntry =
        std::unique_ptr<proto::RowIndexEntry>(new proto::RowIndexEntry());
      rowIndexPosition = std::unique_ptr<RowIndexPositionRecorder>(
                     new RowIndexPositionRecorder(*rowIndexEntry));
      indexStream =
        factory.createStream(proto::Stream_Kind_ROW_INDEX);
    }
  }

  ColumnWriter::~ColumnWriter() {
    // PASS
  }

  void ColumnWriter::add(ColumnVectorBatch& batch,
                         uint64_t offset,
                         uint64_t numValues) {
    notNullEncoder->add(batch.notNull.data() + offset, numValues, nullptr);
  }

  void ColumnWriter::flush(std::vector<proto::Stream>& streams) {
    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_PRESENT);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(notNullEncoder->flush());
    streams.push_back(stream);
  }

  uint64_t ColumnWriter::getEstimatedSize() const {
    return notNullEncoder->getBufferSize();
  }

  void ColumnWriter::getStripeStatistics(
    std::vector<proto::ColumnStatistics>& stats) const {
    getProtoBufStatistics(stats, colStripeStatistics.get());
  }

  void ColumnWriter::mergeStripeStatsIntoFileStats() {
    colFileStatistics->merge(*colStripeStatistics);
    colStripeStatistics->reset();
  }

  void ColumnWriter::mergeRowGroupStatsIntoStripeStats() {
    colStripeStatistics->merge(*colIndexStatistics);
    colIndexStatistics->reset();
  }

  void ColumnWriter::getFileStatistics(
    std::vector<proto::ColumnStatistics>& stats) const {
    getProtoBufStatistics(stats, colFileStatistics.get());
  }

  void ColumnWriter::createRowIndexEntry() {
    proto::ColumnStatistics *indexStats = rowIndexEntry->mutable_statistics();
    colIndexStatistics->toProtoBuf(*indexStats);

    *rowIndex->add_entry() = *rowIndexEntry;

    rowIndexEntry->clear_positions();
    rowIndexEntry->clear_statistics();

    if (enableStats) {
      colStripeStatistics->merge(*colIndexStatistics);
    }
    colIndexStatistics->reset();

    recordPosition();
  }

  void ColumnWriter::writeIndex(std::vector<proto::Stream> &streams) const {
    // write row index to output stream
    rowIndex->SerializeToZeroCopyStream(indexStream.get());

    // construct row index stream
    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_ROW_INDEX);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(indexStream->flush());
    streams.push_back(stream);
  }

  void ColumnWriter::recordPosition() const {
    notNullEncoder->recordPosition(rowIndexPosition.get());
  }

  void ColumnWriter::resetIndex() {
    // clear row index
    rowIndex->clear_entry();
    rowIndexEntry->clear_positions();
    rowIndexEntry->clear_statistics();

    // write current positions
    recordPosition();
  }

  class StructColumnWriter : public ColumnWriter {
  public:
    StructColumnWriter(
                       const Type& type,
                       StreamsFactory& factory,
                       const WriterOptions& options);
    ~StructColumnWriter();

    virtual void add(ColumnVectorBatch& rowBatch,
                     uint64_t offset,
                     uint64_t numValues) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;
    virtual void getColumnEncoding(
      std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void getStripeStatistics(
      std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void getFileStatistics(
      std::vector<proto::ColumnStatistics>& stats) const override;

    virtual void mergeStripeStatsIntoFileStats() override;

    virtual void mergeRowGroupStatsIntoStripeStats() override;

    virtual void createRowIndexEntry() override;

    virtual void writeIndex(
      std::vector<proto::Stream> &streams) const override;

    virtual void resetIndex() override;

  private:
    std::vector<ColumnWriter *> children;
  };

  StructColumnWriter::StructColumnWriter(
                                         const Type& type,
                                         StreamsFactory& factory,
                                         const WriterOptions& options) :
                                         ColumnWriter(type, factory, options) {
    for(unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
      const Type& child = *type.getSubtype(i);
      children.push_back(buildWriter(child, factory, options).release());
    }

    if (enableIndex) {
      recordPosition();
    }
  }

  StructColumnWriter::~StructColumnWriter() {
    for (uint32_t i = 0; i < children.size(); ++i) {
      delete children[i];
    }
  }

  void StructColumnWriter::add(
                              ColumnVectorBatch& rowBatch,
                              uint64_t offset,
                              uint64_t numValues) {
    ColumnWriter::add(rowBatch, offset, numValues);

    const StructVectorBatch & structBatch =
          dynamic_cast<const StructVectorBatch &>(rowBatch);

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->add(*structBatch.fields[i], offset, numValues);
    }

    // update stats
    if (enableIndex || enableStats) {
      bool hasNull = false;
      if (!structBatch.hasNulls) {
        colIndexStatistics->increase(numValues);
      } else {
        const char* notNull = structBatch.notNull.data() + offset;
        for (uint64_t i = 0; i < numValues; ++i) {
          if (notNull[i]) {
            colIndexStatistics->increase(1);
          } else if (!hasNull) {
            hasNull = true;
          }
        }
      }
      colIndexStatistics->setHasNull(hasNull);
    }
  }

  void StructColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->flush(streams);
    }
  }

  void StructColumnWriter::writeIndex(
                      std::vector<proto::Stream> &streams) const {
    ColumnWriter::writeIndex(streams);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->writeIndex(streams);
    }
  }

  uint64_t StructColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    for (uint32_t i = 0; i < children.size(); ++i) {
      size += children[i]->getEstimatedSize();
    }
    return size;
  }

  void StructColumnWriter::getColumnEncoding(
                      std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(proto::ColumnEncoding_Kind_DIRECT);
    encoding.set_dictionarysize(0);
    encodings.push_back(encoding);
    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getColumnEncoding(encodings);
    }
  }

  void StructColumnWriter::getStripeStatistics(
    std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getStripeStatistics(stats);

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getStripeStatistics(stats);
    }
  }

  void StructColumnWriter::mergeStripeStatsIntoFileStats() {
    ColumnWriter::mergeStripeStatsIntoFileStats();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->mergeStripeStatsIntoFileStats();
    }
  }

  void StructColumnWriter::getFileStatistics(
    std::vector<proto::ColumnStatistics>& stats) const {
    ColumnWriter::getFileStatistics(stats);

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->getFileStatistics(stats);
    }
  }

  void StructColumnWriter::mergeRowGroupStatsIntoStripeStats()  {
    ColumnWriter::mergeRowGroupStatsIntoStripeStats();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->mergeRowGroupStatsIntoStripeStats();
    }
  }

  void StructColumnWriter::createRowIndexEntry() {
    ColumnWriter::createRowIndexEntry();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->createRowIndexEntry();
    }
  }

  void StructColumnWriter::resetIndex() {
    ColumnWriter::resetIndex();

    for (uint32_t i = 0; i < children.size(); ++i) {
      children[i]->resetIndex();
    }
  }

  class IntegerColumnWriter : public ColumnWriter {
  public:
    IntegerColumnWriter(
                        const Type& type,
                        StreamsFactory& factory,
                        const WriterOptions& options);

    virtual void add(ColumnVectorBatch& rowBatch,
                     uint64_t offset,
                     uint64_t numValues) override;

    virtual void flush(std::vector<proto::Stream>& streams) override;

    virtual uint64_t getEstimatedSize() const override;

    virtual void getColumnEncoding(
              std::vector<proto::ColumnEncoding>& encodings) const override;

    virtual void recordPosition() const override;

  protected:
    std::unique_ptr<RleEncoder> rleEncoder;

  private:
    RleVersion rleVersion;
  };

  IntegerColumnWriter::IntegerColumnWriter(
                        const Type& type,
                        StreamsFactory& factory,
                        const WriterOptions& options) :
                          ColumnWriter(type, factory, options),
                          rleVersion(options.getRleVersion()) {
    std::unique_ptr<BufferedOutputStream> dataStream =
      factory.createStream(proto::Stream_Kind_DATA);
    rleEncoder = createRleEncoder(
                                  std::move(dataStream),
                                  true,
                                  rleVersion,
                                  memPool);

    if (enableIndex) {
      recordPosition();
    }
  }

  void IntegerColumnWriter::add(
                              ColumnVectorBatch& rowBatch,
                              uint64_t offset,
                              uint64_t numValues) {
    ColumnWriter::add(rowBatch, offset, numValues);

    const LongVectorBatch & longBatch =
                    dynamic_cast<const LongVectorBatch &>(rowBatch);

    const int64_t* data = longBatch.data.data() + offset;
    const char* notNull = longBatch.hasNulls ?
                          longBatch.notNull.data() + offset : nullptr;

    rleEncoder->add(data, numValues, notNull);

    // update stats
    if (enableIndex || enableStats) {
      IntegerColumnStatisticsImpl* intStats =
        dynamic_cast<IntegerColumnStatisticsImpl*>(colIndexStatistics.get());
      for (uint64_t i = 0; i < numValues; ++i) {
        if (notNull == nullptr || notNull[i]) {
          intStats->increase(1);
          intStats->update(data[i], 1);
        } else if (!intStats->hasNull()) {
          intStats->setHasNull(true);
        }
      }
    }
  }

  void IntegerColumnWriter::flush(std::vector<proto::Stream>& streams) {
    ColumnWriter::flush(streams);

    proto::Stream stream;
    stream.set_kind(proto::Stream_Kind_DATA);
    stream.set_column(static_cast<uint32_t>(columnId));
    stream.set_length(rleEncoder->flush());
    streams.push_back(stream);
  }

  uint64_t IntegerColumnWriter::getEstimatedSize() const {
    uint64_t size = ColumnWriter::getEstimatedSize();
    size += rleEncoder->getBufferSize();
    return size;
  }

  void IntegerColumnWriter::getColumnEncoding(
                       std::vector<proto::ColumnEncoding>& encodings) const {
    proto::ColumnEncoding encoding;
    encoding.set_kind(rleVersion == RleVersion_1 ?
                                proto::ColumnEncoding_Kind_DIRECT :
                                proto::ColumnEncoding_Kind_DIRECT_V2);
    encoding.set_dictionarysize(0);
    encodings.push_back(encoding);
  }

  void IntegerColumnWriter::recordPosition() const {
    ColumnWriter::recordPosition();
    rleEncoder->recordPosition(rowIndexPosition.get());
  }

  std::unique_ptr<ColumnWriter> buildWriter(
                                            const Type& type,
                                            StreamsFactory& factory,
                                            const WriterOptions& options) {
    switch (static_cast<int64_t>(type.getKind())) {
      case STRUCT:
        return std::unique_ptr<ColumnWriter>(
          new StructColumnWriter(
                                 type,
                                 factory,
                                 options));
      case INT:
      case LONG:
      case SHORT:
        return std::unique_ptr<ColumnWriter>(
          new IntegerColumnWriter(
                                  type,
                                  factory,
                                  options));
      default:
        throw NotImplementedYet("Type is not supported yet for creating "
                                  "ColumnWriter.");
    }
  }
}
