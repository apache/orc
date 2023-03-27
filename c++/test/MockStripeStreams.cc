#include "MockStripeStreams.hh"

namespace orc {
  MemoryPool& MockStripeStreams::getMemoryPool() const {
    return *getDefaultPool();
  }

  ReaderMetrics* MockStripeStreams::getReaderMetrics() const {
    return getDefaultReaderMetrics();
  }

  const Timezone& MockStripeStreams::getWriterTimezone() const {
    return getTimezoneByName("America/Los_Angeles");
  }

  const Timezone& MockStripeStreams::getReaderTimezone() const {
    return getTimezoneByName("GMT");
  }

  std::unique_ptr<SeekableInputStream> MockStripeStreams::getStream(uint64_t columnId,
                                                                    proto::Stream_Kind kind,
                                                                    bool stream) const {
    return std::unique_ptr<SeekableInputStream>(getStreamProxy(columnId, kind, stream));
  }

}  // namespace orc
