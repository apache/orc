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

#include "orc/Geospatial.hh"

#include <algorithm>
#include <sstream>
#include <cstring>
#include "orc/Exceptions.hh"

namespace orc::geospatial {

template <typename T>
inline std::enable_if_t<std::is_trivially_copyable_v<T>, T> SafeLoadAs(
    const uint8_t* unaligned) {
  std::remove_const_t<T> ret;
  std::memcpy(&ret, unaligned, sizeof(T));
  return ret;
}

template <typename U, typename T>
inline std::enable_if_t<std::is_trivially_copyable_v<T> &&
                            std::is_trivially_copyable_v<U> && sizeof(T) == sizeof(U),
                        U>
SafeCopy(T value) {
  std::remove_const_t<U> ret;
  std::memcpy(&ret, static_cast<const void*>(&value), sizeof(T));
  return ret;
}

static bool isLittleEndian() {
  static union {
    uint32_t i;
    char c[4];
  } num = {0x01020304};
  return num.c[0] == 4;
}

#if defined(_MSC_VER)
#  include <intrin.h>  // IWYU pragma: keep
#  define ORC_BYTE_SWAP64 _byteswap_uint64
#  define ORC_BYTE_SWAP32 _byteswap_ulong
#else
#  define ORC_BYTE_SWAP64 __builtin_bswap64
#  define ORC_BYTE_SWAP32 __builtin_bswap32
#endif

// Swap the byte order (i.e. endianness)
static inline uint32_t ByteSwap(uint32_t value) {
  return static_cast<uint32_t>(ORC_BYTE_SWAP32(value));
}
static inline double ByteSwap(double value) {
  const uint64_t swapped = ORC_BYTE_SWAP64(SafeCopy<uint64_t>(value));
  return SafeCopy<double>(swapped);
}

std::string BoundingBox::ToString() const {
  std::stringstream ss;
  ss << "BoundingBox" << std::endl;
  ss << "  x: [" << min[0] << ", " << max[0] << "]" << std::endl;
  ss << "  y: [" << min[1] << ", " << max[1] << "]" << std::endl;
  ss << "  z: [" << min[2] << ", " << max[2] << "]" << std::endl;
  ss << "  m: [" << min[3] << ", " << max[3] << "]" << std::endl;

  return ss.str();
}

/// \brief Object to keep track of the low-level consumption of a well-known binary
/// geometry
///
/// Briefly, ISO well-known binary supported by the Parquet spec is an endian byte
/// (0x01 or 0x00), followed by geometry type + dimensions encoded as a (uint32_t),
/// followed by geometry-specific data. Coordinate sequences are represented by a
/// uint32_t (the number of coordinates) plus a sequence of doubles (number of coordinates
/// multiplied by the number of dimensions).
class WKBBuffer {
 public:
  WKBBuffer() : data_(nullptr), size_(0) {}
  WKBBuffer(const uint8_t* data, int64_t size) : data_(data), size_(size) {}

  uint8_t ReadUInt8() { return ReadChecked<uint8_t>(); }

  uint32_t ReadUInt32(bool swap) {
    auto value = ReadChecked<uint32_t>();
    return swap ? ByteSwap(value) : value;
  }

  template <typename Coord, typename Visit>
  void ReadCoords(uint32_t nCoords, bool swap, Visit&& visit) {
    size_t total_bytes = nCoords * sizeof(Coord);
    if (size_ < total_bytes) {
    }

    if (swap) {
      Coord coord;
      for (uint32_t i = 0; i < nCoords; i++) {
        coord = ReadUnchecked<Coord>();
        for (auto& c : coord) {
          c = ByteSwap(c);
        }

        std::forward<Visit>(visit)(coord);
      }
    } else {
      for (uint32_t i = 0; i < nCoords; i++) {
        std::forward<Visit>(visit)(ReadUnchecked<Coord>());
      }
    }
  }

  size_t size() const { return size_; }

 private:
  const uint8_t* data_;
  size_t size_;

  template <typename T>
  T ReadChecked() {
    if (size_ < sizeof(T)) {
      std::stringstream ss;
      ss << "Can't read" << sizeof(T) << " bytes from WKBBuffer with " << size_ << " remaining";
      throw ParseError(ss.str());
    }

    return ReadUnchecked<T>();
  }

  template <typename T>
  T ReadUnchecked() {
    T out = SafeLoadAs<T>(data_);
    data_ += sizeof(T);
    size_ -= sizeof(T);
    return out;
  }
};

using GeometryTypeAndDimensions = std::pair<GeometryType, Dimensions>;

namespace {

std::optional<GeometryTypeAndDimensions> ParseGeometryType(uint32_t wkbGeometryType) {
  // The number 1000 can be used because WKB geometry types are constructed
  // on purpose such that this relationship is true (e.g., LINESTRING ZM maps
  // to 3002).
  uint32_t geometryTypeComponent = wkbGeometryType % 1000;
  uint32_t dimensionsComponent = wkbGeometryType / 1000;

  auto minGeometryTypeValue = static_cast<uint32_t>(GeometryType::kValueMin);
  auto maxGeometryTypeValue = static_cast<uint32_t>(GeometryType::kValueMax);
  auto minDimensionValue = static_cast<uint32_t>(Dimensions::kValueMin);
  auto maxDimensionValue = static_cast<uint32_t>(Dimensions::kValueMax);

  if (geometryTypeComponent < minGeometryTypeValue ||
      geometryTypeComponent > maxGeometryTypeValue ||
      dimensionsComponent < minDimensionValue ||
      dimensionsComponent > maxDimensionValue) {
      return std::nullopt;
  }

  return std::make_optional(GeometryTypeAndDimensions{static_cast<GeometryType>(geometryTypeComponent),
                                                      static_cast<Dimensions>(dimensionsComponent)});
}

}  // namespace

std::vector<int32_t> WKBGeometryBounder::GeometryTypes() const {
  std::vector<int32_t> out(geospatialTypes_.begin(), geospatialTypes_.end());
  std::sort(out.begin(), out.end());
  return out;
}

void WKBGeometryBounder::MergeGeometry(std::string_view bytesWkb) {
  if (!isValid_) { return; }
  MergeGeometry(reinterpret_cast<const uint8_t*>(bytesWkb.data()), bytesWkb.size());
}

void WKBGeometryBounder::MergeGeometry(const uint8_t* bytesWkb, size_t bytesSize) {
  if (!isValid_) { return; }
  WKBBuffer src{bytesWkb, static_cast<int64_t>(bytesSize)};
  try {
    MergeGeometryInternal(&src, /*record_wkb_type=*/true);
  } catch (const ParseError&) {
    Invalidate();
    return;
  }
  if (src.size() != 0) {
    // "Exepcted zero bytes after consuming WKB
    Invalidate();
  }
}

void WKBGeometryBounder::MergeGeometryInternal(WKBBuffer* src, bool recordWkbType) {
  uint8_t endian = src->ReadUInt8();
  bool swap = endian != 0x00;
  if(isLittleEndian()) { swap = endian != 0x01; }

  uint32_t wkbGeometryType = src->ReadUInt32(swap);
  auto geometryTypeAndDimensions = ParseGeometryType(wkbGeometryType);
  if (!geometryTypeAndDimensions.has_value()) {
    Invalidate();
    return;
  }
  auto& [geometry_type, dimensions] = geometryTypeAndDimensions.value();

  // Keep track of geometry types encountered if at the top level
  if (recordWkbType) {
    geospatialTypes_.insert(static_cast<int32_t>(wkbGeometryType));
  }

  switch (geometry_type) {
    case GeometryType::kPoint:
      MergeSequence(src, dimensions, 1, swap);
      break;

    case GeometryType::kLinestring: {
      uint32_t nCoords = src->ReadUInt32(swap);
      MergeSequence(src, dimensions, nCoords, swap);
      break;
    }
    case GeometryType::kPolygon: {
      uint32_t n_parts = src->ReadUInt32(swap);
      for (uint32_t i = 0; i < n_parts; i++) {
        uint32_t nCoords = src->ReadUInt32(swap);
        MergeSequence(src, dimensions, nCoords, swap);
      }
      break;
    }

    // These are all encoded the same in WKB, even though this encoding would
    // allow for parts to be of a different geometry type or different dimensions.
    // For the purposes of bounding, this does not cause us problems. We pass
    // record_wkb_type = false because we do not want the child geometry to be
    // added to the geometry_types list (e.g., for a MultiPoint, we only want
    // the code for MultiPoint to be added, not the code for Point).
    case GeometryType::kMultiPoint:
    case GeometryType::kMultiLinestring:
    case GeometryType::kMultiPolygon:
    case GeometryType::kGeometryCollection: {
      uint32_t n_parts = src->ReadUInt32(swap);
      for (uint32_t i = 0; i < n_parts; i++) {
        MergeGeometryInternal(src, /*record_wkb_type*/ false);
      }
      break;
    }
  }
}

void WKBGeometryBounder::MergeSequence(WKBBuffer* src, Dimensions dimensions,
                                       uint32_t nCoords, bool swap) {
  switch (dimensions) {
    case Dimensions::kXY:
      src->ReadCoords<BoundingBox::XY>(
          nCoords, swap, [&](BoundingBox::XY coord) { box_.UpdateXY(coord); });
      break;
    case Dimensions::kXYZ:
      src->ReadCoords<BoundingBox::XYZ>(
          nCoords, swap, [&](BoundingBox::XYZ coord) { box_.UpdateXYZ(coord); });
      break;
    case Dimensions::kXYM:
      src->ReadCoords<BoundingBox::XYM>(
          nCoords, swap, [&](BoundingBox::XYM coord) { box_.UpdateXYM(coord); });
      break;
    case Dimensions::kXYZM:
      src->ReadCoords<BoundingBox::XYZM>(
          nCoords, swap, [&](BoundingBox::XYZM coord) { box_.UpdateXYZM(coord); });
      break;
    default:
      Invalidate();
  }
}

} // namespace orc::geospatial