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

#pragma once
#include <array>
#include <cmath>
#include <unordered_set>
#include <vector>
#include <string>
#include <string_view>
#include <ostream>

namespace orc::geospatial {

/// \brief Infinity, used to define bounds of empty bounding boxes
constexpr double kInf = std::numeric_limits<double>::infinity();

/// \brief The maximum number of dimensions represented by a geospatial type
/// (i.e., X, Y, Z, and M)
inline constexpr int kMaxDimensions = 4;

/// \brief Valid combinations of dimensions allowed by ISO well-known binary
///
/// These values correspond to the 0, 1000, 2000, 3000 component of the WKB integer
/// geometry type (i.e., the value of geometry_type // 1000).
enum class Dimensions {
  kXY = 0,
  kXYZ = 1,
  kXYM = 2,
  kXYZM = 3,
  kValueMin = 0,
  kValueMax = 3
};

/// \brief The supported set of geometry types allowed by ISO well-known binary
///
/// These values correspond to the 1, 2, ..., 7 component of the WKB integer
/// geometry type (i.e., the value of geometry_type % 1000).
enum class GeometryType {
  kPoint = 1,
  kLinestring = 2,
  kPolygon = 3,
  kMultiPoint = 4,
  kMultiLinestring = 5,
  kMultiPolygon = 6,
  kGeometryCollection = 7,
  kValueMin = 1,
  kValueMax = 7
};

struct BoundingBox {
  using XY = std::array<double, 2>;
  using XYZ = std::array<double, 3>;
  using XYM = std::array<double, 3>;
  using XYZM = std::array<double, 4>;

  BoundingBox(const XYZM& mins, const XYZM& maxes) : min(mins), max(maxes) {}
  BoundingBox() : min{kInf, kInf, kInf, kInf}, max{-kInf, -kInf, -kInf, -kInf} {}
  BoundingBox(const BoundingBox& other) = default;
  BoundingBox& operator=(const BoundingBox&) = default;

  /// \brief Update the X and Y bounds to ensure these bounds contain coord
  void UpdateXY(const XY& coord) {
    UpdateInternal(coord);
  }

  /// \brief Update the X, Y, and Z bounds to ensure these bounds contain coord
  void UpdateXYZ(const XYZ& coord) {
    UpdateInternal(coord);
  }

  /// \brief Update the X, Y, and M bounds to ensure these bounds contain coord
  void UpdateXYM(const XYM& coord) {
    std::array<int, 3> dimensions = {0, 1, 3};
    for (int i = 0; i < 3; i++) {
      auto dimension = dimensions[i];
      if ((std::isnan(min[dimension]) == 0) && (std::isnan(max[dimension]) == 0)) {
        min[dimension] = std::min(min[dimension], coord[i]);
        max[dimension] = std::max(max[dimension], coord[i]);
      }
    }
  }

  /// \brief Update the X, Y, Z, and M bounds to ensure these bounds contain coord
  void UpdateXYZM(const XYZM& coord) {
    UpdateInternal(coord);
  }

  /// \brief Reset these bounds to an empty state such that they contain no coordinates
  void Reset() {
    for (int i = 0; i < kMaxDimensions; i++) {
      min[i] = kInf;
      max[i] = -kInf;
    }
  }

  void Invalidate() {
    for (int i = 0; i < kMaxDimensions; i++) {
      min[i] = std::numeric_limits<double>::quiet_NaN();
      max[i] = std::numeric_limits<double>::quiet_NaN();
    }
  }

  bool BoundEmpty(int dimension) const {
    return std::isinf(min[dimension] - max[dimension]) != 0;
  }

  bool BoundValid(int dimension) const {
    return (std::isnan(min[dimension]) == 0) && (std::isnan(max[dimension]) == 0);
  }

  const XYZM& LowerBound() const {
    return min;
  }

  const XYZM& UpperBound() const {
    return max;
  }

  std::array<bool, kMaxDimensions> DimensionValid() const {
    return {BoundValid(0), BoundValid(1), BoundValid(2), BoundValid(3)};
  }

  std::array<bool, kMaxDimensions> DimensionEmpty() const {
    return {BoundEmpty(0), BoundEmpty(1), BoundEmpty(2), BoundEmpty(3)};
  }

  /// \brief Update these bounds such they also contain other
  void Merge(const BoundingBox& other) {
    for (int i = 0; i < kMaxDimensions; i++) {
      if (std::isnan(min[i]) || std::isnan(max[i]) || std::isnan(other.min[i]) ||
          std::isnan(other.max[i])) {
        min[i] = std::numeric_limits<double>::quiet_NaN();
        max[i] = std::numeric_limits<double>::quiet_NaN();
      } else {
        min[i] = std::min(min[i], other.min[i]);
        max[i] = std::max(max[i], other.max[i]);
      }
    }
  }

  std::string ToString() const;

  XYZM min;
  XYZM max;

 private:
  // This works for XY, XYZ, and XYZM
  template <typename Coord>
  void UpdateInternal(Coord coord) {
    for (size_t i = 0; i < coord.size(); i++) {
      if (!std::isnan(min[i]) && !std::isnan(max[i])) {
        min[i] = std::min(min[i], coord[i]);
        max[i] = std::max(max[i], coord[i]);
      }
    }
  }
};

inline bool operator==(const BoundingBox& lhs, const BoundingBox& rhs) {
  return lhs.min == rhs.min && lhs.max == rhs.max;
}

inline bool operator!=(const BoundingBox& lhs, const BoundingBox& rhs) {
  return !(lhs == rhs);
}

inline std::ostream& operator<<(std::ostream& os, const BoundingBox& obj) {
  os << obj.ToString();
  return os;
}


class WKBBuffer;

/// \brief Accumulate a BoundingBox and geometry types based on zero or more well-known
/// binary blobs
///
/// Note that this class is NOT appropriate for bounding a GEOGRAPHY,
/// whose bounds are not a function purely of the vertices. Geography bounding
/// is not yet implemented.
class WKBGeometryBounder {
 public:
  /// \brief Accumulate the bounds of a serialized well-known binary geometry
  ///
  /// Throws ParquetException for any parse errors encountered. Bounds for
  /// any encountered coordinates are accumulated and the geometry type of
  /// the geometry is added to the internal geometry type list.
  void MergeGeometry(std::string_view bytesWkb);

  void MergeGeometry(const uint8_t* bytesWkb, size_t bytesSize);

    /// \brief Accumulate the bounds of a previously-calculated BoundingBox
  void MergeBox(const BoundingBox& box) { box_.Merge(box); }

  /// \brief Accumulate a previously-calculated list of geometry types
  void MergeGeometryTypes(const std::vector<int> geospatialTypes) {
    geospatialTypes_.insert(geospatialTypes.begin(), geospatialTypes.end());
  }

  /// \brief Accumulate the bounds of a previously-calculated BoundingBox
  void Merge(const WKBGeometryBounder& other) {
    if (!IsValid() || !other.IsValid()) {
      Invalidate();
      return;
    }
    box_.Merge(other.box_);
    geospatialTypes_.insert(other.geospatialTypes_.begin(), other.geospatialTypes_.end());
  }

  /// \brief Retrieve the accumulated bounds
  const BoundingBox& Bounds() const { return box_; }

  /// \brief Retrieve the accumulated geometry types
  std::vector<int32_t> GeometryTypes() const;

  /// \brief Reset the internal bounds and geometry types list to an empty state
  void Reset() {
    isValid_ = true;
    box_.Reset();
    geospatialTypes_.clear();
  }

  bool IsValid() const { return isValid_; }

  void Invalidate() {
      isValid_ = false;
      box_.Invalidate();
      geospatialTypes_.clear();
  }


 private:
  BoundingBox box_;
  std::unordered_set<int32_t> geospatialTypes_;
  bool isValid_ = true;

  void MergeGeometryInternal(WKBBuffer* src, bool recordWkbType);

  void MergeSequence(WKBBuffer* src, Dimensions dimensions, uint32_t nCoords, bool swap);
};

}  // namespace orc::geospatial