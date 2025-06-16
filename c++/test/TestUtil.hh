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
#include "orc/Geospatial.hh"

namespace orc {

/// \brief Number of bytes in a WKB Point with X and Y dimensions (uint8_t endian,
/// uint32_t geometry type, 2 * double coordinates)
static constexpr int kWkbPointXYSize = 21;

static bool isLittleEndian() {
    static union {
        uint32_t i;
        char c[4];
    } num = {0x01020304};
    return num.c[0] == 4;
}

static uint8_t kWkbNativeEndianness = isLittleEndian() ? 0x01 : 0x00;

uint32_t GeometryTypeToWKB(geospatial::GeometryType geometryType, bool hasZ, bool hasM);
std::string MakeWKBPoint(const std::vector<double> &xyzm, bool hasZ, bool hasM);

}  // namespace orc