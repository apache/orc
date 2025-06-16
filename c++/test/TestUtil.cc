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

#include "TestUtil.hh"
#include <cassert>
#include <cstring>

namespace orc {
uint32_t GeometryTypeToWKB(geospatial::GeometryType geometryType, bool hasZ, bool hasM) {
    auto wkbGeomType = static_cast<uint32_t>(geometryType);

    if (hasZ) {
        wkbGeomType += 1000;
    }

    if (hasM) {
        wkbGeomType += 2000;
    }

    return wkbGeomType;
}

std::string MakeWKBPoint(const std::vector<double> &xyzm, bool hasZ, bool hasM) {
    // 1:endianness + 4:type + 8:x + 8:y
    int numBytes = kWkbPointXYSize + (hasZ ? sizeof(double) : 0) + (hasM ? sizeof(double) : 0);
    std::string wkb(numBytes, 0);
    char *ptr = wkb.data();

    ptr[0] = kWkbNativeEndianness;
    uint32_t geom_type = GeometryTypeToWKB(geospatial::GeometryType::kPoint, hasZ, hasM);
    std::memcpy(&ptr[1], &geom_type, 4);
    std::memcpy(&ptr[5], &xyzm[0], 8);
    std::memcpy(&ptr[13], &xyzm[1], 8);
    ptr += 21;

    if (hasZ) {
        std::memcpy(ptr, &xyzm[2], 8);
        ptr += 8;
    }

    if (hasM) {
        std::memcpy(ptr, &xyzm[3], 8);
        ptr += 8;
    }

    assert(static_cast<size_t>(ptr - wkb.data()) == wkb.length());
    return wkb;
}

}