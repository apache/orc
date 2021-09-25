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

#include "orc/CustomStatistics.hh"

namespace orc {

  std::string CustomStatistics::name() const {
    static std::string &emptyImplName = *new std::string("EmptyCustomStatisticsImpl");
    return emptyImplName;
  }

  CustomStatistics &CustomStatistics::emptyCustomStatistics() {
    static CustomStatistics &EMPTY_IMPL = *new CustomStatistics();
    return EMPTY_IMPL;
  }

  CustomStatistics::~CustomStatistics() = default;

  CustomStatisticsBuilder::~CustomStatisticsBuilder() = default;

  std::string CustomStatisticsBuilder::name() const {
    return CustomStatistics::emptyCustomStatistics().name();
  }

  CustomStatistics &CustomStatisticsBuilder::build(const std::string& statisticsContent) const {
    (void)statisticsContent;
    return CustomStatistics::emptyCustomStatistics();
  }

  std::vector<CustomStatistics*> CustomStatisticsBuilder::build() const {
    std::vector<CustomStatistics*> vector;
    vector.push_back(&CustomStatistics::emptyCustomStatistics());
    vector.push_back(&CustomStatistics::emptyCustomStatistics());
    vector.push_back(&CustomStatistics::emptyCustomStatistics());
    return vector;
  }

  CustomStatisticsBuilder &CustomStatisticsBuilder::emptyCustomStatisticsBuilder() {
    static CustomStatisticsBuilder &EMPTY_IMPL_BUILDER = *new CustomStatisticsBuilder;
    return EMPTY_IMPL_BUILDER;
  }

  CustomStatisticsBuilder::CustomStatisticsBuilder() {

  }
}
