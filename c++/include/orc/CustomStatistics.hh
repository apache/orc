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

#ifndef ORC_CUSTOM_STATISTICS_HH
#define ORC_CUSTOM_STATISTICS_HH

#include "orc/orc-config.hh"
#include "Vector.hh"

#include <map>

namespace orc {

  class CustomStatistics {
  public:
    virtual ~CustomStatistics();

    static CustomStatistics& emptyCustomStatistics();

    virtual std::string name() const;

    virtual std::string& content() const {
      static std::string &emptyImplContent = *new std::string("");
      return emptyImplContent;
    }

    virtual bool needPersistence() const {
      return false;
    }

    virtual void merge(CustomStatistics& customStatistics) const {
      (void)customStatistics;
    }

    virtual void reset() const {}

    virtual void updateBinary(size_t length) {
      (void)length;
    }

    virtual void updateBoolean(bool value, size_t repetitions) {
      (void)value;
      (void)repetitions;
    }

    virtual void updateDate(int32_t value) {
      (void)value;
    }

    virtual void updateDouble(double value) {
      (void)value;
    }

    virtual void updateInteger(int64_t value, int repetitions) {
      (void)value;
      (void)repetitions;
    }

    virtual void updateString(const char* value, size_t length) {
      (void)value;
      (void)length;
    }

    virtual void updateString(std::string value) {
      (void)&value;
    }

    virtual void updateDecimal(const Decimal& value) {
      (void)value;
    }

    virtual void updateTimestamp(int64_t value) {
      (void)value;
    }

    virtual void updateTimestamp(int64_t milli, int32_t nano) {
      (void)milli;
      (void)nano;
    }

    virtual void updateCollection(uint64_t value) {
      (void)value;
    }
  };

  class CustomStatisticsBuilder {
  public:
    CustomStatisticsBuilder();
    virtual ~CustomStatisticsBuilder();

    static CustomStatisticsBuilder& emptyCustomStatisticsBuilder();

    virtual std::string name() const;

    virtual CustomStatistics &build(const std::string& statisticsContent) const;

    virtual std::vector<CustomStatistics*> build() const;
  };

  class CustomStatisticsRegister {
  public:
    static CustomStatisticsRegister& instance() {
      static CustomStatisticsRegister &instance = *new CustomStatisticsRegister;
      return instance;
    }

    void registerBuilder(CustomStatisticsBuilder* customStatisticsBuilder) {
      registerMap.insert(std::make_pair(customStatisticsBuilder->name(), customStatisticsBuilder));
    }

    const CustomStatisticsBuilder* getCustomStatisticsBuilder(const std::string& name) {
      std::map<std::string, CustomStatisticsBuilder*>::const_iterator iterator = registerMap.find(name);
      if (iterator != registerMap.end()) {
        return iterator->second;
      }
      else {
        return &CustomStatisticsBuilder::emptyCustomStatisticsBuilder();
      }
    }

    void clear() {
      registerMap.clear();
    }

  private:
    std::map<std::string, CustomStatisticsBuilder*> registerMap;
    CustomStatisticsRegister() = default;
  };

}

#endif
