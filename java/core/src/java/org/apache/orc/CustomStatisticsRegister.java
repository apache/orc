/*
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
package org.apache.orc;

import com.google.protobuf.ByteString;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.orc.CustomStatistics.EMPTY_IMPL;

public class CustomStatisticsRegister {

  private static class CustomStatisticsRegisterHandler {
    private static final CustomStatisticsRegister INSTANCE = new CustomStatisticsRegister();
  }

  public static CustomStatisticsRegister getInstance() {
    return CustomStatisticsRegisterHandler.INSTANCE;
  }

  public static final CustomStatisticsBuilder EMPTY_IMPL_BUILDER = new CustomStatisticsBuilder() {
    @Override
    public String name() {
      return EMPTY_IMPL.name();
    }

    @Override
    public CustomStatistics[] build() {
      return new CustomStatistics[] {
          EMPTY_IMPL, EMPTY_IMPL, EMPTY_IMPL
      };
    }

    @Override
    public CustomStatistics build(ByteString statisticsContent) {
      return EMPTY_IMPL;
    }
  };

  private final Map<String, CustomStatisticsBuilder> registerMap = new ConcurrentHashMap<>();

  private CustomStatisticsRegister() {}

  public void register(CustomStatisticsBuilder customStatisticsBuilder) {
    registerMap.put(customStatisticsBuilder.name(), customStatisticsBuilder);
  }

  public CustomStatisticsBuilder getCustomStatisticsBuilder(String name) {
    return registerMap.getOrDefault(name, EMPTY_IMPL_BUILDER);
  }

  public void clear() {
    registerMap.clear();
  }

}
