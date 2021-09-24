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
