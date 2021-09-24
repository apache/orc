package org.apache.orc;

import com.google.protobuf.ByteString;

public interface CustomStatisticsBuilder {

  String name();

  CustomStatistics[] build();

  CustomStatistics build(ByteString statisticsContent);
}
