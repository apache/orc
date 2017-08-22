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

package org.apache.orc.impl;

import java.io.IOException;

public class PlainIntegerWriter implements IntegerWriter {
  private final PositionedOutputStream output;
  private final SerializationUtils utils;

  public PlainIntegerWriter(PositionedOutputStream output) {
    this.output = output;
    this.utils = new SerializationUtils();
  }

  @Override
  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
  }

  @Override
  public void write(long value) throws IOException {
    utils.writeLongLE(output, value);
  }

  @Override
  public void flush() throws IOException {
    output.flush();
  }

  @Override
  public long estimateMemory() {
    return output.getBufferSize();
  }
}
