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

public class DoubleWriterV2 implements DoubleWriter {
  private final PositionedOutputStream output;
  private final Fpc.FpcCompressor fpcCompressor;
  private final double[] literals = new double[32 * 1024];
  private int numLiterals;

  public DoubleWriterV2(PositionedOutputStream output) throws IOException {
    this.output = output;
    this.fpcCompressor = new Fpc.FpcCompressor(output);
  }

  @Override
  public void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }

  @Override
  public void write(double value) throws IOException {
    literals[numLiterals++] = value;
    if (numLiterals == literals.length) {
      writeValues();
    }
  }

  private void writeValues() throws IOException {
    if (numLiterals != 0) {
      fpcCompressor.compress(literals, numLiterals);
      numLiterals = 0;
    }
  }

  @Override
  public void flush() throws IOException {
    if (numLiterals != 0) {
      writeValues();
    }
    output.flush();
  }

  @Override
  public long estimateMemory() {
    return output.getBufferSize();
  }
}
