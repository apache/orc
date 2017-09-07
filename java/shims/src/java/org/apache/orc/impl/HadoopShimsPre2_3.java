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

package org.apache.orc.impl;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Shims for versions of Hadoop up to and including 2.2.x
 */
public class HadoopShimsPre2_3 implements HadoopShims {

  HadoopShimsPre2_3() {
  }

  public DirectDecompressor getDirectDecompressor(
      DirectCompressionType codec) {
    return null;
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    /* not supported */
    return null;
  }

  private static final int BUFFER_SIZE = 256  * 1024;

  static long padStream(OutputStream output,
                        long padding) throws IOException {
    byte[] pad = new byte[(int) Math.min(BUFFER_SIZE, padding)]; // always clear
    while (padding > 0) {
      int writeLen = (int) Math.min(padding, pad.length);
      output.write(pad, 0, writeLen);
      padding -= writeLen;
    }
    return padding;
  }

  @Override
  public long padStreamToBlock(OutputStream output, long padding) throws IOException {
    return padStream(output, padding);
  }

}
