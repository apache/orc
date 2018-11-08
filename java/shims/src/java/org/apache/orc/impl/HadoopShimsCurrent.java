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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.io.compress.zstd.ZStandardDecompressor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Shims for recent versions of Hadoop
 *
 * Adds support for:
 * <ul>
 *   <li>Variable length HDFS blocks</li>
 * </ul>
 */
public class HadoopShimsCurrent implements HadoopShims {

  static class ZstdDirectDecompressWrapper implements DirectDecompressor {
    private final ZStandardDecompressor.ZStandardDirectDecompressor root;
    private boolean isFirstCall = true;

    ZstdDirectDecompressWrapper(ZStandardDecompressor.ZStandardDirectDecompressor root) {
      this.root = root;
    }

    public void decompress(ByteBuffer input, ByteBuffer output) throws IOException {
      if (!isFirstCall) {
        root.reset();
      } else {
        isFirstCall = false;
      }
      root.decompress(input, output);
    }

    @Override
    public void reset() {
      root.reset();
    }

    @Override
    public void end() {
      root.end();
    }
  }

  static DirectDecompressor getDecompressor(DirectCompressionType codec) {
    switch (codec) {
      case ZSTD:
        return new ZstdDirectDecompressWrapper
                (new ZStandardDecompressor.ZStandardDirectDecompressor(0));
      default:
        return HadoopShimsPre2_6.getDecompressor(codec);
    }
  }

  public DirectDecompressor getDirectDecompressor(DirectCompressionType codec) {
    return getDecompressor(codec);
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    return ZeroCopyShims.getZeroCopyReader(in, pool);
  }

  @Override
  public boolean endVariableLengthBlock(OutputStream output) throws IOException {
    return HadoopShimsPre2_9.endVariableLengthBlocks(output);
  }

  @Override
  public KeyProvider getKeyProvider(Configuration conf,
                                    Random random) throws IOException {
    return HadoopShimsPre2_7.createKeyProvider(conf, random);
  }

  @Override
  public CompressionCodec createZstdCodec() {
    // check if native zstd library is loaded; otherwise RuntimeException will be thrown
    ZStandardCodec.checkNativeCodeLoaded();
    ZStandardCodec zStandardCodec = new ZStandardCodec();
    zStandardCodec.setConf(new Configuration());
    return zStandardCodec;
  }
}
