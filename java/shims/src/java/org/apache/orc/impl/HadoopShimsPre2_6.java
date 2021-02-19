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

import org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.orc.shims.SeekableInputStream;


/**
 * Shims for versions of Hadoop less than 2.6
 *
 * Adds support for:
 * <ul>
 *   <li>Direct buffer decompression</li>
 *   <li>Zero copy</li>
 * </ul>
 */
public class HadoopShimsPre2_6 extends HadoopShimsPre2_3 {

  static class SnappyDirectDecompressWrapper implements DirectDecompressor {
    private final SnappyDirectDecompressor root;
    private boolean isFirstCall = true;

    SnappyDirectDecompressWrapper(SnappyDirectDecompressor root) {
      this.root = root;
    }

    @Override
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

  static class ZlibDirectDecompressWrapper implements DirectDecompressor {
    private final ZlibDecompressor.ZlibDirectDecompressor root;
    private boolean isFirstCall = true;

    ZlibDirectDecompressWrapper(ZlibDecompressor.ZlibDirectDecompressor root) {
      this.root = root;
    }

    @Override
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

  static DirectDecompressor getDecompressor( DirectCompressionType codec) {
    switch (codec) {
      case ZLIB:
        return new ZlibDirectDecompressWrapper
            (new ZlibDecompressor.ZlibDirectDecompressor());
      case ZLIB_NOHEADER:
        return new ZlibDirectDecompressWrapper
            (new ZlibDecompressor.ZlibDirectDecompressor
                (ZlibDecompressor.CompressionHeader.NO_HEADER, 0));
      case SNAPPY:
        return new SnappyDirectDecompressWrapper
            (new SnappyDirectDecompressor());
      default:
        return null;
    }
  }

  @Override
  public DirectDecompressor getDirectDecompressor(DirectCompressionType codec) {
    return getDecompressor(codec);
 }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(SeekableInputStream in,
                                              ByteBufferPoolShim pool
                                              ) {
    return ZeroCopyShims.getZeroCopyReader(in, pool);
  }
}
