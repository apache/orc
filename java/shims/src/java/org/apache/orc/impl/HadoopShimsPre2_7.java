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
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Shims for versions of Hadoop less than 2.7
 */
public class HadoopShimsPre2_7 implements HadoopShims {

  static class SnappyDirectDecompressWrapper implements DirectDecompressor {
    private final SnappyDirectDecompressor root;

    SnappyDirectDecompressWrapper(SnappyDirectDecompressor root) {
      this.root = root;
    }

    public void decompress(ByteBuffer input, ByteBuffer output) throws IOException {
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

    ZlibDirectDecompressWrapper(ZlibDecompressor.ZlibDirectDecompressor root) {
      this.root = root;
    }

    public void decompress(ByteBuffer input, ByteBuffer output) throws IOException {
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
            (new SnappyDecompressor.SnappyDirectDecompressor());
      default:
        return null;
    }
  }

  public DirectDecompressor getDirectDecompressor( DirectCompressionType codec) {
    return getDecompressor(codec);
 }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    return ZeroCopyShims.getZeroCopyReader(in, pool);
  }

  @Override
  public long padStreamToBlock(OutputStream output,
                               long padding) throws IOException {
    return HadoopShimsPre2_3.padStream(output, padding);
  }

}
