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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Shims for recent versions of Hadoop
 */
public class HadoopShimsCurrent implements HadoopShims {

  private static class SnappyDirectDecompressWrapper implements DirectDecompressor {
    private final SnappyDirectDecompressor root;
    private boolean isFirstCall = true;

    SnappyDirectDecompressWrapper(SnappyDirectDecompressor root) {
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

  private static class ZlibDirectDecompressWrapper implements DirectDecompressor {
    private final ZlibDecompressor.ZlibDirectDecompressor root;
    private boolean isFirstCall = true;

    ZlibDirectDecompressWrapper(ZlibDecompressor.ZlibDirectDecompressor root) {
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

  public DirectDecompressor getDirectDecompressor(
      DirectCompressionType codec) {
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

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    return ZeroCopyShims.getZeroCopyReader(in, pool);
  }

  private static final class FastTextReaderShim implements TextReaderShim {
    private final DataInputStream din;

    public FastTextReaderShim(InputStream in) {
      this.din = new DataInputStream(in);
    }

    @Override
    public void read(Text txt, int len) throws IOException {
      txt.readWithKnownLength(din, len);
    }
  }

  @Override
  public TextReaderShim getTextReaderShim(InputStream in) throws IOException {
    return new FastTextReaderShim(in);
  }

}
