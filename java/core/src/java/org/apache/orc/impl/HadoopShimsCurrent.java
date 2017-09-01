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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor.SnappyDirectDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

/**
 * Shims for recent versions of Hadoop
 */
public class HadoopShimsCurrent implements HadoopShims {

  private static class SnappyDirectDecompressWrapper implements DirectDecompressor {
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

  private static class ZlibDirectDecompressWrapper implements DirectDecompressor {
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

  private static class VariableBlockFillerShim implements BlockFillerShim {
    static final EnumSet<SyncFlag> variableBlocksSupported = checkVariableBlocksSupport();
    static final BlockFillerShim fallback = new ZeroFillerShim();

    private static EnumSet<SyncFlag> checkVariableBlocksSupport() {
      if (SyncFlag.values().length > 1) {
        // make sure this can compile on 2.6.x
        return EnumSet.of(SyncFlag.valueOf("END_BLOCK"));
      }
      return null;
    }

    @Override
    public long fill(OutputStream output, long padding) throws IOException {
      if (padding == 0) {
        return 0;
      }
      if (output instanceof HdfsDataOutputStream && variableBlocksSupported != null) {
        ((HdfsDataOutputStream) output).hsync(variableBlocksSupported);
        return 0; // no padding
      }
      return fallback.fill(output, padding);
    }
  }
  
  @Override
  public BlockFillerShim getBlockFillerShim(FileSystem fs) throws IOException {
    // this is currently specialized by name, because the direct class access breaks
    if (fs.getClass().getName().equals("org.apache.hadoop.hdfs.DistributedFileSystem")) {
      try {
        // use class name lookups
        return (BlockFillerShim) Class.forName(
            "org.apache.orc.impl.HadoopShimsCurrent.VariableBlockFillerShim").newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        // fall through to old implementation
      }
    }
    return new ZeroFillerShim();
  }

}
