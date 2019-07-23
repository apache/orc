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
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
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

  public DirectDecompressor getDirectDecompressor(DirectCompressionType codec) {
    return HadoopShimsPre2_6.getDecompressor(codec);
  }

  @Override
  public ZeroCopyReaderShim getZeroCopyReader(FSDataInputStream in,
                                              ByteBufferPoolShim pool
                                              ) throws IOException {
    return ZeroCopyShims.getZeroCopyReader(in, pool);
  }

  @Override
  public boolean endVariableLengthBlock(OutputStream output) throws IOException {
    if (output instanceof HdfsDataOutputStream) {
      HdfsDataOutputStream hdfs = (HdfsDataOutputStream) output;
      hdfs.hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.END_BLOCK));
      return true;
    }
    return false;
  }

  @Override
  public KeyProvider getHadoopKeyProvider(Configuration conf,
                                          Random random) throws IOException {
    return HadoopShimsPre2_7.createKeyProvider(conf, random);
  }
}
