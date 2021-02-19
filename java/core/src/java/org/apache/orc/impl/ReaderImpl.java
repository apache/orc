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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;

public class ReaderImpl extends ReaderImplCore implements Reader {

  /**
  * Constructor that let's the user specify additional options.
   * @param path pathname for file
   * @param options options for reading
   */
  public ReaderImpl(Path path, OrcFile.ReaderOptions options) throws IOException {
    super(path == null ? null : path.toString(), options);
  }

  @Override
  public Reader.Options options() {
    return new Reader.Options(conf);
  }

  protected OrcTail extractFileTail(FileSystem fs, Path path,
      long maxFileLength) throws IOException {
    return super.extractFileTail(HadoopShimsFactory.get().createFileIO(() -> fs), path.toString(),
        maxFileLength);
  }

  protected static void ensureOrcFooter(FSDataInputStream in,
      Path path,
      int psLen,
      ByteBuffer buffer) throws IOException {
    ReaderImplCore.ensureOrcFooter(
        new HadoopShimsPre2_3.HadoopSeekableInputStream(in), path.toString(), psLen, buffer);
  }
}
