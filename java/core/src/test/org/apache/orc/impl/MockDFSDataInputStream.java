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

import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.IdentityHashMap;

public class MockDFSDataInputStream extends InputStream implements Seekable, PositionedReadable, HasEnhancedByteBufferAccess {

  private ByteBuffer hdfsBlockBuffer;
  private int startPosition;
  private int currentPosition;
  private IdentityHashMap<ByteBuffer, ByteBufferPool> bufferStore = new IdentityHashMap(0);

  public MockDFSDataInputStream(ByteBuffer hdfsBlockBuffer, int startPosition) {
    this.hdfsBlockBuffer = hdfsBlockBuffer;
    this.startPosition = startPosition;
    this.currentPosition = startPosition;
  }

  @Override
  public int read() throws IOException {
    currentPosition++;
    return hdfsBlockBuffer.get();
  }

  @Override
  public ByteBuffer read(ByteBufferPool byteBufferPool, int i, EnumSet<ReadOption> enumSet) throws IOException, UnsupportedOperationException {
    ByteBuffer copy = hdfsBlockBuffer.duplicate();
    copy.limit(copy.position() + i);
    currentPosition += i;
    hdfsBlockBuffer.position(currentPosition - startPosition);
    bufferStore.put(copy, byteBufferPool);
    return copy;
  }

  @Override
  public void releaseBuffer(ByteBuffer byteBuffer) {
    Object val = bufferStore.remove(byteBuffer);
    if (val == null) {
      throw new IllegalArgumentException("tried to release a buffer that was not created by this stream, " + byteBuffer);
    }
  }

  @Override
  public void seek(long l) throws IOException {
    currentPosition = (int) l;
    hdfsBlockBuffer.position(currentPosition - startPosition);
  }

  @Override
  public long getPos() throws IOException {
    return currentPosition;
  }

  @Override
  public boolean seekToNewSource(long l) throws IOException {
    throw new RuntimeException("unsupported");
  }

  public boolean isAllReleased() {
    return bufferStore.isEmpty();
  }

  @Override
  public int read(long l, byte[] bytes, int i, int i1) throws IOException {
    throw new RuntimeException("unsupported");
  }

  @Override
  public void readFully(long l, byte[] bytes, int i, int i1) throws IOException {
    throw new RuntimeException("unsupported");
  }

  @Override
  public void readFully(long l, byte[] bytes) throws IOException {
    throw new RuntimeException("unsupported");
  }
}
