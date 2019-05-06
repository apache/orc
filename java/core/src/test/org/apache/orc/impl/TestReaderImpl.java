/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.FileFormatException;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TestVectorOrcFile;
import org.junit.Test;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class TestReaderImpl {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private final Path path = new Path("test-file.orc");
  private FSDataInputStream in;
  private int psLen;
  private ByteBuffer buffer;

  @Before
  public void setup() {
    in = null;
  }

  @Test
  public void testEnsureOrcFooterSmallTextFile() throws IOException {
    prepareTestCase("1".getBytes());
    thrown.expect(FileFormatException.class);
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooterLargeTextFile() throws IOException {
    prepareTestCase("This is Some Text File".getBytes());
    thrown.expect(FileFormatException.class);
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooter011ORCFile() throws IOException {
    prepareTestCase(composeContent(OrcFile.MAGIC, "FOOTER"));
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testEnsureOrcFooterCorrectORCFooter() throws IOException {
    prepareTestCase(composeContent("", OrcFile.MAGIC));
    ReaderImpl.ensureOrcFooter(in, path, psLen, buffer);
  }

  @Test
  public void testOptionSafety() throws IOException {
    Reader.Options options = new Reader.Options();
    String expected = options.toString();
    Configuration conf = new Configuration();
    Path path = new Path(TestVectorOrcFile.getFileFromClasspath
        ("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows(options);
    assertEquals(expected, options.toString());
  }

  private void prepareTestCase(byte[] bytes) {
    buffer = ByteBuffer.wrap(bytes);
    psLen = buffer.get(bytes.length - 1) & 0xff;
    in = new FSDataInputStream(new SeekableByteArrayInputStream(bytes));
  }

  private byte[] composeContent(String headerStr, String footerStr) throws CharacterCodingException {
    ByteBuffer header = Text.encode(headerStr);
    ByteBuffer footer = Text.encode(footerStr);
    int headerLen = header.remaining();
    int footerLen = footer.remaining() + 1;

    ByteBuffer buf = ByteBuffer.allocate(headerLen + footerLen);

    buf.put(header);
    buf.put(footer);
    buf.put((byte) footerLen);
    return buf.array();
  }

  private static final class SeekableByteArrayInputStream extends ByteArrayInputStream
          implements Seekable, PositionedReadable {

    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public void seek(long pos) throws IOException {
      this.reset();
      this.skip(pos);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } finally {
        seek(oldPos);
      }
      return nread;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
      int nread = 0;
      while (nread < length) {
        int nbytes = read(position + nread, buffer, offset + nread, length - nread);
        if (nbytes < 0) {
          throw new EOFException("End of file reached before reading fully.");
        }
        nread += nbytes;
      }
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }

  static class MockInputStream extends FSDataInputStream {
    MockFileSystem fs;

    public MockInputStream(MockFileSystem fs) {
      super(new SeekableByteArrayInputStream(new byte[0]));
      this.fs = fs;
    }

    public void close() {
      fs.removeStream(this);
    }
  }

  static class MockFileSystem extends FileSystem {
    final List<MockInputStream> streams = new ArrayList<>();

    public MockFileSystem(Configuration conf) {
      setConf(conf);
    }

    @Override
    public URI getUri() {
      try {
        return new URI("mock:///");
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("bad uri", e);
      }
    }

    @Override
    public FSDataInputStream open(Path path, int i) {
      MockInputStream result = new MockInputStream(this);
      streams.add(result);
      return result;
    }

    void removeStream(MockInputStream stream) {
      streams.remove(stream);
    }

    int streamCount() {
      return streams.size();
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission,
                                     boolean b, int i, short i1, long l,
                                     Progressable progressable) throws IOException {
      throw new IOException("Can't create");
    }

    @Override
    public FSDataOutputStream append(Path path, int i,
                                     Progressable progressable) throws IOException {
      throw new IOException("Can't append");
    }

    @Override
    public boolean rename(Path path, Path path1) {
      return false;
    }

    @Override
    public boolean delete(Path path, boolean b) {
      return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) {
      return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {
      // ignore
    }

    @Override
    public Path getWorkingDirectory() {
      return new Path("/");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) {
      return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) {
      return new FileStatus();
    }
  }

  @Test
  public void testClosingRowsFirst() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf);
    Reader reader = OrcFile.createReader(new Path("/foo"),
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(1, fs.streamCount());
    RecordReader rows = reader.rows();
    assertEquals(1, fs.streamCount());
    RecordReader rows2 = reader.rows();
    assertEquals(2, fs.streamCount());
    rows.close();
    assertEquals(1, fs.streamCount());
    rows2.close();
    assertEquals(0, fs.streamCount());
    reader.close();
    assertEquals(0, fs.streamCount());
  }

  @Test
  public void testClosingReaderFirst() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf);
    Reader reader = OrcFile.createReader(new Path("/foo"),
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(1, fs.streamCount());
    RecordReader rows = reader.rows();
    assertEquals(1, fs.streamCount());
    reader.close();
    assertEquals(1, fs.streamCount());
    rows.close();
    assertEquals(0, fs.streamCount());
  }

  @Test
  public void testClosingMultiple() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf);
    Reader reader = OrcFile.createReader(new Path("/foo"),
        OrcFile.readerOptions(conf).filesystem(fs));
    Reader reader2 = OrcFile.createReader(new Path("/bar"),
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(2, fs.streamCount());
    reader.close();
    assertEquals(1, fs.streamCount());
    reader2.close();
    assertEquals(0, fs.streamCount());
  }
}
