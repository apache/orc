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

import org.apache.orc.CompressionCodec;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.UUID;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;

public class IsalCodec implements CompressionCodec, DirectDecompressionCodec {
  private static final HadoopShims SHIMS = HadoopShims.Factory.get();
  // Note: shim path does not care about levels and strategies (only used for decompression).
  private HadoopShims.DirectDecompressor decompressShim = null;
  private Boolean direct = null;

  // HACK - Use this as a global lock in the JNI layer
  private static Class clazz = ZlibCodec.class;
  private long stream;
  private boolean finished;

  private int level;
  private int strategy;

  private static boolean isLoaded = false;
  private static File nativeLibFile = null;

  static {
    System.out.println("before load isal codec ");
    //System.load("/usr/lib/libIsalCodec.so");
    if (!isLoaded) {
      try {
        nativeLibFile = findNativeLibrary();
        if (nativeLibFile != null) {
          // Load extracted or specified snappyjava native library.
          System.load(nativeLibFile.getAbsolutePath());
        } else {
          throw new IOException("can not load Native Isal Codec");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      System.out.println("after load isal codec ");
      initIDs();
      System.out.println("after initIDs ");
      isLoaded = true;
    }
  }

  public IsalCodec() {
    level = Deflater.DEFAULT_COMPRESSION;
    strategy = Deflater.DEFAULT_STRATEGY;
  }

  private IsalCodec(int level, int strategy) {
    this.level = level;
    this.strategy = strategy;
  }

  @Override
  public boolean compress(ByteBuffer in, ByteBuffer out,
                          ByteBuffer overflow) throws IOException {
    int length = in.remaining();

    stream = deflateinit(level, strategy, in.array(), in.arrayOffset() + in.position(), length);

    int outSize = 0;
    int size = 0;
    int offset = out.arrayOffset() + out.position();
    //int remain = out.remaining();
    //System.out.printf("begin deflate: in  offset %d length %d. out, offset %d length %d\n",
    //        in.arrayOffset() + in.position(), in.remaining(),
    //       out.arrayOffset() + out.position(), out.remaining());
    finished = false;
    while(true != finished)
    {
      size = deflate(stream, 0, out.array(), offset, out.remaining());
      if(size < 0)
      {
        end(stream);
        return false;
      }
      out.position(size + out.position());
      outSize += size;
      offset += size;
      //System.out.printf("finish deflate: size %d. in  offset %d length %d. out, offset %d length %d\n",
      //        size, in.arrayOffset() + in.position(), in.remaining(),
      //        out.arrayOffset() + out.position(), out.remaining());
      // if we run out of space in the out buffer, use the overflow
      if((true != finished && overflow == null) || outSize >= in.remaining()) {
        System.out.println("!!!!!!!!!!!!!!!!!!overflow !!!!!!!!!!!!!!!!. finished flag " + finished + " outSize " + outSize + " in remain " + in.remaining());
        if(overflow == null) {
          System.out.println("!!!!!!!!!!overflow is null");
        }
        end(stream);
        return false;
      }

      if(true != finished && overflow != null) {
        out = overflow;
        overflow = null;
        offset = out.arrayOffset() + out.position();
      }
        //remain = 262144;
    }
    end(stream);

    //System.out.printf("after deflate: length %d, outSize %d\n", length, outSize);
    return true;
  }

  @Override
  public void decompress(ByteBuffer in, ByteBuffer out) throws IOException {

    int outSize = 0;
   // int length = in.remaining();

    if(in.isDirect() && out.isDirect()) {
      System.out.println("isal codec enter direct decompress");
      throw new IllegalArgumentException("No directDecompress method in ISA-L codec");
      //directDecompress(in, out);
      //return;
    }
    stream = inflateinit(level, strategy, in.array(), in.arrayOffset() + in.position(), in.remaining());

    try {
      while (0 >= outSize) {

        int count = inflate(stream, 0, out.array(),
                out.arrayOffset() + out.position(),
                out.remaining());
        if (count >= 0) {
          outSize += count;
          out.position(count + out.position());
        } else {
          throw new DataFormatException("inflate error return");
        }
      }
    } catch (DataFormatException dfe) {
      throw new IOException("Bad compression data", dfe);
    }
    out.flip();
    end(stream);
    in.position(in.limit());
  }

  @Override
  public boolean isAvailable() {
    if (direct == null) {
      // see nowrap option in new Inflater(boolean) which disables zlib headers
      try {
        ensureShim();
        direct = (decompressShim != null);
      } catch (UnsatisfiedLinkError ule) {
        direct = Boolean.valueOf(false);
      }
    }
    return direct.booleanValue();
  }

  private void ensureShim() {
    if (decompressShim == null) {
      decompressShim = SHIMS.getDirectDecompressor(
          HadoopShims.DirectCompressionType.ZLIB_NOHEADER);
    }
  }

  @Override
  public void directDecompress(ByteBuffer in, ByteBuffer out) throws IOException {
    ensureShim();
    decompressShim.decompress(in, out);
    out.flip(); // flip for read
  }

  @Override
  public CompressionCodec modify(/* @Nullable */ EnumSet<Modifier> modifiers) {

    if (modifiers == null) {
      return this;
    }

    int l = this.level;
    int s = this.strategy;

    for (Modifier m : modifiers) {
      switch (m) {
      case BINARY:
        /* filtered == less LZ77, more huffman */
        s = Deflater.FILTERED;
        break;
      case TEXT:
        s = Deflater.DEFAULT_STRATEGY;
        break;
      case FASTEST:
        // deflate_fast looking for 8 byte patterns
        l = Deflater.BEST_SPEED;
        break;
      case FAST:
        // deflate_fast looking for 16 byte patterns
        l = Deflater.BEST_SPEED + 1;
        break;
      case DEFAULT:
        // deflate_slow looking for 128 byte patterns
        l = Deflater.DEFAULT_COMPRESSION;
        break;
      default:
        break;
      }
    }
    return new IsalCodec(l, s);
  }

  @Override
  public void reset() {
    level = Deflater.DEFAULT_COMPRESSION;
    strategy = Deflater.DEFAULT_STRATEGY;
    if (decompressShim != null) {
      decompressShim.reset();
    }
  }

  @Override
  public void close() {
    if (decompressShim != null) {
      decompressShim.end();
    }
  }

  private static boolean contentsEquals(InputStream in1, InputStream in2)
          throws IOException
  {
    if (!(in1 instanceof BufferedInputStream)) {
      in1 = new BufferedInputStream(in1);
    }
    if (!(in2 instanceof BufferedInputStream)) {
      in2 = new BufferedInputStream(in2);
    }

    int ch = in1.read();
    while (ch != -1) {
      int ch2 = in2.read();
      if (ch != ch2) {
        return false;
      }
      ch = in1.read();
    }
    int ch2 = in2.read();
    return ch2 == -1;
  }

  /**
   * Extract the specified library file to the target folder
   *
   * @param libFolderForCurrentOS
   * @param libraryFileName
   * @param targetFolder
   * @return
   */
  private static File extractLibraryFile(String libFolderForCurrentOS, String libraryFileName, String targetFolder)
  {
    String nativeLibraryFilePath = libFolderForCurrentOS + "/" + libraryFileName;

    // Attach UUID to the native library file to ensure multiple class loaders can read the libsnappy-java multiple times.
    String uuid = UUID.randomUUID().toString();
    String extractedLibFileName = String.format("snappy-%s-%s", uuid, libraryFileName);
    File extractedLibFile = new File(targetFolder, extractedLibFileName);

    try {
      // Extract a native library file into the target directory
      InputStream reader = null;
      FileOutputStream writer = null;
      try {
        reader = IsalCodec.class.getResourceAsStream(nativeLibraryFilePath);
        try {
          writer = new FileOutputStream(extractedLibFile);

          byte[] buffer = new byte[8192];
          int bytesRead = 0;
          while ((bytesRead = reader.read(buffer)) != -1) {
            writer.write(buffer, 0, bytesRead);
          }
        }
        finally {
          if (writer != null) {
            writer.close();
          }
        }
      }
      finally {
        if (reader != null) {
          reader.close();
        }

        // Delete the extracted lib file on JVM exit.
        extractedLibFile.deleteOnExit();
      }

      // Set executable (x) flag to enable Java to load the native library
      boolean success = extractedLibFile.setReadable(true) &&
              extractedLibFile.setWritable(true, true) &&
              extractedLibFile.setExecutable(true);
      if (!success) {
        // Setting file flag may fail, but in this case another error will be thrown in later phase
      }

      // Check whether the contents are properly copied from the resource folder
      {
        InputStream nativeIn = null;
        InputStream extractedLibIn = null;
        try {
          nativeIn = IsalCodec.class.getResourceAsStream(nativeLibraryFilePath);
          extractedLibIn = new FileInputStream(extractedLibFile);

          if (!contentsEquals(nativeIn, extractedLibIn)) {
            throw new IOException(String.format("Failed to write a native library file at %s", extractedLibFile));
          }
        }
        finally {
          if (nativeIn != null) {
            nativeIn.close();
          }
          if (extractedLibIn != null) {
            extractedLibIn.close();
          }
        }
      }

      return new File(targetFolder, extractedLibFileName);
    }
    catch (IOException e) {
      e.printStackTrace(System.err);
      return null;
    }
  }

  static File findNativeLibrary()
  {
    // Load an OS-dependent native library inside a jar file
    String snappyNativeLibraryName = "libIsalCodec.so";
    String snappyNativeLibraryPath = "/org/apache/orc/native";
    boolean hasNativeLib = hasResource(snappyNativeLibraryPath + "/" + snappyNativeLibraryName);

    if (!hasNativeLib) {
      try {
        throw new  IOException("no native Isal Codec library is found");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // Temporary folder for the native lib. Use the value of org.xerial.snappy.tempdir or java.io.tmpdir
    File tempFolder = new File(System.getProperty("/org/apache/orc/tmpdir", System.getProperty("java.io.tmpdir")));
    if (!tempFolder.exists()) {
      boolean created = tempFolder.mkdirs();
      if (!created) {
        // if created == false, it will fail eventually in the later part
      }
    }

    // Extract and load a native library inside the jar file
    return extractLibraryFile(snappyNativeLibraryPath, snappyNativeLibraryName, tempFolder.getAbsolutePath());
  }

  private static boolean hasResource(String path)
  {
    return IsalCodec.class.getResource(path) != null;
  }


  private native static void initIDs();
  private native static long deflateinit(int level, int strategy, byte[] b, int off, int len);
  private native static long inflateinit(int level, int strategy, byte[] b, int off, int len);
  private native int deflate(long strm, int flush, byte[] in, int off, int len);
  private native int inflate(long strm, int flush, byte[] in, int off, int len);
  private native static void end(long strm);
}
