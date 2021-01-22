/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import org.apache.orc.core.Reader;
import org.apache.orc.shims.FileIO;

public class OrcAcidUtilsCore {
  public static final String ACID_STATS = "hive.acid.stats";
  public static final String DELTA_SIDE_FILE_SUFFIX = "_flush_length";

  /**
   * Get the filename of the ORC ACID side file that contains the lengths
   * of the intermediate footers.
   * @param main the main ORC filename
   * @return the name of the side file
   */
  public static String getSideFile(String main) {
    return main + DELTA_SIDE_FILE_SUFFIX;
  }

  /**
   * Read the side file to get the last flush length.
   * @param fs the file system to use
   * @param deltaFile the path of the delta file
   * @return the maximum size of the file to use
   * @throws IOException
   */
  public static long getLastFlushLength(FileIO fs,
                                        String deltaFile) throws IOException {
    String lengths = getSideFile(deltaFile);
    long result = Long.MAX_VALUE;
    if(!fs.exists(lengths)) {
      return result;
    }
    try (DataInputStream stream = new DataInputStream(
        fs.createInputFile(lengths).getInputStream())) {
      result = -1;
      while (stream.available() > 0) {
        result = stream.readLong();
      }
      return result;
    } catch (IOException ioe) {
      return result;
    }
  }

  private static final CharsetDecoder utf8Decoder =
      StandardCharsets.UTF_8.newDecoder();

  public static AcidStats parseAcidStats(Reader reader) {
    if (reader.hasMetadataValue(ACID_STATS)) {
      try {
        ByteBuffer val = reader.getMetadataValue(ACID_STATS).duplicate();
        return new AcidStats(utf8Decoder.decode(val).toString());
      } catch (CharacterCodingException e) {
        throw new IllegalArgumentException("Bad string encoding for " +
            ACID_STATS, e);
      }
    } else {
      return null;
    }
  }

}
