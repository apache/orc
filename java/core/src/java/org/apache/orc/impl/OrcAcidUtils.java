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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.Reader;

import java.io.IOException;
import org.apache.orc.shims.FileIO;


public class OrcAcidUtils extends OrcAcidUtilsCore {

  /**
   * Get the filename of the ORC ACID side file that contains the lengths
   * of the intermediate footers.
   * @param main the main ORC filename
   * @return the name of the side file
   */
  public static Path getSideFile(Path main) {
    return new Path(OrcAcidUtilsCore.getSideFile(main.toString()));
  }

  /**
   * Read the side file to get the last flush length.
   * @param fs the file system to use
   * @param deltaFile the path of the delta file
   * @return the maximum size of the file to use
   * @throws IOException
   */
  public static long getLastFlushLength(FileSystem fs,
                                        Path deltaFile) throws IOException {
    FileIO fileIO = HadoopShimsFactory.get().createFileIO(() -> fs);
    return getLastFlushLength(fileIO, deltaFile.toString());
  }

  public static AcidStats parseAcidStats(Reader reader) {
    return OrcAcidUtilsCore.parseAcidStats(reader);
  }
}
