/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl.acid;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;

import java.io.IOException;
import java.util.List;

@InterfaceAudience.Private
public class OrcFileAcidHelper {

  public static Reader getReader(Path path, OrcFile.ReaderOptions options) throws IOException {
    if (options.getValidTxns() == null) {
      throw new IOException("You must set the valid transaction list before calling " +
          "createReaderForAcidFile");
    }

    FileStatus stat = options.getFilesystem().getFileStatus(path);

    // If they haven't parsed the directory, do that now
    if (options.getAcidDir() == null) {
      // Make the path absolute
      Path p = path.isAbsolute() ? path : stat.getPath();
      // If this is an acid file, the relevant directory should always be two levels up
      // (dir/[base|delta]/bucket_x).  If it's a pre-acid file than it should be one level up
      Path baseDir = AcidConstants.BUCKET_PATTERN.matcher(p.getName()).matches()
          ? p.getParent().getParent() : p.getParent();
      options.acidDir(AcidDirectoryParser.parseDirectory(baseDir, options.getConfiguration(),
          options.getValidTxns()));
    }

    // See if we should be reading this file at all.  Assuming the reader doesn't understand the
    // Acid file layout we may be passed files that aren't relevant
    if (options.getAcidDir().shouldBeUsedForInput(stat)) {
      List<ParsedAcidFile> deleteDeltas = options.getAcidDir().getRelevantDeletes(stat);
      return new AcidReader(path, options, deleteDeltas, options.getValidTxns());
    } else {
      return NullReader.getNullReader();
    }

  }


}
