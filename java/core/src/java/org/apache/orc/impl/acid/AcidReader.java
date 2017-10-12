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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.orc.OrcFile;
import org.apache.orc.RecordReader;
import org.apache.orc.impl.ReaderImpl;

import java.io.IOException;

class AcidReader extends ReaderImpl {
  private final ValidTxnList validTxns;
  private final ParsedAcidDirectory baseDir;

  AcidReader(Path path, OrcFile.ReaderOptions options) throws IOException {
    super(path, options);
    this.validTxns = options.getValidTxns();
    this.baseDir = options.getAcidDir();
  }

  @Override
  public RecordReader rows() throws IOException {
    return rows(options());
  }

  @Override
  public RecordReader rows(Options options) throws IOException {
    return new AcidRecordReader(this, options, validTxns, baseDir);
  }
}
