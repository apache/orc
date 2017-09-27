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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AcidTestBase {
  protected Configuration conf;
  protected FileSystem fs;
  protected Path baseDir;

  @Before
  public void createFs() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    baseDir = new Path("target/testing-tmp/test-acid");
  }

  @After
  public void cleanupFs() throws IOException {
    fs.delete(baseDir, true);
  }

  protected FileStatus createFile(String name) throws IOException {
    Path parent = baseDir;
    Path namePath = new Path(name);
    if (namePath.getParent() != null) {
      Path dirPath = new Path(baseDir, namePath.getParent());
      fs.mkdirs(dirPath);
      parent = dirPath;
    }
    Path path = new Path(parent, namePath.getName());
    FSDataOutputStream out = fs.create(path);
    out.writeBytes("abc123");
    out.close();
    return fs.getFileStatus(path);
  }

  protected List<FileStatus> parcedAcidFileListToFileStatusList(List<ParsedAcidFile> files) {
    List<FileStatus> stats = new ArrayList<>(files.size());
    for (ParsedAcidFile file : files) stats.add(file.getFileStatus());
    return stats;
  }

}
