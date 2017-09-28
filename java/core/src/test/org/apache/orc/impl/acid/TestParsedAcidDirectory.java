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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestParsedAcidDirectory extends AcidTestBase {
  private List<FileStatus> expectedDeletes;

  @Before
  public void setExpectedDeletes() {
    expectedDeletes = new ArrayList<>();
  }

  @Test
  public void shouldBeUsed() throws IOException {
    Map<FileStatus, Boolean> fileStats = new HashMap<>();

    fileStats.put(createFile("000000_0"), false);
    fileStats.put(createFile("subdir/000000_3"), false);
    fileStats.put(createFile("_done"), false);
    fileStats.put(createFile("random"), false);
    fileStats.put(createFile("delta_025_025/bucket_0"), false);
    fileStats.put(createFile("delta_025_30/bucket_0"), false);
    fileStats.put(createFile("delta_025_30/bucket_1"), false);
    fileStats.put(createFile("delta_50_99/bucket_0"), true);
    fileStats.put(createFile("delta_100_100/bucket_0"), true);
    fileStats.put(createFile("delta_101_101/bucket_0"), false);
    fileStats.put(createFile("base_10/bucket_0"), false);
    fileStats.put(createFile("base_49/bucket_0"), true);
    fileStats.put(createFile("base_49/bucket_1"), true);

    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    for (Map.Entry<FileStatus, Boolean> e : fileStats.entrySet()) {
      Assert.assertEquals(e.getKey().getPath().toString(),
          e.getValue(), dir.shouldBeUsedForInput(e.getKey()));
    }
  }

  @Test
  public void relevantDeletesOrig() throws IOException {
    FileStatus orig = createFile("000000_1");
    expectedDeletes.add(createFile("delete_delta_025_025/bucket_0"));
    expectedDeletes.add(createFile("delete_delta_026_027/bucket_1"));

    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    List<FileStatus> deleteFiles = parcedAcidFileListToFileStatusList(dir.getRelevantDeletes(orig));
    Collections.sort(expectedDeletes);
    Collections.sort(deleteFiles);
    Assert.assertEquals(expectedDeletes, deleteFiles);
  }

  @Test
  public void relevantDeletesBase() throws IOException {
    FileStatus base = createFile("base_10/bucket_1");
    expectedDeletes.add(createFile("delete_delta_025_025/bucket_0"));
    expectedDeletes.add(createFile("delete_delta_026_027/bucket_1"));

    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    List<FileStatus> deleteFiles = parcedAcidFileListToFileStatusList(dir.getRelevantDeletes(base));
    Collections.sort(expectedDeletes);
    Collections.sort(deleteFiles);
    Assert.assertEquals(expectedDeletes, deleteFiles);
  }

  @Test
  public void relevantDeletesDeltaSingleTransaction() throws IOException {
    createFile("base_10/000000");
    FileStatus delta = createFile("delta_025_025/bucket_0");
    expectedDeletes.add(createFile("delete_delta_025_025/bucket_0"));
    expectedDeletes.add(createFile("delete_delta_025_025/bucket_1"));

    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    List<FileStatus> deleteFiles = parcedAcidFileListToFileStatusList(dir.getRelevantDeletes(delta));
    Collections.sort(expectedDeletes);
    Collections.sort(deleteFiles);
    Assert.assertEquals(expectedDeletes, deleteFiles);
  }

  @Test
  public void relevantDeletesDeltaMinorCompacted() throws IOException {
    createFile("base_10/000000");
    FileStatus delta = createFile("delta_025_030/bucket_0");
    expectedDeletes.add(createFile("delete_delta_025_030/bucket_0"));
    expectedDeletes.add(createFile("delete_delta_025_030/bucket_1"));

    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    List<FileStatus> deleteFiles = parcedAcidFileListToFileStatusList(dir.getRelevantDeletes(delta));
    Collections.sort(expectedDeletes);
    Collections.sort(deleteFiles);
    Assert.assertEquals(expectedDeletes, deleteFiles);
  }

  @Test
  public void relevantDeletesDeltaDeltaBefore() throws IOException {
    createFile("base_10/000000");
    FileStatus delta24_27 = createFile("delta_024_027/bucket_0");
    FileStatus delta28_28 = createFile("delta_028_028/bucket_0");
    createFile("delta_028_028/bucket_1");
    createFile("delta_029_029/bucket_0");
    createFile("delta_029_029/bucket_1");
    expectedDeletes.add(createFile("delete_delta_029_029/bucket_0"));
    expectedDeletes.add(createFile("delete_delta_029_029/bucket_1"));

    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    List<FileStatus> deleteFiles =
        parcedAcidFileListToFileStatusList(dir.getRelevantDeletes(delta24_27));
    Collections.sort(expectedDeletes);
    Collections.sort(deleteFiles);
    Assert.assertEquals(expectedDeletes, deleteFiles);

    deleteFiles = parcedAcidFileListToFileStatusList(dir.getRelevantDeletes(delta28_28));
    Collections.sort(expectedDeletes);
    Collections.sort(deleteFiles);
    Assert.assertEquals(expectedDeletes, deleteFiles);
  }

  @Test
  public void relevantDeletesDeltaDeleteBefore() throws IOException {
    createFile("base_10/000000");
    createFile("delta_020_024/bucket_0");
    createFile("delta_020_024/bucket_1");
    FileStatus delta25_29 = createFile("delta_025_029/bucket_0");
    FileStatus delta30_30 = createFile("delta_030_030/bucket_0");
    createFile("delete_delta_020_024/bucket_0");
    createFile("delete_delta_020_024/bucket_1");

    ParsedAcidDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    List<FileStatus> deleteFiles =
        parcedAcidFileListToFileStatusList(dir.getRelevantDeletes(delta25_29));
    Assert.assertTrue(deleteFiles.isEmpty());

    deleteFiles = parcedAcidFileListToFileStatusList(dir.getRelevantDeletes(delta30_30));
    Assert.assertTrue(deleteFiles.isEmpty());
  }
}
