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
package org.apache.orc.impl.acid;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCompactorTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAcidDirectoryParser {
  private Configuration conf;
  private FileSystem fs;
  private Path baseDir;
  private Set<String> expectedInputs;
  private Set<String> expectedDeletes;
  private Set<String> expectedPreAcid;

  @Before
  public void createFs() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    baseDir = new Path("target/testing-tmp/test-acid-directory-parser");
  }

  @Before
  public void initExpectedFiles() {
    expectedInputs = new HashSet<>();
    expectedDeletes = new HashSet<>();
    expectedPreAcid = new HashSet<>();
  }

  @After
  public void cleanupFs() throws IOException {
    fs.delete(baseDir, true);
  }

  private Path createFile(String name) throws IOException {
    Path parent = baseDir;
    Path namePath = new Path(name);
    if (namePath.getParent() != null) {
      Path dirPath = new Path(baseDir, namePath.getParent());
      fs.mkdirs(dirPath);
      parent = dirPath;
    }
    Path path = new Path(parent, name);
    FSDataOutputStream out = fs.create(path);
    out.writeBytes("abc123");
    out.close();
    return path;
  }

  private void checkExpected(AcidVersionedDirectory dir) {
    checkExpected(expectedInputs, dir.getInputFiles(), "inputs");
    checkExpected(expectedDeletes, dir.getDeleteFiles(), "deletes");
    checkExpected(expectedPreAcid, dir.getPreAcidFiles(), "pre-acids");
  }

  private void checkExpected(Set<String> expected, List<FileStatus> files, String name) {
    Assert.assertEquals("Found wrong number of " + name, expected.size(), files.size());
    for (FileStatus stat : files) {
      Assert.assertTrue("Found unexpected file in " + name + ": " + stat.getPath().getName(),
          expected.contains(stat.getPath().getName()));
    }
  }

  @Test
  public void testOriginal() throws IOException {
    expectedPreAcid.add(createFile("000000_0").getName());
    expectedPreAcid.add(createFile("000000_1").getName());
    expectedPreAcid.add(createFile("000000_2").getName());
    expectedPreAcid.add(createFile("subdir/000000_3").getName());
    createFile("_done");
    expectedPreAcid.add(createFile("random").getName());

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":")));
  }

  @Test
  public void testOriginalDeltas() throws Exception {
    expectedPreAcid.add(createFile("000000_0").getName());
    expectedPreAcid.add(createFile("000000_1").getName());
    expectedPreAcid.add(createFile("000000_2").getName());
    expectedPreAcid.add(createFile("subdir/000000_3").getName());
    createFile("_done");
    expectedPreAcid.add(createFile("random").getName());
    createFile("delta_025_025/bucket_0");
    createFile("delta_029_029/bucket_0");
    expectedInputs.add(createFile("delta_025_30/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_50_99/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_100_100/bucket_0").getParent().getName());
    createFile("delta_101_101/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":")));
  }

  @Test
  public void testBaseDeltas() throws Exception {
    createFile("base_5/bucket_0");
    createFile("base_10/bucket_0");
    expectedInputs.add(createFile("base_49/bucket_0").getParent().getName());
    createFile("delta_025_025/bucket_0");
    createFile("delta_029_029/bucket_0");
    createFile("delta_025_030/bucket_0");
    expectedInputs.add(createFile("delta_050_105/bucket_0").getParent().getName());
    createFile("delta_090_120/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
             new ValidReadTxnList("100:" + Long.MAX_VALUE + ":")));
  }

  @Test
  public void testBestBase() throws Exception {
    createFile("base_5/bucket_0");
    createFile("base_10/bucket_0");
    createFile("base_25/bucket_0");
    createFile("delta_098_100/bucket_0");
    expectedInputs.add(createFile("base_100/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_120_130/bucket_0").getParent().getName());
    createFile("base_200/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("150:" + Long.MAX_VALUE + ":")));
  }

  @Test
  public void testOverlapingDelta() throws Exception {
    expectedInputs.add(createFile("delta_0000063_63/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_000062_62/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_00061_61/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_40_60/bucket_0").getParent().getName());
    createFile("delta_0060_60/bucket_0");
    createFile("delta_052_55/bucket_0");
    expectedInputs.add(createFile("base_50/bucket_0").getParent().getName());

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":")));
  }

  /*
   * Hive 1.3.0 delta dir naming scheme which supports multi-statement txns
   */
  @Test
  public void testOverlapingDelta2() throws Exception {
    expectedInputs.add(createFile("delta_0000063_63_0/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_000062_62_0/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_000062_62_3/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_00061_61_0/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_40_60/bucket_0").getParent().getName());
    createFile("delta_0060_60_1/bucket_0");
    createFile("delta_0060_60_4/bucket_0");
    createFile("delta_0060_60_7/bucket_0");
    createFile("delta_052_55/bucket_0");
    createFile("delta_058_58/bucket_0");
    expectedInputs.add(createFile("base_50/bucket_0").getParent().getName());

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":")));
  }

  @Test
  public void deltasWithOpenTxnInRead() throws Exception {
    expectedInputs.add(createFile("delta_1_1/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_2_5/bucket_0").getParent().getName());

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:4:4")));
  }

  @Test
  public void deltasWithOpenTxnInRead2() throws Exception {
    expectedInputs.add(createFile("delta_1_1/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_2_5/bucket_0").getParent().getName());
    createFile("delta_4_4_1/bucket_0");
    createFile("delta_4_4_3/bucket_0");
    createFile("delta_101_101_1/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:4:4")));
  }

  @Test
  public void deltasWithOpenTxnsNotInCompact() throws Exception {
    expectedInputs.add(createFile("delta_1_1/bucket_0").getParent().getName());
    createFile("delta_2_5/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidCompactorTxnList("4:" + Long.MAX_VALUE)));
  }

  @Test
  public void deltasWithOpenTxnsNotInCompact2() throws Exception {
    expectedInputs.add(createFile("delta_1_1/bucket_0").getParent().getName());
    createFile("delta_2_5/bucket_0");
    createFile("delta_2_5/bucket_0" + AcidConstants.DELTA_SIDE_FILE_SUFFIX);
    createFile("delta_6_10/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidCompactorTxnList("3:" + Long.MAX_VALUE)));
  }

  @Test
  public void testBaseWithDeleteDeltas() throws Exception {
    createFile("base_5/bucket_0");
    createFile("base_10/bucket_0");
    expectedInputs.add(createFile("base_49/bucket_0").getParent().getName());
    createFile("delta_025_025/bucket_0");
    createFile("delta_029_029/bucket_0");
    createFile("delete_delta_029_029/bucket_0");
    createFile("delta_025_030/bucket_0");
    createFile("delete_delta_025_030/bucket_0");
    expectedInputs.add(createFile("delta_050_105/bucket_0").getParent().getName());
    expectedDeletes.add(createFile("delete_delta_050_105/bucket_0").getParent().getName());
    // The delete_delta_110_110 should not be read because it is greater than the high watermark.
    createFile("delete_delta_110_110/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":")));
  }

  @Test
  public void testOverlapingDeltaAndDeleteDelta() throws Exception {
    expectedInputs.add(createFile("delta_0000063_63/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_000062_62/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_00061_61/bucket_0").getParent().getName());
    expectedDeletes.add(createFile("delete_delta_00064_64/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_40_60/bucket_0").getParent().getName());
    expectedDeletes.add(createFile("delete_delta_40_60/bucket_0").getParent().getName());
    createFile("delta_0060_60/bucket_0");
    createFile("delta_052_55/bucket_0");
    createFile("delete_delta_052_55/bucket_0");
    expectedInputs.add(createFile("base_50/bucket_0").getParent().getName());

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":")));
  }

  // This test checks that if we have a minor compacted delta for the txn range [40,60]
  // then it will make any delete delta in that range as obsolete.
  @Test
  public void testMinorCompactedDeltaMakesInBetweenDeleteDeltaObsolete() throws Exception {
    expectedInputs.add(createFile("delta_40_60/bucket_0").getParent().getName());
    createFile("delete_delta_50_50/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":")));
  }

  // This tests checks that appropriate delta and delete_deltas are included when minor
  // compactions specifies a valid open txn range.
  @Test
  public void deltasAndDeleteDeltasWithOpenTxnsNotInCompact() throws Exception {
    expectedInputs.add(createFile("delta_1_1/bucket_0").getParent().getName());
    expectedDeletes.add(createFile("delete_delta_2_2/bucket_0").getParent().getName());
    createFile("delta_2_5/bucket_0");
    createFile("delete_delta_2_5/bucket_0");
    createFile("delta_2_5/bucket_0" + AcidConstants.DELTA_SIDE_FILE_SUFFIX);
    createFile("delete_delta_7_7/bucket_0");
    createFile("delta_6_10/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidCompactorTxnList("4:" + Long.MAX_VALUE + ":")));
  }

  @Test
  public void deleteDeltasWithOpenTxnInRead() throws Exception {
    expectedInputs.add(createFile("delta_1_1/bucket_0").getParent().getName());
    expectedInputs.add(createFile("delta_2_5/bucket_0").getParent().getName());
    expectedDeletes.add(createFile("delete_delta_2_5/bucket_0").getParent().getName());
    // Note that delete_delta_3_3 should not be read, when a minor compacted
    // [delete_]delta_2_5 is present.
    createFile("delete_delta_3_3/bucket_0");
    createFile("delta_4_4_1/bucket_0");
    createFile("delta_4_4_3/bucket_0");
    createFile("delta_101_101_1/bucket_0");

    checkExpected(AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:4:4")));
  }
}