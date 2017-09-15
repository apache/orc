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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestAcidDirectoryParser {
  private static int pathSeq = 0;

  private Configuration conf;
  private FileSystem fs;
  private Path baseDir;

  @Before
  public void createFs() throws IOException {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    baseDir = new Path("test-acid-directory-parser_" + pathSeq++);
    fs.deleteOnExit(baseDir);
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

  @Test
  public void testOriginal() throws IOException {
    Set<String> expected = new HashSet<>();
    expected.add(createFile("000000_0").getName());
    expected.add(createFile("000000_1").getName());
    expected.add(createFile("000000_2").getName());
    expected.add(createFile("subdir/000000_3").getName());
    createFile("_done");
    expected.add(createFile("random").getName());

    AcidVersionedDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));
    Assert.assertEquals(0, dir.getInputFiles().size());
    Assert.assertEquals(0, dir.getDeleteFiles().size());
    Assert.assertEquals(expected.size(), dir.getPreAcidFiles().size());

    for (FileStatus stat : dir.getPreAcidFiles()) {
      Assert.assertTrue("Found unexpected file " + stat.getPath().getName(),
          expected.contains(stat.getPath().getName()));
    }
  }

  @Test
  public void testOriginalDeltas() throws Exception {
    Set<String> expectedOriginals = new HashSet<>();
    Set<String> expectedDeltas = new HashSet<>();
    expectedOriginals.add(createFile("000000_0").getName());
    expectedOriginals.add(createFile("000000_1").getName());
    expectedOriginals.add(createFile("000000_2").getName());
    expectedOriginals.add(createFile("subdir/000000_3").getName());
    createFile("_done");
    expectedOriginals.add(createFile("random").getName());
    createFile("delta_025_025/bucket_0");
    createFile("delta_029_029/bucket_0");
    expectedDeltas.add(createFile("delta_025_30/bucket_0").getParent().getName());
    expectedDeltas.add(createFile("delta_50_99/bucket_0").getParent().getName());
    expectedDeltas.add(createFile("delta_100_100/bucket_0").getParent().getName());
    createFile("delta_101_101/bucket_0");

    AcidVersionedDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    Assert.assertEquals(expectedDeltas.size(), dir.getInputFiles().size());
    for (FileStatus stat : dir.getInputFiles()) {
      Assert.assertTrue("Found unexpected delta file " + stat.getPath().getName(),
          expectedDeltas.contains(stat.getPath().getName()));
    }

    Assert.assertEquals(expectedOriginals.size(), dir.getPreAcidFiles().size());
    for (FileStatus stat : dir.getPreAcidFiles()) {
      Assert.assertTrue("Found unexpected original file " + stat.getPath().getName(),
          expectedOriginals.contains(stat.getPath().getName()));
    }

    Assert.assertTrue(dir.getDeleteFiles().isEmpty());
  }

  @Test
  public void testBaseDeltas() throws Exception {
    Set<String> expectedDeltas = new HashSet<>();
    createFile("base_5/bucket_0");
    createFile("base_10/bucket_0");
    String expectedBase = createFile("base_49/bucket_0").getParent().getName();
    createFile("delta_025_025/bucket_0");
    createFile("delta_029_029/bucket_0");
    createFile("delta_025_030/bucket_0");
    expectedDeltas.add(createFile("delta_050_105/bucket_0").getParent().getName());
    createFile("delta_090_120/bucket_0");

    AcidVersionedDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
             new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));

    Assert.assertEquals(expectedDeltas.size() + 1, dir.getInputFiles().size());
    boolean foundExpectedBase = false;
    for (FileStatus stat : dir.getInputFiles()) {
      if (stat.getPath().getName().equals(expectedBase)) {
        foundExpectedBase = true;
      } else {
        Assert.assertTrue("Found expected file in inputs " + stat.getPath(),
            expectedDeltas.contains(stat.getPath().getName()));
      }
    }
    Assert.assertTrue("Failed to find expected base " + expectedBase, foundExpectedBase);

    Assert.assertEquals(0, dir.getPreAcidFiles().size());
    Assert.assertEquals(0, dir.getDeleteFiles().size());
  }

  @Test
  public void testBestBase() throws Exception {
    Set<String> expectedDeltas = new HashSet<>();
    createFile("base_5/bucket_0");
    createFile("base_10/bucket_0");
    createFile("base_25/bucket_0");
    createFile("delta_098_100/bucket_0");
    String expectedBase = createFile("base_100/bucket_0").getParent().getName();
    expectedDeltas.add(createFile("delta_120_130/bucket_0").getParent().getName());
    createFile("base_200/bucket_0");

    AcidVersionedDirectory dir = AcidDirectoryParser.parseDirectory(baseDir, conf,
        new ValidReadTxnList("150:" + Long.MAX_VALUE + ":"));

    Assert.assertEquals(expectedDeltas.size() + 1, dir.getInputFiles().size());
    boolean foundExpectedBase = false;
    for (FileStatus stat : dir.getInputFiles()) {
      if (stat.getPath().getName().equals(expectedBase)) {
        foundExpectedBase = true;
      } else {
        Assert.assertTrue("Found expected file in inputs " + stat.getPath(),
            expectedDeltas.contains(stat.getPath().getName()));
      }
    }
    Assert.assertTrue("Failed to find expected base " + expectedBase, foundExpectedBase);

    Assert.assertEquals(0, dir.getPreAcidFiles().size());
    Assert.assertEquals(0, dir.getDeleteFiles().size());
  }

  /*
  @Test
  public void testObsoleteOriginals() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000000_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/000001_1", 500, new byte[0]));
    Path part = new MockPath(fs, "/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidReadTxnList("150:" + Long.MAX_VALUE + ":"));
    // Obsolete list should include the two original bucket files, and the old base dir
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(3, obsolete.size());
    assertEquals("mock:/tbl/part1/base_5", obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/base_10", dir.getBaseDirectory().toString());
  }

  @Test
  public void testOverlapingDelta() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_0000063_63/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_000062_62/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_00061_61/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_0060_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));
    assertEquals("mock:/tbl/part1/base_50", dir.getBaseDirectory().toString());
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(2, obsolete.size());
    assertEquals("mock:/tbl/part1/delta_052_55", obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0060_60", obsolete.get(1).getPath().toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(4, delts.size());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_00061_61", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62", delts.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0000063_63", delts.get(3).getPath().toString());
  }

  /**
   * Hive 1.3.0 delta dir naming scheme which supports multi-statement txns
   * @throws Exception
   */
  /*
  @Test
  public void testOverlapingDelta2() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
      new MockFile("mock:/tbl/part1/delta_0000063_63_0/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_000062_62_0/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_000062_62_3/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_00061_61_0/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_0060_60_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_0060_60_4/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_0060_60_7/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_058_58/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
      AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));
    assertEquals("mock:/tbl/part1/base_50", dir.getBaseDirectory().toString());
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(5, obsolete.size());
    assertEquals("mock:/tbl/part1/delta_052_55", obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_058_58", obsolete.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0060_60_1", obsolete.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0060_60_4", obsolete.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0060_60_7", obsolete.get(4).getPath().toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(5, delts.size());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_00061_61_0", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62_0", delts.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62_3", delts.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0000063_63_0", delts.get(4).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnInRead() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:4:4"));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_2_5", delts.get(1).getPath().toString());
  }

  /**
   * @since 1.3.0
   * @throws Exception
   */
  /*
  @Test
  public void deltasWithOpenTxnInRead2() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
      new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_4_4_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_4_4_3/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_101_101_1/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:4:4"));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_2_5", delts.get(1).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnsNotInCompact() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidCompactorTxnList("4:" + Long.MAX_VALUE));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
  }

  @Test
  public void deltasWithOpenTxnsNotInCompact2() throws Exception {
    Configuration conf = new Configuration();
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0" + AcidUtils.DELTA_SIDE_FILE_SUFFIX, 500,
            new byte[0]),
        new MockFile("mock:/tbl/part1/delta_6_10/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidCompactorTxnList("3:" + Long.MAX_VALUE));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
  }

  @Test
  public void testBaseWithDeleteDeltas() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_49/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_029_029/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_025_030/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_050_105/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_050_105/bucket_0", 0, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_110_110/bucket_0", 0, new byte[0]));
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(new TestInputOutputFormat.MockPath(fs,
            "mock:/tbl/part1"), conf, new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));
    assertEquals("mock:/tbl/part1/base_49", dir.getBaseDirectory().toString());
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(7, obsolete.size());
    assertEquals("mock:/tbl/part1/base_10", obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/base_5", obsolete.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delete_delta_025_030", obsolete.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_025_030", obsolete.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_025_025", obsolete.get(4).getPath().toString());
    assertEquals("mock:/tbl/part1/delete_delta_029_029", obsolete.get(5).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_029_029", obsolete.get(6).getPath().toString());
    assertEquals(0, dir.getOriginalFiles().size());
    List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
    assertEquals(2, deltas.size());
    assertEquals("mock:/tbl/part1/delete_delta_050_105", deltas.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_050_105", deltas.get(1).getPath().toString());
    // The delete_delta_110_110 should not be read because it is greater than the high watermark.
  }

  @Test
  public void testOverlapingDeltaAndDeleteDelta() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_0000063_63/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_000062_62/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_00061_61/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_00064_64/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_0060_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_052_55/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));
    assertEquals("mock:/tbl/part1/base_50", dir.getBaseDirectory().toString());
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(3, obsolete.size());
    assertEquals("mock:/tbl/part1/delete_delta_052_55", obsolete.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_052_55", obsolete.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0060_60", obsolete.get(2).getPath().toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(6, delts.size());
    assertEquals("mock:/tbl/part1/delete_delta_40_60", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_00061_61", delts.get(2).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_000062_62", delts.get(3).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_0000063_63", delts.get(4).getPath().toString());
    assertEquals("mock:/tbl/part1/delete_delta_00064_64", delts.get(5).getPath().toString());
  }

  @Test
  public void testMinorCompactedDeltaMakesInBetweenDelteDeltaObsolete() throws Exception {
    // This test checks that if we have a minor compacted delta for the txn range [40,60]
    // then it will make any delete delta in that range as obsolete.
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_50_50/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:" + Long.MAX_VALUE + ":"));
    List<FileStatus> obsolete = dir.getObsolete();
    assertEquals(1, obsolete.size());
    assertEquals("mock:/tbl/part1/delete_delta_50_50", obsolete.get(0).getPath().toString());
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(1, delts.size());
    assertEquals("mock:/tbl/part1/delta_40_60", delts.get(0).getPath().toString());
  }

  @Test
  public void deltasAndDeleteDeltasWithOpenTxnsNotInCompact() throws Exception {
    // This tests checks that appropriate delta and delete_deltas are included when minor
    // compactions specifies a valid open txn range.
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
        new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_2_2/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_2_5/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_2_5/bucket_0" + AcidUtils.DELTA_SIDE_FILE_SUFFIX, 500,
            new byte[0]),
        new MockFile("mock:/tbl/part1/delete_delta_7_7/bucket_0", 500, new byte[0]),
        new MockFile("mock:/tbl/part1/delta_6_10/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir =
        AcidUtils.getAcidState(part, conf, new ValidCompactorTxnList("4:" + Long.MAX_VALUE + ":"));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(2, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delete_delta_2_2", delts.get(1).getPath().toString());
  }

  @Test
  public void deleteDeltasWithOpenTxnInRead() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname,
        AcidUtils.AcidOperationalProperties.getDefault().toInt());
    MockFileSystem fs = new MockFileSystem(conf,
      new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delete_delta_2_5/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delete_delta_3_3/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_4_4_1/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_4_4_3/bucket_0", 500, new byte[0]),
      new MockFile("mock:/tbl/part1/delta_101_101_1/bucket_0", 500, new byte[0]));
    Path part = new MockPath(fs, "mock:/tbl/part1");
    AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReadTxnList("100:4:4"));
    List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
    assertEquals(3, delts.size());
    assertEquals("mock:/tbl/part1/delta_1_1", delts.get(0).getPath().toString());
    assertEquals("mock:/tbl/part1/delete_delta_2_5", delts.get(1).getPath().toString());
    assertEquals("mock:/tbl/part1/delta_2_5", delts.get(2).getPath().toString());
    // Note that delete_delta_3_3 should not be read, when a minor compacted
    // [delete_]delta_2_5 is present.
  }

  @Test
  public void testDeleteDeltaSubdirPathGeneration() throws Exception {
    String deleteDeltaSubdirPath = AcidUtils.deleteDeltaSubdir(1, 10);
    assertEquals("delete_delta_0000001_0000010", deleteDeltaSubdirPath);
    deleteDeltaSubdirPath = AcidUtils.deleteDeltaSubdir(1, 10, 5);
    assertEquals("delete_delta_0000001_0000010_0005", deleteDeltaSubdirPath);
  }

  @Test
  public void testDeleteEventDeltaDirPathFilter() throws Exception {
    Path positivePath = new Path("delete_delta_000001_000010");
    Path negativePath = new Path("delta_000001_000010");
    assertEquals(true, AcidUtils.deleteEventDeltaDirFilter.accept(positivePath));
    assertEquals(false, AcidUtils.deleteEventDeltaDirFilter.accept(negativePath));
  }

  @Test
  public void testAcidOperationalProperties() throws Exception {
    AcidUtils.AcidOperationalProperties testObj = AcidUtils.AcidOperationalProperties.getDefault();
    assertsForAcidOperationalProperties(testObj, "default");

    testObj = AcidUtils.AcidOperationalProperties.parseInt(1);
    assertsForAcidOperationalProperties(testObj, "split_update");

    testObj = AcidUtils.AcidOperationalProperties.parseString("default");
    assertsForAcidOperationalProperties(testObj, "default");

  }

  private void assertsForAcidOperationalProperties(AcidUtils.AcidOperationalProperties testObj,
      String type) throws Exception {
    switch(type) {
      case "split_update":
      case "default":
        assertEquals(true, testObj.isSplitUpdate());
        assertEquals(false, testObj.isHashBasedMerge());
        assertEquals(1, testObj.toInt());
        assertEquals("|split_update", testObj.toString());
        break;
      default:
        break;
    }
  }

  @Test
  public void testAcidOperationalPropertiesSettersAndGetters() throws Exception {
    AcidUtils.AcidOperationalProperties oprProps = AcidUtils.AcidOperationalProperties.getDefault();
    Configuration testConf = new Configuration();
    // Test setter for configuration object.
    AcidUtils.setAcidOperationalProperties(testConf, oprProps);
    assertEquals(1, testConf.getInt(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname, -1));
    // Test getter for configuration object.
    assertEquals(oprProps.toString(), AcidUtils.getAcidOperationalProperties(testConf).toString());

    Map<String, String> parameters = new HashMap<String, String>();
    // Test setter for map object.
    AcidUtils.setAcidOperationalProperties(parameters, oprProps);
    assertEquals(oprProps.toString(),
        parameters.get(HiveConf.ConfVars.HIVE_TXN_OPERATIONAL_PROPERTIES.varname));
    // Test getter for map object.
    assertEquals(1, AcidUtils.getAcidOperationalProperties(parameters).toInt());
    parameters.put(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, oprProps.toString());
    // Set the appropriate key in the map and test that we are able to read it back correctly.
    assertEquals(1, AcidUtils.getAcidOperationalProperties(parameters).toInt());
  }
  */
}