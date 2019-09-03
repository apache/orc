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
package org.apache.orc;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Test over an orc file that does not store time zone information in the footer
 * and it was written from a time zone that observes DST for one of the timestamp
 * values stored ('2014-06-06 12:34:56.0').
 */
@RunWith(Parameterized.class)
public class TestOrcDSTNoTimezone {
  Configuration conf;
  FileSystem fs;
  String readerTimeZone;
  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
  static TimeZone defaultTimeZone = TimeZone.getDefault();

  public TestOrcDSTNoTimezone(String readerTZ) {
    this.readerTimeZone = readerTZ;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    List<Object[]> result = Arrays.asList(new Object[][]{
        {"America/Los_Angeles"},
        {"Europe/Berlin"},
        {"Asia/Jerusalem"}
    });
    return result;
  }

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
  }

  @After
  public void restoreTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  @Test
  public void testReadOldTimestampFormat() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Path oldFilePath = new Path(getClass().getClassLoader().
        getSystemResource("orc-file-dst-no-timezone.orc").getPath());
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(fs).useUTCTimestamp(true));
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    TypeDescription schema = reader.getSchema();
    VectorizedRowBatch batch = schema.createRowBatch(10);
    TimestampColumnVector ts = (TimestampColumnVector) batch.cols[0];

    boolean[] include = new boolean[schema.getMaximumId() + 1];
    include[schema.getChildren().get(0).getId()] = true;
    RecordReader rows = reader.rows
        (reader.options().include(include));
    assertTrue(rows.nextBatch(batch));
    Timestamp timestamp = ts.asScratchTimestamp(0);
    assertEquals(Timestamp.valueOf("2014-01-01 12:34:56.0").toString(),
        formatter.format(timestamp));

    // check the contents of second row
    rows.seekToRow(1);
    assertTrue(rows.nextBatch(batch));
    assertEquals(1, batch.size);
    timestamp = ts.asScratchTimestamp(0);
    assertEquals(Timestamp.valueOf("2014-06-06 12:34:56.0").toString(),
        formatter.format(timestamp));

    // handle the close up
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }
}
