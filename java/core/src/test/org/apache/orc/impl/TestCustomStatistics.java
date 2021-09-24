/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.protobuf.ByteString;
import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CustomStatistics;
import org.apache.orc.CustomStatisticsBuilder;
import org.apache.orc.CustomStatisticsRegister;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCustomStatistics {

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  TypeDescription schema;

  @BeforeEach
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    TDigestBuilder tDigestBuilder = new TDigestBuilder();
    conf.set(OrcConf.CUSTOM_STATISTICS_COLUMNS.getAttribute(), "id:" + tDigestBuilder.name());
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("testWriterImpl.orc");
    fs.create(testFilePath, true);
    schema = TypeDescription.fromString("struct<id:int,name:string>");
    CustomStatisticsRegister.getInstance().register(tDigestBuilder);
  }

  @AfterEach
  public void deleteTestFile() throws Exception {
    fs.delete(testFilePath, false);
  }

  private void write() throws IOException {
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .overwrite(true)
            .setSchema(schema)
    );
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector id = (LongColumnVector) batch.cols[0];
    BytesColumnVector name = (BytesColumnVector) batch.cols[1];
    for (int r = 1; r < 20; ++r) {
      int row = batch.size++;
      id.vector[row] = r;
      byte[] buffer = String.valueOf(r).getBytes(StandardCharsets.UTF_8);
      name.setRef(row, buffer, 0, buffer.length);
    }
    int row = batch.size++;
    id.vector[row] = 1_000_000;
    byte[] buffer = String.valueOf(1_000_000).getBytes(StandardCharsets.UTF_8);
    name.setRef(row, buffer, 0, buffer.length);
    writer.addRowBatch(batch);
    writer.close();
  }

  private void read() throws IOException {
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
    ColumnStatistics[] statistics = reader.getStatistics();
    ColumnStatistics idStatistic = statistics[1];
    ColumnStatistics nameStatistic = statistics[2];
    assertTrue(idStatistic.isCustomStatsExists());
    assertFalse(nameStatistic.isCustomStatsExists());

    CustomStatistics customStatistics = idStatistic.getCustomStatistics();

    assertTrue(customStatistics instanceof TDigestCustomStatistics);

    TDigest tDigest = ((TDigestCustomStatistics) customStatistics).getTDigest();

    assertEquals(18, tDigest.quantile(0.89999999), 0);
    assertEquals(19, tDigest.quantile(0.9), 0);
    assertEquals(19, tDigest.quantile(0.949999999), 0);
    assertEquals(1_000_000, tDigest.quantile(0.95), 0);

    assertEquals(0.925, tDigest.cdf(19), 1e-11);
    assertEquals(0.95, tDigest.cdf(19.0000001), 1e-11);
    assertEquals(0.9, tDigest.cdf(19 - 0.0000001), 1e-11);
  }

  @Test
  public void testReadEncryption() throws IOException {
    write();
    read();
  }

  public static class TDigestBuilder implements CustomStatisticsBuilder {

    @Override
    public String name() {
      return "TDigestForInteger";
    }

    @Override
    public CustomStatistics[] build() {
      return new CustomStatistics[] {
          new TDigestCustomStatistics(false),
          new TDigestCustomStatistics(false),
          new TDigestCustomStatistics(true),
      };
    }

    @Override
    public CustomStatistics build(ByteString statisticsContent) {
      return new TDigestCustomStatistics(
          AVLTreeDigest.fromBytes(statisticsContent.asReadOnlyByteBuffer()));
    }
  }

  public static class TDigestCustomStatistics implements CustomStatistics {

    private TDigest tDigest;

    private boolean needPersistence;

    public TDigestCustomStatistics(TDigest tDigest) {
      this.tDigest = tDigest;
    }

    public TDigestCustomStatistics(boolean needPersistence) {
      this.tDigest = TDigest.createAvlTreeDigest(100);
      this.needPersistence = needPersistence;
    }

    @Override
    public String name() {
      return "TDigestForInteger";
    }

    @Override
    public void updateInteger(long value, int repetitions) {
      tDigest.add(value, repetitions);
    }

    @Override
    public ByteString content() {
      ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.byteSize());
      tDigest.asBytes(byteBuffer);
      byteBuffer.flip();
      return ByteString.copyFrom(byteBuffer);
    }

    @Override
    public boolean needPersistence() {
      return needPersistence;
    }

    @Override
    public void merge(CustomStatistics customStatistics) {
      if (customStatistics instanceof TDigestCustomStatistics) {
        tDigest.add(((TDigestCustomStatistics) customStatistics).tDigest);
      }
    }

    @Override
    public void reset() {
      tDigest = TDigest.createAvlTreeDigest(100);
    }

    public TDigest getTDigest() {
      return tDigest;
    }
  }
}
