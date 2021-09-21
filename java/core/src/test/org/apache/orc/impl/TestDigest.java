package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDigest {

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  TypeDescription schema;

  @BeforeEach
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    conf.set(OrcConf.DIGEST_COLUMNS.getAttribute(), "id:100");
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("testWriterImpl.orc");
    fs.create(testFilePath, true);
    schema = TypeDescription.fromString("struct<id:int,name:string>");
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
    assertTrue(statistics[1].hasDigest());
    assertFalse(statistics[2].hasDigest());
    assertEquals(18, statistics[1].quantile(0.89999999), 0);
    assertEquals(19, statistics[1].quantile(0.9), 0);
    assertEquals(19, statistics[1].quantile(0.949999999), 0);
    assertEquals(1_000_000, statistics[1].quantile(0.95), 0);

    assertEquals(0.925, statistics[1].cdf(19), 1e-11);
    assertEquals(0.95, statistics[1].cdf(19.0000001), 1e-11);
    assertEquals(0.9, statistics[1].cdf(19 - 0.0000001), 1e-11);
  }

  @Test
  public void testReadEncryption() throws IOException {
    write();
    read();
  }
}
