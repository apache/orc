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

package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.filter.FilterContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

public class TestRowFilteringIOSkip {
  private final static Logger LOG = LoggerFactory.getLogger(TestRowFilteringIOSkip.class);
  private static final Path workDir = new Path(System.getProperty("test.tmp.dir",
                                                                  "target" + File.separator + "test"
                                                                  + File.separator + "tmp"));

  private static Configuration conf;
  private static FileSystem fs;
  private static final Path filePath = new Path(workDir, "skip_file.orc");

  private static final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createDecimal().withPrecision(20).withScale(6))
    .addField("f3", TypeDescription.createLong())
    .addField("f4", TypeDescription.createString())
    .addField("ridx", TypeDescription.createLong());
  private static final boolean[] FirstColumnOnly = new boolean[] {true, true, false, false, false
    , false};
  private static final long RowCount = 4000000L;
  private static final String[] FilterColumns = new String[] {"f1", "ridx"};
  private static final int scale = 3;

  @BeforeClass
  public static void setup() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);

    if (fs.exists(filePath)) {
      return;
    }

    LOG.info("Creating file {} with schema {}", filePath, schema);
    try (Writer writer = OrcFile.createWriter(filePath,
                                              OrcFile.writerOptions(conf)
                                                .fileSystem(fs)
                                                .overwrite(true)
                                                .rowIndexStride(8192)
                                                .setSchema(schema))) {
      Random rnd = new Random(1024);
      VectorizedRowBatch b = schema.createRowBatch();
      for (int rowIdx = 0; rowIdx < RowCount; rowIdx++) {
        long v = rnd.nextLong();
        for (int colIdx = 0; colIdx < schema.getChildren().size() - 1; colIdx++) {
          switch (schema.getChildren().get(colIdx).getCategory()) {
            case LONG:
              ((LongColumnVector) b.cols[colIdx]).vector[b.size] = v;
              break;
            case DECIMAL:
              HiveDecimalWritable d = new HiveDecimalWritable();
              d.setFromLongAndScale(v, scale);
              ((DecimalColumnVector) b.cols[colIdx]).vector[b.size] = d;
              break;
            case STRING:
              ((BytesColumnVector) b.cols[colIdx]).setVal(b.size,
                                                          String.valueOf(v)
                                                            .getBytes(StandardCharsets.UTF_8));
              break;
            default:
              throw new IllegalArgumentException();
          }
        }
        // Populate the rowIdx
        ((LongColumnVector) b.cols[4]).vector[b.size] = rowIdx;

        b.size += 1;
        if (b.size == b.getMaxSize()) {
          writer.addRowBatch(b);
          b.reset();
        }
      }
      if (b.size > 0) {
        writer.addRowBatch(b);
        b.reset();
      }
    }
    LOG.info("Created file {}", filePath);
  }

  @Test
  public void writeIsSuccessful() throws IOException {
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    Assert.assertEquals(RowCount, r.getNumberOfRows());
    Assert.assertTrue(r.getStripes().size() > 1);
  }

  @Test
  public void readFirstColumn() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount = 0;
    try (RecordReader rr = r.rows(r.options().include(FirstColumnOnly))) {
      while (rr.nextBatch(b)) {
        Assert.assertTrue(((LongColumnVector) b.cols[0]).vector[0] != 0);
        rowCount += b.size;
      }
    }
    FileSystem.Statistics stats = readEnd();
    Assert.assertEquals(RowCount, rowCount);
    // We should read less than half the length of the file
    Assert.assertTrue(String.format("Bytes read %d is not half of file size %d",
                                    stats.getBytesRead(),
                                    r.getContentLength()),
                      stats.getBytesRead() < r.getContentLength() / 2);
  }

  @Test
  public void readWithSArg() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 0L)
      .build();
    Reader.Options options = r.options()
      .searchArgument(sarg, new String[] {"f1"});
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    Assert.assertEquals(RowCount, rowCount);
    Assert.assertTrue(p >= 100);
  }

  private long validateFilteredRecordReader(RecordReader rr, VectorizedRowBatch b)
    throws IOException {
    long rowCount = 0;
    while (rr.nextBatch(b)) {
      validateBatch(b, -1);
      rowCount += b.size;
    }
    return rowCount;
  }

  private void validateBatch(VectorizedRowBatch b, long expRowNum) {
    HiveDecimalWritable d = new HiveDecimalWritable();

    for (int i = 0; i < b.size; i++) {
      int rowIdx;
      if (b.selectedInUse) {
        rowIdx = b.selected[i];
      } else {
        rowIdx = i;
      }
      long expValue = ((LongColumnVector) b.cols[0]).vector[rowIdx];
      d.setFromLongAndScale(expValue, scale);
      Assert.assertEquals(d, ((DecimalColumnVector) b.cols[1]).vector[rowIdx]);
      Assert.assertEquals(expValue, ((LongColumnVector) b.cols[2]).vector[rowIdx]);
      BytesColumnVector sv = (BytesColumnVector) b.cols[3];
      Assert.assertEquals(String.valueOf(expValue),
                          sv.toString(rowIdx));
      if (expRowNum != -1) {
        Assert.assertEquals(expRowNum + i, ((LongColumnVector) b.cols[4]).vector[rowIdx]);
      }
    }
  }

  @Test
  public void filterAllRows() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    Reader.Options options = r.options()
      .setRowFilter(FilterColumns, new InFilter(new HashSet<>(0), 0));
    long rowCount = 0;
    try (RecordReader rr = r.rows(options)) {
      while (rr.nextBatch(b)) {
        Assert.assertTrue(((LongColumnVector) b.cols[0]).vector[0] != 0);
        Assert.assertTrue(((LongColumnVector) b.cols[0]).vector[0] != 0);
        rowCount += b.size;
      }
    }
    FileSystem.Statistics stats = readEnd();
    Assert.assertEquals(0, rowCount);
    // We should read less than half the length of the file
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    Assert.assertTrue(String.format("Bytes read %.2f%% should be less than 50%%",
                                    readPercentage),
                      readPercentage < 50);
  }

  @Test
  public void readEverything() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows()) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    Assert.assertEquals(RowCount, rowCount);
    Assert.assertTrue(p >= 100);
  }

  private double readPercentage(FileSystem.Statistics stats, long fileSize) {
    double p = stats.getBytesRead() * 100.0 / fileSize;
    LOG.info(String.format("%nFileSize: %d%nReadSize: %d%nRead %%: %.2f",
                           fileSize,
                           stats.getBytesRead(),
                           p));
    return p;
  }

  @Test
  public void readEverythingWithFilter() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(r.options()
                                    .setRowFilter(FilterColumns, new AllowAllFilter()))) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    Assert.assertEquals(RowCount, rowCount);
    Assert.assertTrue(p >= 100);
  }

  @Test
  public void filterAlternateBatches() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    Reader.Options options = r.options()
      .setRowFilter(FilterColumns, new AlternateFilter());
    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    FileSystem.Statistics stats = readEnd();
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    System.out.println("Read percentage: " + readPercentage);
    Assert.assertTrue(RowCount > rowCount);
  }

  @Test
  public void filterWithSeek() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    Reader.Options options = r.options()
      .setRowFilter(FilterColumns, new AlternateFilter());

    long seekRow;
    try (RecordReader rr = r.rows(options)) {
      // Validate the first batch
      Assert.assertTrue(rr.nextBatch(b));
      validateBatch(b, 0);
      Assert.assertEquals(b.size, rr.getRowNumber());

      // Read the next batch, will skip a batch that is filtered
      Assert.assertTrue(rr.nextBatch(b));
      validateBatch(b, 2048);
      Assert.assertEquals(2048 + 1024, rr.getRowNumber());

      // Seek forward
      seekToRow(rr, b, 4096);

      // Seek back to the filtered batch
      long bytesRead = readEnd().getBytesRead();
      seekToRow(rr, b, 1024);
      // No IO should have taken place
      Assert.assertEquals(bytesRead, readEnd().getBytesRead());

      // Seek forward to next row group, where the first batch is not filtered
      seekToRow(rr, b, 8192);

      // Seek forward to next row group but position on filtered batch
      seekToRow(rr, b, (8192 * 2) + 1024);

      // Seek forward to next stripe
      seekRow = r.getStripes().get(0).getNumberOfRows();
      seekToRow(rr, b, seekRow);

      // Seek back to previous stripe, filtered row, it should require more IO as a result of
      // stripe change
      bytesRead = readEnd().getBytesRead();
      seekToRow(rr, b, 1024);
      Assert.assertTrue("Change of stripe should require more IO",
                        readEnd().getBytesRead() > bytesRead);
    }
    FileSystem.Statistics stats = readEnd();
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    Assert.assertTrue(readPercentage > 130);
  }

  private void seekToRow(RecordReader rr, VectorizedRowBatch b, long row) throws IOException {
    rr.seekToRow(row);
    Assert.assertTrue(rr.nextBatch(b));
    long expRowNum;
    if ((row / b.getMaxSize()) % 2 == 0) {
      expRowNum = row;
    } else {
      // As the seek batch gets filtered
      expRowNum = row + b.getMaxSize();
    }
    validateBatch(b, expRowNum);
    Assert.assertEquals(expRowNum + b.getMaxSize(), rr.getRowNumber());
  }

  private static class InFilter implements Consumer<FilterContext> {
    private final Set<Long> ids;
    private final int colIdx;

    private InFilter(Set<Long> ids, int colIdx) {
      this.ids = ids;
      this.colIdx = colIdx;
    }

    @Override
    public void accept(FilterContext b) {
      int newSize = 0;
      for (int i = 0; i < b.getSelectedSize(); i++) {
        if (ids.contains(getValue(b, i))) {
          b.getSelected()[newSize] = i;
          newSize += 1;
        }
      }
      b.setSelectedInUse(true);
      b.setSelectedSize(newSize);
    }

    private Long getValue(FilterContext b, int rowIdx) {
      LongColumnVector v = ((LongColumnVector) b.getCols()[colIdx]);
      int valIdx = rowIdx;
      if (v.isRepeating) {
        valIdx = 0;
      }
      if (!v.noNulls && v.isNull[valIdx]) {
        return null;
      } else {
        return v.vector[valIdx];
      }
    }
  }

  /**
   * Fill odd batches values in a default read
   * if ridx(rowIdx) / 1024 is even then allow otherwise fail
   */
  private static class AlternateFilter implements Consumer<FilterContext> {
    @Override
    public void accept(FilterContext b) {
      LongColumnVector v = (LongColumnVector) b.getCols()[4];

      if ((v.vector[0] / 1024) % 2 == 1) {
        b.setSelectedInUse(true);
        b.setSelectedSize(0);
      }
    }
  }

  private static class AllowAllFilter implements Consumer<FilterContext> {

    @Override
    public void accept(FilterContext batch) {
      // do nothing every row is allowed
    }

  }

  private static void readStart() {
    FileSystem.clearStatistics();
  }

  private static FileSystem.Statistics readEnd() {
    return FileSystem.getAllStatistics().get(0);
  }
}
