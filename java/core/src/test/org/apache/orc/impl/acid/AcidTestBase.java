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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
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
    Path path = createPath(name);
    FSDataOutputStream out = fs.create(path);
    out.writeBytes("abc123");
    out.close();
    return fs.getFileStatus(path);
  }

  protected Path createPath(String name) throws IOException {
    Path parent = baseDir;
    Path namePath = new Path(name);
    if (namePath.getParent() != null) {
      Path dirPath = new Path(baseDir, namePath.getParent());
      fs.mkdirs(dirPath);
      parent = dirPath;
    }
    return new Path(parent, namePath.getName());
  }

  protected List<FileStatus> parcedAcidFileListToFileStatusList(List<ParsedAcidFile> files) {
    List<FileStatus> stats = new ArrayList<>(files.size());
    for (ParsedAcidFile file : files) stats.add(file.getFileStatus());
    return stats;
  }

  protected Path writeAcidOrcFile(String name,
                                  boolean isDelete,
                                  long[] origTxns,
                                  long[] buckets,
                                  long[] rowIds,
                                  long[] currentTxns,
                                  String[] rows) throws IOException {
    Path path = createPath(name);
    TypeDescription schema = isDelete ?
        TypeDescription.createStruct()
        .addField(AcidConstants.ROW_ID_OPERATION_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROW_ID_ORIG_TXN_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROW_ID_BUCKET_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROW_ID_ROW_ID_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROW_ID_CURRENT_TXN_COL_NAME, TypeDescription.createLong())
      : TypeDescription.createStruct()
        .addField(AcidConstants.ROW_ID_OPERATION_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROW_ID_ORIG_TXN_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROW_ID_BUCKET_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROW_ID_ROW_ID_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROW_ID_CURRENT_TXN_COL_NAME, TypeDescription.createLong())
        .addField(AcidConstants.ROWS_STRUCT_COL_NAME, TypeDescription.createStruct()
            .addField("str", TypeDescription.createString()));
    Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf)
        .setSchema(schema)
        .stripeSize(100000)
        .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();

    batch.size = origTxns.length;

    LongColumnVector opCol = (LongColumnVector) batch.cols[AcidConstants.ROW_ID_OPERATION_OFFSET];
    opCol.isRepeating = true;
    opCol.vector[0] = isDelete ? AcidConstants.OPERATION_DELETE : AcidConstants.OPERATION_INSERT;

    boolean repeating = true;
    for (long origTxn : origTxns) repeating &= origTxn == origTxns[0];
    LongColumnVector origTxnCol =
        (LongColumnVector) batch.cols[AcidConstants.ROW_ID_ORIG_TXN_OFFSET];
    if (repeating) {
      origTxnCol.isRepeating = true;
      origTxnCol.vector[0] = origTxns[0];
    } else {
      System.arraycopy(origTxns, 0, origTxnCol.vector, 0, origTxns.length);
    }

    repeating = true;
    for (long bucket : buckets) repeating &= bucket == buckets[0];
    LongColumnVector bucketCol = (LongColumnVector) batch.cols[AcidConstants.ROW_ID_BUCKET_OFFSET];
    if (repeating) {
      bucketCol.isRepeating = true;
      bucketCol.vector[0] = buckets[0];
    } else {
      System.arraycopy(buckets, 0, bucketCol.vector, 0, buckets.length);
    }

    LongColumnVector rowIdCol = (LongColumnVector) batch.cols[AcidConstants.ROW_ID_ROW_ID_OFFSET];
    for (int i = 0; i < batch.size; i++) rowIdCol.vector[i] = rowIds[i];

    repeating = true;
    for (long currentTxn : currentTxns) repeating &= currentTxn == currentTxns[0];
    LongColumnVector currentTxnCol =
        (LongColumnVector) batch.cols[AcidConstants.ROW_ID_CURRENT_TXN_OFFSET];
    if (repeating) {
      currentTxnCol.isRepeating = true;
      currentTxnCol.vector[0] = origTxns[0];
    } else {
      System.arraycopy(currentTxns, 0, currentTxnCol.vector, 0, currentTxns.length);
    }

    if (!isDelete) {
      BytesColumnVector str =
          (BytesColumnVector) ((StructColumnVector) batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];
      for (int i = 0; i < rows.length; i++) {
        if (rows[i] == null) {
          str.noNulls = false;
          str.isNull[i] = true;
        } else {
          str.setVal(i, rows[i].getBytes());
        }
      }
    }

    writer.addRowBatch(batch);
    writer.close();

    return path;
  }

  protected Path insertFile(String name,
                            long[] origTxns,
                            long[] buckets,
                            String[] rows) throws IOException {
    long[] rowIds = new long[origTxns.length];
    for (int i = 0; i < rowIds.length; i++) rowIds[i] = i;
    return writeAcidOrcFile(name, false, origTxns, buckets, rowIds, origTxns, rows);
  }

  protected Path deleteDeltaFile(String name,
                                 long[] origTxns,
                                 long[] buckets,
                                 long[] rowIds,
                                 long[] currentTxns) throws IOException {
    return writeAcidOrcFile(name, true, origTxns, buckets, rowIds, currentTxns, null);
  }

  protected Path multiTxnInsertFile(String name) throws IOException {
    long[] txns = new long[]{1, 2, 2, 3, 3};
    long[] buckets = new long[5];
    for (int i = 0; i < buckets.length; i++) buckets[i] = BucketCodec.V1.encode(0, 0);
    String[] rows = new String[]{
        "mary had a little lamb",
        "its fleece was white as snow",
        "and everywhere that mary went",
        "the lamb was sure to go",
        null};
    return insertFile(name, txns, buckets, rows);
  }

  protected Path singleTxnInsertFile(String name) throws IOException {
    long[] txns = new long[]{10, 10, 10, 10, 10};
    long[] buckets = new long[5];
    for (int i = 0; i < buckets.length; i++) buckets[i] = BucketCodec.V1.encode(0, 0);
    String[] rows = new String[]{
        "hickery dickery dock",
        "the mouse ran up the clock",
        "the clock struck one",
        "down the mouse did run",
        "hickery dickery dock."};
    return insertFile(name, txns, buckets, rows);
  }
}
