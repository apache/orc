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

  protected Path writeAcidOrcFile(String name) throws IOException {
    Path path = createPath(name);
    TypeDescription schema = TypeDescription.createStruct()
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

    batch.size = 5;

    LongColumnVector opCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_OPERATION_OFFSET];
    opCol.isRepeating = true;
    opCol.vector[0] = AcidConstants.OPERATION_INSERT;

    LongColumnVector origTxnCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ORIG_TXN_OFFSET];
    origTxnCol.vector[0] = 1;
    origTxnCol.vector[1] = 2;
    origTxnCol.vector[2] = 2;
    origTxnCol.vector[3] = 3;
    origTxnCol.vector[4] = 3;

    LongColumnVector bucketCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_BUCKET_OFFSET];
    bucketCol.isRepeating = true;
    bucketCol.vector[0] = BucketCodec.V1.encode(0, 0);

    LongColumnVector rowIdCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_ROW_ID_OFFSET];
    for (int i = 0; i < batch.size; i++) rowIdCol.vector[i] = i;

    LongColumnVector currentTxnCol = (LongColumnVector)batch.cols[AcidConstants.ROW_ID_CURRENT_TXN_OFFSET];
    currentTxnCol.vector[0] = 1;
    currentTxnCol.vector[1] = 2;
    currentTxnCol.vector[2] = 2;
    currentTxnCol.vector[3] = 3;
    currentTxnCol.vector[4] = 3;

    BytesColumnVector str =
        (BytesColumnVector)((StructColumnVector)batch.cols[AcidConstants.ROWS_STRUCT_COL]).fields[0];
    str.noNulls = false;
    str.isNull[4] = true;
    str.setVal(0, "mary had a little lamb".getBytes());
    str.setVal(1, "its fleece was white as snow".getBytes());
    str.setVal(2, "and everywhere that mary went".getBytes());
    str.setVal(3, "the lamb was sure to go".getBytes());

    writer.addRowBatch(batch);
    writer.close();

    return path;
  }
}
