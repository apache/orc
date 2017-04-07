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

package org.apache.orc.bench.convert.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.OrcBenchmarkUtilities;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.convert.BatchWriter;
import org.apache.orc.bench.CompressionKind;
import org.apache.orc.bench.Utilities;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Properties;

public class ParquetWriter implements BatchWriter {
  private final FileSinkOperator.RecordWriter writer;
  private final TypeDescription schema;
  private final ParquetHiveRecord record;

  public ParquetWriter(Path path,
                       TypeDescription schema,
                       Configuration conf,
                       CompressionKind compression
                       ) throws IOException {
    JobConf jobConf = new JobConf(conf);
    Properties tableProperties = Utilities.convertSchemaToHiveConfig(schema);
    this.schema = schema;
    jobConf.set(ParquetOutputFormat.COMPRESSION, getCodec(compression).name());
    writer = new MapredParquetOutputFormat().getHiveRecordWriter(jobConf, path,
        ParquetHiveRecord.class, compression != CompressionKind.NONE,
        tableProperties, Reporter.NULL);
    record = new ParquetHiveRecord(null,
        OrcBenchmarkUtilities.createObjectInspector(schema));
  }

  public void writeBatch(VectorizedRowBatch batch) throws IOException {
    for(int r=0; r < batch.size; ++r) {
      record.value = OrcBenchmarkUtilities.nextObject(batch, schema, r,
          (Writable) record.value);
      writer.write(record);
    }
  }

  public void close() throws IOException {
    writer.close(false);
  }

  public static CompressionCodecName getCodec(CompressionKind kind) {
    switch (kind) {
      case NONE:
        return CompressionCodecName.UNCOMPRESSED;
      case ZLIB:
        return CompressionCodecName.GZIP;
      case SNAPPY:
        return CompressionCodecName.SNAPPY;
      default:
        throw new IllegalArgumentException("Unsupported codec " + kind);
    }
  }
}
