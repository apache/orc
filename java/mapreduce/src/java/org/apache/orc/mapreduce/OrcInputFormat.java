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

package org.apache.orc.mapreduce;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

/**
 * An ORC input format that satisfies the org.apache.hadoop.mapreduce API.
 */
public class OrcInputFormat<V extends Writable>
    extends FileInputFormat<NullWritable, V> {

  /**
   * Put the given SearchArgument into the configuration for an OrcInputFormat.
   * @param conf the configuration to modify
   * @param sarg the SearchArgument to put in the configuration
   * @param columnNames the list of column names for the SearchArgument
   */
  public static void setSearchArgument(Configuration conf,
                                       SearchArgument sarg,
                                       String[] columnNames) {
    org.apache.orc.mapred.OrcInputFormat.setSearchArgument(conf, sarg,
        columnNames);
  }

  @Override
  public RecordReader<NullWritable, V>
      createRecordReader(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext
                         ) throws IOException, InterruptedException {
    FileSplit split = (FileSplit) inputSplit;
    Configuration conf = taskAttemptContext.getConfiguration();
    Reader file = OrcFile.createReader(split.getPath(),
        OrcFile.readerOptions(conf)
            .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf)));
    TypeDescription schema =
        TypeDescription.fromString(OrcConf.SCHEMA.getString(conf));
    Reader.Options options = new Reader.Options()
        .range(split.getStart(), split.getLength())
        .useZeroCopy(OrcConf.USE_ZEROCOPY.getBoolean(conf))
        .skipCorruptRecords(OrcConf.SKIP_CORRUPT_DATA.getBoolean(conf));
    if (schema == null) {
      schema = file.getSchema();
    } else {
      options.schema(schema);
    }
    options.include(org.apache.orc.mapred.OrcInputFormat.parseInclude(schema,
        OrcConf.INCLUDE_COLUMNS.getString(conf)));
    String kryoSarg = OrcConf.KRYO_SARG.getString(conf);
    String sargColumns = OrcConf.SARG_COLUMNS.getString(conf);
    if (kryoSarg != null && sargColumns != null) {
      byte[] sargBytes = Base64.decodeBase64(kryoSarg);
      SearchArgument sarg =
          new Kryo().readObject(new Input(sargBytes), SearchArgumentImpl.class);
      options.searchArgument(sarg, sargColumns.split(","));
    }
    return new OrcMapreduceRecordReader<V>(file, options);
  }
}
