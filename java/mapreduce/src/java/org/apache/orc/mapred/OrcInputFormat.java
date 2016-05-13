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

package org.apache.orc.mapred;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

  
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;

/**
 * A MapReduce/Hive input format for ORC files.
 */
public class OrcInputFormat<V extends Writable>
    extends FileInputFormat<NullWritable, V> {

  /**
   * Convert a string with a comma separated list of column ids into the
   * array of boolean that match the schemas.
   * @param schema the schema for the reader
   * @param columnsStr the comma separated list of column ids
   * @return a boolean array
   */
  static boolean[] parseInclude(TypeDescription schema, String columnsStr) {
    if (columnsStr == null ||
        schema.getCategory() != TypeDescription.Category.STRUCT) {
      return null;
    }
    boolean[] result = new boolean[schema.getMaximumId() + 1];
    result[0] = true;
    List<TypeDescription> types = schema.getChildren();
    for(String idString: columnsStr.split(",")) {
      TypeDescription type = types.get(Integer.parseInt(idString));
      for(int c=type.getId(); c <= type.getMaximumId(); ++c) {
        result[c] = true;
      }
    }
    return result;
  }

  /**
   * Put the given SearchArgument into the configuration for an OrcInputFormat.
   * @param conf the configuration to modify
   * @param sarg the SearchArgument to put in the configuration
   * @param columnNames the list of column names for the SearchArgument
   */
  public static void setSearchArgument(Configuration conf,
                                       SearchArgument sarg,
                                       String[] columnNames) {
    Output out = new Output();
    new Kryo().writeObject(out, sarg);
    conf.set(OrcConf.KRYO_SARG.getAttribute(),
        Base64.encodeBase64String(out.toBytes()));
    StringBuilder buffer = new StringBuilder();
    for(int i=0; i < columnNames.length; ++i) {
      if (i != 0) {
        buffer.append(',');
      }
      buffer.append(columnNames[i]);
    }
    conf.set(OrcConf.SARG_COLUMNS.getAttribute(), buffer.toString());
  }

  @Override
  public RecordReader<NullWritable, V>
  getRecordReader(InputSplit inputSplit,
                  JobConf conf,
                  Reporter reporter) throws IOException {
    FileSplit split = (FileSplit) inputSplit;
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
    options.include(parseInclude(schema,
        OrcConf.INCLUDE_COLUMNS.getString(conf)));
    String kryoSarg = OrcConf.KRYO_SARG.getString(conf);
    String sargColumns = OrcConf.SARG_COLUMNS.getString(conf);
    if (kryoSarg != null && sargColumns != null) {
      byte[] sargBytes = Base64.decodeBase64(kryoSarg);
      SearchArgument sarg =
          new Kryo().readObject(new Input(sargBytes), SearchArgumentImpl.class);
      options.searchArgument(sarg, sargColumns.split(","));
    }
    return new OrcRecordReader(file, options);
  }
}
