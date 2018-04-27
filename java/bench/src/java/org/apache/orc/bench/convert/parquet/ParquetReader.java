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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.convert.BatchReader;

import java.io.IOException;
import java.util.List;

public class ParquetReader implements BatchReader {

  private final NullWritable nada = NullWritable.get();
  private final RecordReader<NullWritable,ArrayWritable> reader;
  private final ArrayWritable value;
  private final Converter[] converters;

  public ParquetReader(Path path,
                       TypeDescription schema,
                       Configuration conf) throws IOException {
    FileSplit split = new FileSplit(path, 0, Long.MAX_VALUE, new String[]{});
    JobConf jobConf = new JobConf(conf);
    reader = new MapredParquetInputFormat().getRecordReader(split, jobConf,
        Reporter.NULL);
    value = reader.createValue();
    converters = new Converter[schema.getChildren().size()];
    List<TypeDescription> children = schema.getChildren();
    for(int c = 0; c < converters.length; ++c) {
      converters[c] = createConverter(children.get(c));
    }
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    batch.reset();
    int maxSize = batch.getMaxSize();
    while (batch.size < maxSize && reader.next(nada, value)) {
      Writable[] values = value.get();
      int row = batch.size++;
      for(int c=0; c < batch.cols.length; ++c) {
        converters[c].convert(batch.cols[c], row, values[c]);
      }
    }
    return batch.size != 0;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  interface Converter {
    void convert(ColumnVector vector, int row, Object value);
  }

  private static class BooleanConverter implements Converter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((LongColumnVector) cv).vector[row] =
            ((BooleanWritable) value).get() ? 1 : 0;
      }
    }
  }

  private static class IntConverter implements Converter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((LongColumnVector) cv).vector[row] =
            ((IntWritable) value).get();
      }
    }
  }

  private static class LongConverter implements Converter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((LongColumnVector) cv).vector[row] =
            ((LongWritable) value).get();
      }
    }
  }

  private static class FloatConverter implements Converter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((DoubleColumnVector) cv).vector[row] =
            ((FloatWritable) value).get();
      }
    }
  }

  private static class DoubleConverter implements Converter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((DoubleColumnVector) cv).vector[row] =
            ((DoubleWritable) value).get();
      }
    }
  }

  private static class StringConverter implements Converter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        Text castValue = (Text) value;
        ((BytesColumnVector) cv).setVal(row, castValue.getBytes(), 0,
            castValue.getLength());
      }
    }
  }

  private static class BinaryConverter implements Converter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        BytesWritable buf = (BytesWritable) value;
        ((BytesColumnVector) cv).setVal(row, buf.getBytes(), 0,
            buf.getLength());
      }
    }
  }

  private static class TimestampConverter implements Converter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        TimestampColumnVector tc = (TimestampColumnVector) cv;
        tc.time[row] = ((TimestampWritable) value).getSeconds();
        tc.nanos[row] = ((TimestampWritable) value).getNanos();
      }
    }
  }

  private static class DecimalConverter implements Converter {
    final int scale;
    DecimalConverter(int scale) {
      this.scale = scale;
    }
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        DecimalColumnVector tc = (DecimalColumnVector) cv;
        tc.vector[row].set((HiveDecimalWritable) value);
      }
    }
  }

  private static class ListConverter implements Converter {
    final Converter childConverter;

    ListConverter(TypeDescription schema) {
      childConverter = createConverter(schema.getChildren().get(0));
    }

    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ListColumnVector tc = (ListColumnVector) cv;
        Writable[] array = ((ArrayWritable) value).get();
        int start = tc.childCount;
        int len = array.length;
        tc.childCount += len;
        tc.child.ensureSize(tc.childCount, true);
        for(int i=0; i < len; ++i) {
          childConverter.convert(tc.child, start + i, array[i]);
        }
      }
    }
  }

  private static class StructConverter implements Converter {
    final Converter[] childConverters;

    StructConverter(TypeDescription schema) {
      List<TypeDescription> children = schema.getChildren();
      childConverters = new Converter[children.size()];
      for(int i=0; i < childConverters.length; ++i) {
        childConverters[i] = createConverter(children.get(i));
      }
    }

    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        StructColumnVector tc = (StructColumnVector) cv;
        Writable[] record = ((ArrayWritable) value).get();
        for(int c=0; c < tc.fields.length; ++c) {
          childConverters[c].convert(tc.fields[c], row, record[c]);
        }
      }
    }
  }

  static Converter createConverter(TypeDescription types) {
    switch (types.getCategory()) {
      case BINARY:
        return new BinaryConverter();
      case BOOLEAN:
        return new BooleanConverter();
      case BYTE:
      case SHORT:
      case INT:
        return new IntConverter();
      case LONG:
        return new LongConverter();
      case FLOAT:
        return new FloatConverter();
      case DOUBLE:
        return new DoubleConverter();
      case CHAR:
      case VARCHAR:
      case STRING:
        return new StringConverter();
      case TIMESTAMP:
        return new TimestampConverter();
      case DECIMAL:
        return new DecimalConverter(types.getScale());
      case LIST:
        return new ListConverter(types);
      case STRUCT:
        return new StructConverter(types);
      default:
        throw new IllegalArgumentException("Unhandled type " + types);
    }
  }
}
