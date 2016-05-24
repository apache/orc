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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public class TestMrUnit {
  JobConf conf = new JobConf();

  /**
   * Split the input struct into its two parts.
   */
  public static class MyMapper
      implements Mapper<NullWritable, OrcStruct, OrcKey, OrcValue> {
    private OrcKey keyWrapper = new OrcKey();
    private OrcValue valueWrapper = new OrcValue();

    @Override
    public void map(NullWritable key, OrcStruct value,
                    OutputCollector<OrcKey, OrcValue> outputCollector,
                    Reporter reporter) throws IOException {
      keyWrapper.key = value.getFieldValue(0);
      valueWrapper.value = value.getFieldValue(1);
      outputCollector.collect(keyWrapper, valueWrapper);
    }

    @Override
    public void close() throws IOException {
      // PASS
    }

    @Override
    public void configure(JobConf jobConf) {
      // PASS
    }
  }

  /**
   * Glue the key and values back together.
   */
  public static class MyReducer
      implements Reducer<OrcKey, OrcValue, NullWritable, OrcStruct> {
    private OrcStruct output = new OrcStruct(TypeDescription.fromString
        ("struct<first:struct<x:int,y:int>,second:struct<z:string>>"));
    private final NullWritable nada = NullWritable.get();

    @Override
    public void reduce(OrcKey key, Iterator<OrcValue> iterator,
                       OutputCollector<NullWritable, OrcStruct> collector,
                       Reporter reporter) throws IOException {
      output.setFieldValue(0, key.key);
      while (iterator.hasNext()) {
        OrcValue value = iterator.next();
        output.setFieldValue(1, value.value);
        collector.collect(nada, output);
      }
    }

    @Override
    public void close() throws IOException {
      // PASS
    }

    @Override
    public void configure(JobConf jobConf) {
      // PASS
    }
  }

  /**
   * This class is intended to support MRUnit's object copying for input and
   * output objects.
   *
   * Real mapreduce contexts should NEVER use this class.
   *
   * The type string is serialized before each value.
   */
  public static class OrcStructSerialization
      implements Serialization<OrcStruct> {

    @Override
    public boolean accept(Class<?> cls) {
      return OrcStruct.class.isAssignableFrom(cls);
    }

    @Override
    public Serializer<OrcStruct> getSerializer(Class<OrcStruct> aClass) {
      return new Serializer<OrcStruct>() {
        DataOutputStream dataOut;

        public void open(OutputStream out) {
          if(out instanceof DataOutputStream) {
            dataOut = (DataOutputStream)out;
          } else {
            dataOut = new DataOutputStream(out);
          }
        }

        public void serialize(OrcStruct w) throws IOException {
          Text.writeString(dataOut, w.getSchema().toString());
          w.write(dataOut);
        }

        public void close() throws IOException {
          dataOut.close();
        }
      };
    }

    @Override
    public Deserializer<OrcStruct> getDeserializer(Class<OrcStruct> aClass) {
      return new Deserializer<OrcStruct>() {
        DataInputStream input;

        @Override
        public void open(InputStream inputStream) throws IOException {
          if(inputStream instanceof DataInputStream) {
            input = (DataInputStream)inputStream;
          } else {
            input = new DataInputStream(inputStream);
          }
        }

        @Override
        public OrcStruct deserialize(OrcStruct orcStruct) throws IOException {
          String typeStr = Text.readString(input);
          OrcStruct result = new OrcStruct(TypeDescription.fromString(typeStr));
          result.readFields(input);
          return result;
        }

        @Override
        public void close() throws IOException {
          // PASS
        }
      };
    }
  }

  @Test
  public void testMapred() throws IOException {
    conf.set("io.serializations",
        OrcStructSerialization.class.getName() + "," +
            WritableSerialization.class.getName());
    OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.setString(conf, "struct<x:int,y:int>");
    OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(conf, "struct<z:string>");
    MyMapper mapper = new MyMapper();
    mapper.configure(conf);
    MyReducer reducer = new MyReducer();
    reducer.configure(conf);
    MapReduceDriver<NullWritable, OrcStruct,
                    OrcKey, OrcValue,
                    NullWritable, OrcStruct> driver =
        new MapReduceDriver<>(mapper, reducer);
    driver.setConfiguration(conf);
    NullWritable nada = NullWritable.get();
    OrcStruct input = (OrcStruct) OrcStruct.createValue(
        TypeDescription.fromString("struct<one:struct<x:int,y:int>,two:struct<z:string>>"));
    IntWritable x =
        (IntWritable) ((OrcStruct) input.getFieldValue(0)).getFieldValue(0);
    IntWritable y =
        (IntWritable) ((OrcStruct) input.getFieldValue(0)).getFieldValue(1);
    Text z = (Text) ((OrcStruct) input.getFieldValue(1)).getFieldValue(0);

    // generate the input stream
    for(int r=0; r < 20; ++r) {
      x.set(100 -  (r / 4));
      y.set(r*2);
      z.set(Integer.toHexString(r));
      driver.withInput(nada, input);
    }

    // generate the expected outputs
    for(int g=4; g >= 0; --g) {
      x.set(100 - g);
      for(int i=0; i < 4; ++i) {
        int r = g * 4 + i;
        y.set(r * 2);
        z.set(Integer.toHexString(r));
        driver.withOutput(nada, input);
      }
    }
    driver.runTest();
  }
}
