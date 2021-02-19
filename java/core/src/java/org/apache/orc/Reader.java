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

import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.HadoopShimsFactory;


/**
 * The interface for reading ORC files.
 *
 * One Reader can support multiple concurrent RecordReader.
 */
public interface Reader extends org.apache.orc.core.Reader {

  /**
   * Options for creating a RecordReader.
   */
  class Options extends org.apache.orc.core.Reader.Options {
    private String[] columnNames;

    public Options() {
      // PASS
    }

    public Options(org.apache.orc.shims.Configuration conf) {
      super(conf);
    }

    public Options(Configuration conf) {
      super(HadoopShimsFactory.get().createConfiguration(null, conf));
    }

    @Override
    public Options include(boolean[] include) {
      super.include(include);
      return this;
    }

    @Override
    public Options range(long offset, long length) {
      super.range(offset, length);
      return this;
    }

    @Override
    public Options schema(TypeDescription schema) {
      super.schema(schema);
      return this;
    }

    @Override
    public Options setRowFilter(String[] filterColumnNames, Consumer<VectorizedRowBatch> filterCallback) {
      super.setRowFilter(filterColumnNames, filterCallback);
      return this;
    }

    public Options searchArgument(SearchArgument sarg, String[] columnNames) {
      super.searchArgument(sarg);
      this.columnNames = columnNames;
      return this;
    }

    @Override
    public Options useZeroCopy(boolean value) {
      super.useZeroCopy(value);
      return this;
    }

    @Override
    public Options dataReader(DataReader value) {
      super.dataReader(value);
      return this;
    }

    @Override
    public Options skipCorruptRecords(boolean value) {
      super.skipCorruptRecords(value);
      return this;
    }

    @Override
    public Options tolerateMissingSchema(boolean value) {
      super.tolerateMissingSchema(value);
      return this;
    }

    @Override
    public Options forcePositionalEvolution(boolean value) {
      super.forcePositionalEvolution(value);
      return this;
    }

    @Override
    public Options positionalEvolutionLevel(int value) {
      super.positionalEvolutionLevel(value);
      return this;
    }


    @Override
    public Options isSchemaEvolutionCaseAware(boolean value) {
      super.isSchemaEvolutionCaseAware(value);
      return this;
    }

    @Override
    public Options includeAcidColumns(boolean includeAcidColumns) {
      super.includeAcidColumns(includeAcidColumns);
      return this;
    }

    public String[] getColumnNames() {
      return columnNames;
    }

    @Override
    public Options clone() {
      return (Options) super.clone();
    }
  }

  /**
   * Create a default options object that can be customized for creating
   * a RecordReader.
   * @return a new default Options object
   */
  Options options();
}
