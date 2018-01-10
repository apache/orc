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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

import java.util.ServiceLoader;

/**
 * The API for masking data during column encryption for ORC.
 *
 * They apply to an individual column (via ColumnVector) instead of a
 * VectorRowBatch.
 *
 */
public interface DataMask {

  /**
   * The standard DataMasks can be created using this short cut.
   *
   * For example, DataMask.Standard.NULLIFY.build(schema) will build a
   * nullify DataMask.
   */
  enum Standard {
    NULLIFY("nullify"),
    REDACT("redact"),
    SHA256("sha256");

    Standard(String name) {
      this.name = name;
    }

    private final String name;

    public String getName() {
      return name;
    }

    public DataMask build(TypeDescription schema, String... params) {
      return Factory.build(name, schema, params);
    }
  }

  /**
   * Mask the given range of values
   * @param original the original input data
   * @param masked the masked output data
   * @param start the first data element to mask
   * @param length the number of data elements to mask
   */
  void maskData(ColumnVector original, ColumnVector masked,
                int start, int length);


  /**
   * Providers can provide one or more kinds of data masks.
   * Because they are discovered using a service loader, they may be added
   * by third party jars.
   */
  interface Provider {
    /**
     * Build a mask with the given parameters.
     * @param name the kind of masking
     * @param schema the type of the field
     * @param params the list of parameters with the name in params[0]
     * @return the new data mask or null if this name is unknown
     */
    DataMask build(String name, TypeDescription schema, String... params);
  }

  /**
   * To create a DataMask, the users should come through this API.
   *
   * It supports extension via additional DataMask.Provider implementations
   * that are accessed through Java's ServiceLoader API.
   */
  class Factory {
    private static final ServiceLoader<Provider> LOADER =
        ServiceLoader.load(Provider.class);

    /**
     * Build a new DataMask instance.
     * @param name the name of the mask
     * @param schema the type of the field
     * @param params a list of parameters to the mask
     * @return a new DataMask
     * @throws IllegalArgumentException if no such kind of data mask was found
     *
     * @see org.apache.orc.impl.mask.MaskProvider for the standard provider
     */
    public static DataMask build(String name,
                           TypeDescription schema,
                           String... params) {
      for(Provider provider: LOADER) {
        DataMask result = provider.build(name, schema, params);
        if (result != null) {
          return result;
        }
      }
      StringBuilder msg = new StringBuilder();
      msg.append("Can't find data mask - ");
      msg.append(name);
      for(int i=0; i < params.length; ++i) {
        msg.append(", ");
        msg.append(params[i]);
      }
      throw new IllegalArgumentException(msg.toString());
    }
  }
}
