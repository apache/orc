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
package org.apache.orc.pojowriter;

import java.lang.reflect.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.pojowriter.writer.Writer;
import org.apache.orc.pojowriter.writer.WriterMappingRegistry;

/**
 * Provides functionality to write pojos to {@link VectorizedRowBatch}
 */
public class PojoWriter {

  protected Map<Class<?>, Map<String, Field>> fieldsCache = new HashMap<>();
  protected TypeDescription typeDescription;
  protected WriterMappingRegistry writerMappingRegistry = new WriterMappingRegistry();

  /**
   * Creates an instance for a defined TypeDescription.
   * See {@link PojoSchemaBuilder}
   * @param typeDescription Orc definition
   * @param writerMappingRegistry WriterMapping configuration
   */
  public PojoWriter(TypeDescription typeDescription,
      WriterMappingRegistry writerMappingRegistry) {
    this.writerMappingRegistry = writerMappingRegistry;
    this.typeDescription = typeDescription;
  }

  /**
   * Writes an object to a VectorizezRowBatch at the specified row.
   * @param obj Object to be written
   * @param batch Batch to be written to
   * @param rowIndex Row in batch that will be written
   * @return
   */
  public VectorizedRowBatch writeObject(Object obj, VectorizedRowBatch batch,
      int rowIndex) {
    Map<String, Field> fields = getFields(obj.getClass());
    List<String> tdNames = typeDescription.getFieldNames();

    int colIndex = 0;
    for (String td : tdNames) {
      Field field = fields.get(td);
      Writer writer = writerMappingRegistry.getWriter(field.getType());
      try {
        writer.write(batch, colIndex, rowIndex,
            FieldUtils.readField(field, obj, true));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      colIndex++;
    }
    return batch;
  }

  protected Map<String, Field> getFields(Class<?> clazz) {
    Map<String, Field> fieldMap = fieldsCache.get(clazz);

    if (fieldMap == null) {
      fieldMap = new HashMap<>();
      List<Field> fields = FieldUtils.getAllFieldsList(clazz);
      for (Field field : fields) {
        fieldMap.put(field.getName(), field);
      }
    }
    return fieldMap;
  }
}
