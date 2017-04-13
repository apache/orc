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

import java.io.Serializable;
import java.lang.reflect.Field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.orc.TypeDescription;
import org.apache.orc.pojowriter.annotation.OrcPojo;
import org.apache.orc.pojowriter.writer.WriterMappingRegistry;

/**
 * Class to generate an orc schema from a java class
 */
public class PojoSchemaBuilder {

  /**
   * Utility class for building a {@link TypeDescription} from a java class. If {@link OrcPojo} is present fields are taken into account,
   * otherwise all fields supported by writerMappingRegistry
   * @param clazz
   * @param writerMappingRegistry
   * @return
   */
  public TypeDescription build(Class<?> clazz, WriterMappingRegistry writerMappingRegistry ) {
    TypeDescription td = TypeDescription.createStruct();
    List<Field> fields = null;

    OrcPojo orcPojo = clazz.getAnnotation(OrcPojo.class);
    if (orcPojo != null) {
      fields = new ArrayList<>();
      String[] fieldNames = orcPojo.fields();
      for (int i = 0; i < fieldNames.length; i++) {
        fields.add(FieldUtils.getField(clazz, fieldNames[i], true));
      }
    } else {
      fields = FieldUtils.getAllFieldsList(clazz);
      AlphabeticalComparator comparator = new AlphabeticalComparator();
      Collections.sort(fields, comparator);
    }

    writerMappingRegistry = new WriterMappingRegistry();
    for (Field field : fields) {
      td.addField(field.getName(),
          writerMappingRegistry.getTypeDescription(field.getType()));
    }
    return td;
  }

  private static class AlphabeticalComparator implements Comparator<Field>, Serializable {
    public int compare(Field o1, Field o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }
}
