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
package org.apache.orc.pojowriter.writer;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.orc.TypeDescription;

/**
 * Class that contains type mapping between java and orc type writers.
 * Default initialization:
 *     int -> Int
 *     Integer -> Int
 *     long -> Long
 *     Long -> Long
 *     String -> String
 *     java.util.Date -> Date
 *     boolean -> Boolean
 *     Boolean -> Boolean
 *     double -> Double
 *     Double -> Double
 *     Timestamp -> Timestamp
 *     BigDecimal -> Decimal
 */
public class WriterMappingRegistry {
  volatile private Map<Class<?>, WriterMapping<?>> registry = new HashMap<>();

  public WriterMappingRegistry() {
    registry.put(int.class,
        new WriterMapping<>(new IntWriter(), TypeDescription.createInt()));
    registry.put(Integer.class,
        new WriterMapping<>(new IntWriter(), TypeDescription.createLong()));
    registry.put(long.class,
        new WriterMapping<>(new LongWriter(), TypeDescription.createLong()));
    registry.put(Long.class,
        new WriterMapping<>(new LongWriter(), TypeDescription.createLong()));
    registry.put(String.class, new WriterMapping<>(new StringWriter(),
        TypeDescription.createString()));
    registry.put(Date.class,
        new WriterMapping<>(new DateWriter(), TypeDescription.createDate()));
    registry.put(Boolean.class, new WriterMapping<>(new BooleanWriter(),
        TypeDescription.createBoolean()));
    registry.put(boolean.class, new WriterMapping<>(new BooleanWriter(),
        TypeDescription.createBoolean()));
    registry.put(double.class, new WriterMapping<>(new DoubleWriter(),
        TypeDescription.createDouble()));
    registry.put(Double.class, new WriterMapping<>(new DoubleWriter(),
        TypeDescription.createDouble()));
    registry.put(Timestamp.class, new WriterMapping<>(new TimestampWriter(),
        TypeDescription.createTimestamp()));
    registry.put(BigDecimal.class, new WriterMapping<>(new DecimalWriter(),
        TypeDescription.createDecimal()));
  }

  /**
   * Defines or overwrites a type mapping
   * @param c java class to map
   * @param mapping writer mapping implementation
   */
  public void register(Class<?> c, WriterMapping<?> mapping) {
    registry.put(c, mapping);
  }

  /**
   * Resolves a type writer mapping for a java class
   * @param c Class to resolve
   * @return
   */
  public WriterMapping<?> getMapping(Class<?> c) {
    return registry.get(c);
  }

  /**
   * Resolves a type writer for a java class
   * @param c Class to resolve
   * @return
   */
  public Writer getWriter(Class<?> c) {
    return registry.get(c).getWriter();
  }

  /**
   * Resolves a TypeDescription for a java class
   * @param c Class to resolve
   * @return
   */
  public TypeDescription getTypeDescription(Class<?> c) {
    return registry.get(c).getTypeDescription().clone();
  }
}
