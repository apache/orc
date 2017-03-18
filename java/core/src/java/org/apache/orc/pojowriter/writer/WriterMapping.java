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

import org.apache.orc.TypeDescription;

/**
 * Associates a writer with a java with a orc type
 * @param <V>
 */
public class WriterMapping<V extends Writer> {

  private V writer;
  private TypeDescription typeDescription;

  public WriterMapping(V writer, TypeDescription typeDescription) {
    this.writer = writer;
    this.typeDescription = typeDescription;
  }

  public V getWriter() {
    return writer;
  }

  public void setWriter(V writer) {
    this.writer = writer;
  }

  public TypeDescription getTypeDescription() {
    return typeDescription;
  }

  public void setTypeDescription(TypeDescription typeDescription) {
    this.typeDescription = typeDescription;
  }
}
