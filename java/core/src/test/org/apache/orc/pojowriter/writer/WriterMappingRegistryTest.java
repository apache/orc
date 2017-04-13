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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.pojowriter.writer.Writer;
import org.apache.orc.pojowriter.writer.WriterMapping;
import org.apache.orc.pojowriter.writer.WriterMappingRegistry;
import org.junit.Test;

public class WriterMappingRegistryTest extends WriterMappingRegistry {

	class DummyWriter implements Writer {
		@Override
		public void write(VectorizedRowBatch batch, int colIndex, int row, Object value)  {}
	}
	
	@Test
	public void testGetMapping() {
		TypeDescription typeDescription = TypeDescription.createStruct();
		DummyWriter dummyWriter = new DummyWriter();
		WriterMapping<DummyWriter> mapping = new WriterMapping<>(dummyWriter, typeDescription);
		register(int.class, mapping);
		assertEquals(mapping, getMapping(int.class));
	}

	@Test
	public void testGetWriter() {
		TypeDescription typeDescription = TypeDescription.createStruct();
		DummyWriter dummyWriter = new DummyWriter();
		WriterMapping<DummyWriter> mapping = new WriterMapping<>(dummyWriter, typeDescription);
		register(int.class, mapping);
		assertEquals(dummyWriter, getWriter(int.class));
	}

	@Test
	public void testGetTypeDescription() {
		TypeDescription typeDescription = TypeDescription.createStruct();
		DummyWriter dummyWriter = new DummyWriter();
		WriterMapping<DummyWriter> mapping = new WriterMapping<>(dummyWriter, typeDescription);
		register(int.class, mapping);
		assertEquals(typeDescription, getTypeDescription(int.class));
	}

}
