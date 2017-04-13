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
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Test;
import org.mockito.Mockito;

public class StringWriterTest extends StringWriter {

	@Test
	public void testWrite() throws UnsupportedEncodingException {
		BytesColumnVector v = new BytesColumnVector();
		VectorizedRowBatch batch = Mockito.mock(VectorizedRowBatch.class);
		batch.cols = new ColumnVector[2];
		batch.cols[0] = v;
		
		write(batch, 0, 1, "string");

		assertEquals(null, v.vector[0]);
		byte[] data = Arrays.copyOfRange(v.vector[1], v.start[1], v.length[1]);
		assertTrue(Arrays.equals("string".getBytes("UTF-8"), data));
	}
}
