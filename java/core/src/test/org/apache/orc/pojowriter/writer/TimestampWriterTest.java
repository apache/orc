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

import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.Test;
import org.mockito.Mockito;

public class TimestampWriterTest extends TimestampWriter {

	@Test
	public void testWrite() {
		TimestampColumnVector v = new TimestampColumnVector(2);
		VectorizedRowBatch batch = Mockito.mock(VectorizedRowBatch.class);
		batch.cols = new ColumnVector[2];
		batch.cols[0] = v;
		Date date = new Date();
		Timestamp timestamp = new Timestamp(date.getTime());
		write(batch, 0, 1, timestamp);
		assertEquals(0L, v.time[0]);
		assertEquals(0L, v.nanos[0]);
		assertEquals(timestamp.getTime(), v.time[1]);
		assertEquals(timestamp.getNanos(), v.nanos[1]);
	}
}
