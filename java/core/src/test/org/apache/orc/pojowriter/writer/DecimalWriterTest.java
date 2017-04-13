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

import java.math.BigDecimal;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.Test;
import org.mockito.Mockito;

public class DecimalWriterTest extends DecimalWriter {

	@Test
	public void testWrite() {
		DecimalColumnVector v = new DecimalColumnVector(2, 1, 0);
		VectorizedRowBatch batch = Mockito.mock(VectorizedRowBatch.class);
		batch.cols = new ColumnVector[2];
		batch.cols[0] = v;
		BigDecimal bigDecimal = new BigDecimal(123456789.123456789);
		
		write(batch, 0, 1, bigDecimal);
		
		assertEquals(new HiveDecimalWritable(HiveDecimal.create(0.0)), v.vector[0]);
		assertEquals(bigDecimal.scale(), v.vector[1].scale());
		assertEquals(bigDecimal.precision(), v.vector[1].precision());
		assertEquals(bigDecimal, v.vector[1].getHiveDecimal().bigDecimalValue());
	}
}
