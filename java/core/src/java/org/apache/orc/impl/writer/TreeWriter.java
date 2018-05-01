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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * The writers for the specific writers of each type. This provides
 * the generic API that they must all implement.
 */
public interface TreeWriter {

  /**
   * Estimate the memory currently used to buffer the stripe.
   * @return the number of bytes
   */
  long estimateMemory();

  /**
   * Estimate the memory used if the file was read into Hive's Writable
   * types. This is used as an estimate for the query optimizer.
   * @return the number of bytes
   */
  long getRawDataSize();

  /**
   * Write a VectorizedRowBath to the file. This is called by the WriterImplV2
   * at the top level.
   * @param batch the list of all of the columns
   * @param offset the first row from the batch to write
   * @param length the number of rows to write
   */
  void writeRootBatch(VectorizedRowBatch batch, int offset,
                      int length) throws IOException;

  /**
   * Write a ColumnVector to the file. This is called recursively by
   * writeRootBatch.
   * @param vector the data to write
   * @param offset the first value offset to write.
   * @param length the number of values to write
   */
  void writeBatch(ColumnVector vector, int offset,
                  int length) throws IOException;

  /**
   * Create a row index entry at the current point in the stripe.
   */
  void createRowIndexEntry() throws IOException;

  /**
   * Flush the TreeWriter stream
   * @throws IOException
   */
  void flushStreams() throws IOException;

  /**
   * Write the stripe out to the file.
   * @param stripeFooter the stripe footer that contains the information about the
   *                layout of the stripe. The TreeWriterBase is required to update
   *                the footer with its information.
   * @param stats the stripe statistics information
   * @param requiredIndexEntries the number of index entries that are
   *                             required. this is to check to make sure the
   *                             row index is well formed.
   */
  void writeStripe(OrcProto.StripeFooter.Builder stripeFooter,
                   OrcProto.StripeStatistics.Builder stats,
                   int requiredIndexEntries) throws IOException;

  /**
   * During a stripe append, we need to update the file statistics.
   * @param stripeStatistics the statistics for the new stripe
   */
  void updateFileStatistics(OrcProto.StripeStatistics stripeStatistics);

  /**
   * Add the file statistics to the file footer.
   * @param footer the file footer builder
   */
  void writeFileStatistics(OrcProto.Footer.Builder footer);

  class Factory {
    public static TreeWriter create(TypeDescription schema,
                                    WriterContext streamFactory,
                                    boolean nullable) throws IOException {
      OrcFile.Version version = streamFactory.getVersion();
      switch (schema.getCategory()) {
        case BOOLEAN:
          return new BooleanTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case BYTE:
          return new ByteTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case SHORT:
        case INT:
        case LONG:
          return new IntegerTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case FLOAT:
          return new FloatTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case DOUBLE:
          return new DoubleTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case STRING:
          return new StringTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case CHAR:
          return new CharTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case VARCHAR:
          return new VarcharTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case BINARY:
          return new BinaryTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case TIMESTAMP:
          return new TimestampTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case DATE:
          return new DateTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case DECIMAL:
          if (version == OrcFile.Version.UNSTABLE_PRE_2_0 &&
              schema.getPrecision() <= TypeDescription.MAX_DECIMAL64_PRECISION) {
            return new Decimal64TreeWriter(schema.getId(),
                schema, streamFactory, nullable);
          }
          return new DecimalTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case STRUCT:
          return new StructTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case MAP:
          return new MapTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case LIST:
          return new ListTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        case UNION:
          return new UnionTreeWriter(schema.getId(),
              schema, streamFactory, nullable);
        default:
          throw new IllegalArgumentException("Bad category: " +
              schema.getCategory());
      }
    }

  }
}
