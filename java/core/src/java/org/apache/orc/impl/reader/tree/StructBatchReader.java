package org.apache.orc.impl.reader.tree;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.TreeReaderFactory;

import java.io.IOException;

public class StructBatchReader extends BatchReader<TreeReaderFactory.StructTreeReader> {

  public StructBatchReader(TreeReaderFactory.StructTreeReader rowReader) {
    super(rowReader);
  }

  @Override
  public void nextBatch(VectorizedRowBatch batch, int batchSize) throws IOException {
    for (int i = 0; i < rootType.fields.length &&
                    (vectorColumnCount == -1 || i < vectorColumnCount); ++i) {
      ColumnVector colVector = batch.cols[i];
      if (colVector != null) {
        colVector.reset();
        colVector.ensureSize(batchSize, false);
        rootType.fields[i].nextVector(colVector, null, batchSize);
      }
    }
    resetBatch(batch, batchSize);
  }
}
