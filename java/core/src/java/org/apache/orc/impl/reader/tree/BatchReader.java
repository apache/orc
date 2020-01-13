package org.apache.orc.impl.reader.tree;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;

public class BatchReader<T extends TypeReader> {
  // The row type reader
  public final T rootType;

  protected int vectorColumnCount = -1;

  public BatchReader(T rootType) {
    this.rootType = rootType;
  }

  public void startStripe(StripePlanner planner) throws IOException {
    rootType.startStripe(planner);
  }

  public void setVectorColumnCount(int vectorColumnCount) {
    this.vectorColumnCount = vectorColumnCount;
  }

  /**
   * Handle an elementary type
   *
   * @param batch     the batch to read into
   * @param batchSize the number of rows to read
   * @throws IOException
   */
  public void nextBatch(VectorizedRowBatch batch,
                        int batchSize) throws IOException {
    batch.cols[0].reset();
    batch.cols[0].ensureSize(batchSize, false);
    rootType.nextVector(batch.cols[0], null, batchSize);
    resetBatch(batch, batchSize);
  }

  protected void resetBatch(VectorizedRowBatch batch, int batchSize) {
    batch.selectedInUse = false;
    batch.size = batchSize;
  }

  public void skipRows(long rows) throws IOException {
    rootType.skipRows(rows);
  }

  public void seek(PositionProvider[] index) throws IOException {
    rootType.seek(index);
  }
}
