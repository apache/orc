package org.apache.orc.impl.writer;

import org.apache.orc.impl.PositionRecorder;

/**
 * A record of the current position in the stream when a chunk starts so that
 * we can play it back when the writer needs an index position.
 */
final public class ChunkStartPosition implements PositionRecorder {
  private int length = 0;
  private long[] values = new long[5];

  @Override
  public void addPosition(long offset) {
    if (length == values.length) {
      long[] newValues = new long[values.length * 2];
      System.arraycopy(values, 0, newValues, 0, values.length);
      values = newValues;
    }
    values[length++] = offset;
  }

  public void reset() {
    length = 0;
  }

  /**
   * Play back the saved position.
   * @param output where to record the position.
   */
  public void playbackPosition(PositionRecorder output) {
    for(int i=0; i < length; ++i) {
      output.addPosition(values[i]);
    }
  }
}
