package org.apache.orc.impl.reader.tree;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;

public interface TypeReader {
  void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException;

  void startStripe(StripePlanner planner) throws IOException;

  void seek(PositionProvider[] index) throws IOException;

  void seek(PositionProvider index) throws IOException;

  void skipRows(long rows) throws IOException;

  void nextVector(ColumnVector previous,
                  boolean[] isNull,
                  int batchSize) throws IOException;

  int getColumnId();
}
