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

package org.apache.orc.impl.reader.tree;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.io.filter.FilterContext;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.reader.StripePlanner;

import java.io.IOException;

/**
 * Wrapper reader that implements the level logic.
 * The methods are invoked on the reader only if the read level matches.
 */
public class LevelTypeReader implements TypeReader {
  private final TypeReader reader;

  public LevelTypeReader(TypeReader reader) {
    this.reader = reader;
  }

  @Override
  public void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException {
    reader.checkEncoding(encoding);
  }

  @Override
  public void startStripe(StripePlanner planner, ReadLevel rLevel) throws IOException {
    if (reader.getReadLevel() != rLevel) {
      return;
    }

    reader.startStripe(planner, rLevel);
  }

  @Override
  public void seek(PositionProvider[] index, ReadLevel rLevel) throws IOException {
    if (reader.getReadLevel() != rLevel) {
      return;
    }

    reader.seek(index, rLevel);
  }

  @Override
  public void seek(PositionProvider index, ReadLevel rLevel) throws IOException {
    if (reader.getReadLevel() != rLevel) {
      return;
    }

    reader.seek(index, rLevel);
  }

  @Override
  public void skipRows(long rows, ReadLevel rLevel) throws IOException {
    if (reader.getReadLevel() != rLevel) {
      return;
    }

    reader.skipRows(rows, rLevel);
  }

  @Override
  public void nextVector(ColumnVector previous,
                         boolean[] isNull,
                         int batchSize,
                         FilterContext filterContext,
                         ReadLevel rLevel) throws IOException {
    if (reader.getReadLevel() != rLevel) {
      return;
    }

    reader.nextVector(previous, isNull, batchSize, filterContext, rLevel);
  }

  @Override
  public int getColumnId() {
    return reader.getColumnId();
  }

  @Override
  public ReadLevel getReadLevel() {
    return reader.getReadLevel();
  }

  public TypeReader getReader() {
    return reader;
  }
}
