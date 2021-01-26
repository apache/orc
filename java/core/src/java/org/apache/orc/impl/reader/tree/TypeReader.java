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
import java.util.EnumSet;

public interface TypeReader {
  void checkEncoding(OrcProto.ColumnEncoding encoding) throws IOException;

  void startStripe(StripePlanner planner, EnumSet<ReadLevel> readLevel) throws IOException;

  void seek(PositionProvider[] index, EnumSet<ReadLevel> readLevel) throws IOException;

  void seek(PositionProvider index, EnumSet<ReadLevel> readLevel) throws IOException;

  void skipRows(long rows, EnumSet<ReadLevel> readLevel) throws IOException;

  void nextVector(ColumnVector previous,
                  boolean[] isNull,
                  int batchSize,
                  FilterContext filterContext,
                  EnumSet<ReadLevel> readLevel) throws IOException;

  int getColumnId();

  ReadLevel getReadLevel();

  /**
   * Determines if the child of the parent should be allowed based on the read level. The child
   * is allowed based on the read level or if the child is a LEAD_PARENT, this allows the handling
   * of FOLLOW children on the LEAD_PARENT
   * @param reader the child reader that is being evaluated
   * @param readLevel the requested read level
   * @return true if allowed by read level or if it is a LEAD_PARENT otherwise false
   */
  static boolean allowChild(TypeReader reader, EnumSet<ReadLevel> readLevel) {
    return readLevel.contains(reader.getReadLevel())
           || reader.getReadLevel() == ReadLevel.LEAD_PARENT;
  }

  enum ReadLevel {
    LEAD_CHILD,    // Read only the elementary filter columns
    LEAD_PARENT,   // Read only the parent filter columns e.g. Struct and Union
    FOLLOW;        // Read the non-filter columns

    public static final EnumSet<ReadLevel> ALL = EnumSet.allOf(ReadLevel.class);
    public static final EnumSet<ReadLevel> LEADERS = EnumSet.of(LEAD_PARENT, LEAD_CHILD);
    public static final EnumSet<ReadLevel> FOLLOWERS = EnumSet.of(FOLLOW);
    public static final EnumSet<ReadLevel> LEADER_PARENTS = EnumSet.of(LEAD_PARENT);
    public static final EnumSet<ReadLevel> FOLLOWERS_WITH_PARENTS = EnumSet.of(LEAD_PARENT, FOLLOW);
  }
}
