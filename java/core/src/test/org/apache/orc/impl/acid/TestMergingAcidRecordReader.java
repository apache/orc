/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.impl.acid;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

// TODO See comment on MergingAcidRecordReader
public class TestMergingAcidRecordReader extends AcidTestBase {

  private Path buildBase() throws IOException {
    return insertFile("base_10/bucket_0", new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        new long[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
        new String[] {
          "When in the course of human events",
          "it becomes necessary for one people to dissolve the political bands",
          "THIS LINE INTENIONALLY LEFT BLANK",
          "which have connected them with another",
          "and to assume among the powers of the earth,",
          "the separate and equal station to which",
          "the Laws of Nature and of Nature's God entitle them,",
          "THIS LINE INTENIONALLY LEFT BLANK",
          "a decent respect to the opinions of mankind requires",
          "that they should declare the causes which impel them to the separation."
        });
  }

  private Path buildInsertDelta() throws IOException {
    return insertFile("delta_11_11/bucket_0", new long[] {11, 11, 11}, new long[] {0, 0, 0},
        new String[] {
          "We hold these truths to be self-evident, that all men are created equal,",
          "that they are endowed by their Creator with certain unalienable Rights,",
          "that among these are Life, Liberty and the pursuit of Happiness."
        });
  }

  private Path buildUpdateInsertDelta() throws IOException {
    return insertFile("delta_12_12/bucket_0", new long[] {12}, new long[] {0},
        new String[] {"the Laws of Nature and of Nature's God entitle them,"});
  }
}
