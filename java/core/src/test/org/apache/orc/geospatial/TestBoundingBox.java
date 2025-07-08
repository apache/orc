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
package org.apache.orc.geospatial;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBoundingBox {
  @ParameterizedTest
  @MethodSource("emptyTestCases")
  public void testIsXEmpty(double xMin, double xMax, boolean expected) {
    BoundingBox bbox = new BoundingBox(xMin, xMax, 1, 2, 1, 2, 1, 2);
    assertEquals(expected, bbox.isXEmpty());
  }

  @ParameterizedTest
  @MethodSource("emptyTestCases")
  public void testIsYEmpty(double yMin, double yMax, boolean expected) {
    BoundingBox bbox = new BoundingBox(1, 2, yMin, yMax, 1, 2, 1, 2);
    assertEquals(expected, bbox.isYEmpty());
  }

  @ParameterizedTest
  @MethodSource("emptyTestCases")
  public void testIsZEmpty(double zMin, double zMax, boolean expected) {
    BoundingBox bbox = new BoundingBox(1, 2, 1, 2, zMin, zMax, 1, 2);
    assertEquals(expected, bbox.isZEmpty());
  }

  @ParameterizedTest
  @MethodSource("emptyTestCases")
  public void testIsMEmpty(double mMin, double mMax, boolean expected) {
    BoundingBox bbox = new BoundingBox(1, 2, 1, 2, 1, 2, mMin, mMax);
    assertEquals(expected, bbox.isMEmpty());
  }

  private static Stream<Arguments> emptyTestCases() {
    return Stream.of(
            // Initial state
            Arguments.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, true),

            // Normal range
            Arguments.of(1, 2, false),

            // Reverse boundary
            Arguments.of(2, 1, true),

            // Equal boundary
            Arguments.of(5, 5, false),

            // NaN values
            Arguments.of(Double.NaN, 2, false),
            Arguments.of(1, Double.NaN, false),
            Arguments.of(Double.NaN, Double.NaN, false)
    );
  }
}
