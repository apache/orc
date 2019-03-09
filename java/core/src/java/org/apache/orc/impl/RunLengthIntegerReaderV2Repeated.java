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
package org.apache.orc.impl;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader that reads a sequence of light weight compressed integers. Refer
 * {@link RunLengthIntegerWriterV2} for description of various lightweight
 * compression techniques.
 */
public class RunLengthIntegerReaderV2Repeated extends RunLengthIntegerReaderV2 {
  public static final Logger LOG = LoggerFactory.getLogger(RunLengthIntegerReaderV2Repeated.class);

  private boolean areBatchLiteralsRepeating = false;
  private final LookaheadStruct ls = new LookaheadStruct();

  public RunLengthIntegerReaderV2Repeated(InStream input, boolean signed,
      boolean skipCorrupt) throws IOException {
    super(input, signed, skipCorrupt);
  }

  @Override
  protected void readValues(boolean ignoreEof) throws IOException {
    areBatchLiteralsRepeating = false;
    super.readValues(ignoreEof);
  }

  @Override
  public void nextVector(ColumnVector previous,
                         long[] data,
                         int previousLen) throws IOException {
    // if all nulls, just return
    if (previous.isRepeating && !previous.noNulls && previous.isNull[0]) {
      return;
    }
    // We are going to optimistically assume the vector is repeating, and keep reading
    // and not writing to the vector until we are proven wrong.
    // TODO: does it make sense to do in case of !noNulls? That means either there are SOME
    //       nulls, so by definition we cannot be repeating, or that our caller is lazy and
    //       didn't set noNulls or isRepeating properly.
    previous.isRepeating = true;
    if (!previous.noNulls) {
      // We assume that if there are some nulls, but not all nulls, this is unlikely to be
      // repeating, so just take the standard path.
      int dataIx = 0;
      if (used < numLiterals) {
        // 1) Flatten the literals for the general case.
        if (areBatchLiteralsRepeating) {
          Arrays.fill(literals, used, numLiterals, literals[0]);
          areBatchLiteralsRepeating = false;
        }
        // 2) Populate vector from these literals.
        dataIx = nextVectorConsumeLiteralsWithNullsAfterRead(previous, data, previousLen, 0);
      }
      // 3) Go back to the general case.
      nextVectorGeneralCase(previous, data, previousLen, dataIx);
      return;
    }

    // Read literals remaining from previous read and see if they are repeating.
    ls.repeats = nextVectorReadLeftoversForLookahead(previous, data, previousLen);
    if (!previous.isRepeating) {
      // We are only here if areBatchLiteralsRepeating was false (otherwise we'd definitely
      // still be isRepeating); the literals were already copied into data. Back to general case.
      nextVectorGeneralCase(previous, data, previousLen, ls.repeats);
      return;
    }

    ls.value = data[0]; // repeats == 0 would make the value irrelevant.
    while (ls.repeats < previousLen) {
      assert numLiterals == used;
      numLiterals = used = 0;
      // read the first 2 bits and determine the encoding type
      int firstByte = input.read();
      areBatchLiteralsRepeating = false;
      boolean hasMore = handleFirstByte(false, firstByte);
      assert hasMore; // Would have thrown otherwise.
      boolean doContinueLookahead = true;

      switch (currentEncoding) {
      case DIRECT: {
        doContinueLookahead = false;
        readDirectValues(firstByte);
        break;
      }
      case PATCHED_BASE: {
        doContinueLookahead = false;
        readPatchedBaseValues(firstByte);
        break;
      }
      case DELTA: {
        doContinueLookahead = readDeltaValues(firstByte, ls);
        break;
      }
      case SHORT_REPEAT: {
        doContinueLookahead = readShortRepeatValues(firstByte, ls);
        break;
      }
      default: throw new IOException("Unknown encoding " + currentEncoding);
      }
      if (!doContinueLookahead) {
        // We just gave up on the lookeahead.
        // 1) Fill the start of the vector with values, like next() would have done.
        if (ls.repeats > 0) {
          Arrays.fill(data, 0, ls.repeats, ls.value);
        }
        // 2) Finish with the literals we've read above.
        int ix = nextVectorConsumeLiteralsNoNullAfterRead(previous, data, previousLen, ls.repeats);
        // 3) Return to the general case.
        nextVectorGeneralCase(previous, data, previousLen, ix);
        return;
      }

      data[0] = ls.value; // We are still trying the lookahead.
    }
    int tooFarAheadSize = ls.repeats - previousLen;
    assert tooFarAheadSize >= 0;
    assert used == numLiterals;
    if (tooFarAheadSize == 0) return;
    // There are more repeating values than we need for this vector. Save into literals.
    // nextVector MUST be called to handle areBatchLiteralsRepeating; "next()" will not work.
    used = 0;
    numLiterals = tooFarAheadSize;
    literals[0] = ls.value;
    areBatchLiteralsRepeating = true;
  }

  private void nextVectorGeneralCase(
      ColumnVector previous, long[] data, int previousLen, int ix) throws IOException {
    // Repeatedly fill and drain literals until we are done. Assumes no current literals.
    while (ix < previousLen) {
      assert numLiterals == used;
      numLiterals = used = 0;
      // read the first 2 bits and determine the encoding type
      int firstByte = input.read();
      areBatchLiteralsRepeating = false;
      boolean hasMore = handleFirstByte(false, firstByte);
      assert hasMore; // Would have thrown otherwise.

      readValuesFromEncoding(firstByte);

      // Consume some literals after read, and put them into data, as usual.
      if (previous.noNulls) {
        ix =  nextVectorConsumeLiteralsNoNullAfterRead(previous, data, previousLen, ix);
      } else {
        ix = nextVectorConsumeLiteralsWithNullsAfterRead(previous, data, previousLen, ix);
      }
    }
  }

  /**
   * After reading some literals, move them from literals to data, not checking isNull.
   * Assumes we have already given up on isRepeating optimization.
   * @return The next unfilled index in the data array.
   */
  private int nextVectorConsumeLiteralsNoNullAfterRead(
      ColumnVector previous, long[] data, int previousLen, int ix) {
    if (!previous.isRepeating) {
      // No nulls, not repeating - just copy. TODO: shortcut even earlier? read into data?
      int toCopy = Math.min(numLiterals - used, previousLen - ix);
      System.arraycopy(literals, used, data, ix, toCopy);
      used += toCopy;
      return ix + toCopy;
    } else {
      // This method is called when not batching, so isRepeating is very unlikely to be true.
      // TODO: We could just set isRepeating to false and arraycopy.
      // Same as nextVectorConsumeLiteralsWithNullsAfterRead, minus the nulls.
      while (ix < previousLen && used < numLiterals) {
        data[ix] = literals[used++];
        if (previous.isRepeating && ix > 0 && (data[0] != data[ix])) {
          previous.isRepeating = false;
        }
        ++ix;
      }
      return ix;
    }
  }

  /**
   * After reading some literals, move them from literals to data, checking isNull.
   * Assumes we have already given up on isRepeating optimization.
   * @return The next unfilled index in the data array.
   */
  private int nextVectorConsumeLiteralsWithNullsAfterRead(
      ColumnVector previous, long[] data, int previousLen, int ix) {
    while (ix < previousLen) {
      if (!previous.isNull[ix]) {
        if (used == numLiterals) {
          break;
        }
        data[ix] = literals[used++];
      } else {
        // The default value of null for int type in vectorized
        // processing is 1, so set that if the value is null
        data[ix] = 1;
      }

      if (previous.isRepeating && ix > 0 && (data[0] != data[ix] ||
          previous.isNull[0] != previous.isNull[ix])) {
        previous.isRepeating = false;
      }
      ++ix;
    }
    return ix;
  }

  /**
   * In the beginning of nextVector, moves any remaining literals into data[]; makes assumptions
   * about isRepeating handling in nextVector.
   * @return The next unfilled index in the data array.
   */
  private int nextVectorReadLeftoversForLookahead(
      ColumnVector previous, long[] data, int previousLen) {
    // Copy remaining literals from the previous read.
    assert previous.noNulls;
    int leftOver = Math.min(previousLen, numLiterals - used);
    if (leftOver == 0) return 0;
    long repeatingValue = literals[0];
    if (!areBatchLiteralsRepeating) {
      // If leftovers are not repeating, verify them element by element.
      repeatingValue = literals[used];
      for (int i = 1; i < leftOver; ++i) {
        if (literals[used + i] != repeatingValue) {
          previous.isRepeating = false;
          // Just copy the literals into data; the caller will fall back to general case.
          System.arraycopy(literals, used, data, 0, leftOver);
          break;
        }
      }
    }
    used += leftOver;
    if (used == numLiterals) {
      areBatchLiteralsRepeating = false; // Done with repeating literals, if any.
    }
    if (previous.isRepeating) {
      // Initialize the 0th element, if we still hope for isRepeating.
      data[0] = repeatingValue;
    }
    return leftOver;
  }
}
