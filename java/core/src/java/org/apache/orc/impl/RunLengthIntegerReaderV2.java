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

import java.io.EOFException;
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
public class RunLengthIntegerReaderV2 implements IntegerReader {
  public static final Logger LOG = LoggerFactory.getLogger(RunLengthIntegerReaderV2.class);

  private InStream input;
  private final boolean signed;
  private final long[] literals = new long[RunLengthIntegerWriterV2.MAX_SCOPE];
  // Note: isRepeating flag in this reader was never used; this flag only refers to nextVector opti
  private boolean areBatchLiteralsRepeating = false;
  private int numLiterals = 0;
  private int used = 0;
  private final boolean skipCorrupt;
  private final SerializationUtils utils;
  private RunLengthIntegerWriterV2.EncodingType currentEncoding;
  private final LookaheadStruct ls = new LookaheadStruct();

  public RunLengthIntegerReaderV2(InStream input, boolean signed,
      boolean skipCorrupt) throws IOException {
    this.input = input;
    this.signed = signed;
    this.skipCorrupt = skipCorrupt;
    this.utils = new SerializationUtils();
  }

  private final static RunLengthIntegerWriterV2.EncodingType[] encodings = RunLengthIntegerWriterV2.EncodingType.values();
  private void readValues(boolean ignoreEof) throws IOException {
    // read the first 2 bits and determine the encoding type
    int firstByte = input.read();
    if (!handleFirstByte(ignoreEof, firstByte)) {
      return;
    }
    switch (currentEncoding) {
    case SHORT_REPEAT: readShortRepeatValues(firstByte, null); break;
    case DIRECT: readDirectValues(firstByte); break;
    case PATCHED_BASE: readPatchedBaseValues(firstByte); break;
    case DELTA: readDeltaValues(firstByte, null); break;
    default: throw new IOException("Unknown encoding " + currentEncoding);
    }
  }

  private boolean handleFirstByte(boolean ignoreEof, int firstByte) throws EOFException {
    areBatchLiteralsRepeating = false;
    if (firstByte < 0) {
      if (!ignoreEof) {
        throw new EOFException("Read past end of RLE integer from " + input);
      }
      used = numLiterals = 0;
      return false;
    }
    currentEncoding = encodings[(firstByte >>> 6) & 0x03];
    return true;
  }

  private boolean readDeltaValues(
      int firstByte, LookaheadStruct expected) throws IOException {

    // extract the number of fixed bits
    int fb = (firstByte >>> 1) & 0x1f;
    if (fb != 0) {
      fb = utils.decodeBitWidth(fb);
    }

    // extract the blob run length
    int len = (firstByte & 0x01) << 8;
    len |= input.read();

    // read the first value stored as vint
    long firstVal = 0;
    if (signed) {
      firstVal = SerializationUtils.readVslong(input);
    } else {
      firstVal = SerializationUtils.readVulong(input);
    }

    // store first value to result buffer
    long prevVal = firstVal;
    literals[numLiterals++] = firstVal;

    // if fixed bits is 0 then all values have fixed delta
    if (fb == 0) {
      // read the fixed delta value stored as vint (deltas can be negative even
      // if all number are positive)
      long fd = SerializationUtils.readVslong(input);
      if (fd == 0) {
        if (expected != null && (expected.value == firstVal || expected.repeats == 0)) {
          // Don't fill literals; the values are still repeating.
          expected.value = firstVal;
          expected.repeats += (len + 1);
          numLiterals = used = 0;
          return true;
        }
        assert numLiterals == 1;
        Arrays.fill(literals, numLiterals, numLiterals + len, literals[0]);
        numLiterals += len;
      } else {
        // add fixed deltas to adjacent values
        for(int i = 0; i < len; i++) {
          literals[numLiterals++] = literals[numLiterals - 2] + fd;
        }
      }
    } else {
      long deltaBase = SerializationUtils.readVslong(input);
      // add delta base and first value
      literals[numLiterals++] = firstVal + deltaBase;
      prevVal = literals[numLiterals - 1];
      len -= 1;

      // write the unpacked values, add it to previous value and store final
      // value to result buffer. if the delta base value is negative then it
      // is a decreasing sequence else an increasing sequence
      utils.readInts(literals, numLiterals, len, fb, input);
      while (len > 0) {
        if (deltaBase < 0) {
          literals[numLiterals] = prevVal - literals[numLiterals];
        } else {
          literals[numLiterals] = prevVal + literals[numLiterals];
        }
        prevVal = literals[numLiterals];
        len--;
        numLiterals++;
      }
    }
    return false;
  }

  private void readPatchedBaseValues(int firstByte) throws IOException {

    // extract the number of fixed bits
    int fbo = (firstByte >>> 1) & 0x1f;
    int fb = utils.decodeBitWidth(fbo);

    // extract the run length of data blob
    int len = (firstByte & 0x01) << 8;
    len |= input.read();
    // runs are always one off
    len += 1;

    // extract the number of bytes occupied by base
    int thirdByte = input.read();
    int bw = (thirdByte >>> 5) & 0x07;
    // base width is one off
    bw += 1;

    // extract patch width
    int pwo = thirdByte & 0x1f;
    int pw = utils.decodeBitWidth(pwo);

    // read fourth byte and extract patch gap width
    int fourthByte = input.read();
    int pgw = (fourthByte >>> 5) & 0x07;
    // patch gap width is one off
    pgw += 1;

    // extract the length of the patch list
    int pl = fourthByte & 0x1f;

    // read the next base width number of bytes to extract base value
    long base = utils.bytesToLongBE(input, bw);
    long mask = (1L << ((bw * 8) - 1));
    // if MSB of base value is 1 then base is negative value else positive
    if ((base & mask) != 0) {
      base = base & ~mask;
      base = -base;
    }

    // unpack the data blob
    long[] unpacked = new long[len];
    utils.readInts(unpacked, 0, len, fb, input);

    // unpack the patch blob
    long[] unpackedPatch = new long[pl];

    if ((pw + pgw) > 64 && !skipCorrupt) {
      throw new IOException("Corruption in ORC data encountered. To skip" +
          " reading corrupted data, set hive.exec.orc.skip.corrupt.data to" +
          " true");
    }
    int bitSize = utils.getClosestFixedBits(pw + pgw);
    utils.readInts(unpackedPatch, 0, pl, bitSize, input);

    // apply the patch directly when decoding the packed data
    int patchIdx = 0;
    long currGap = 0;
    long currPatch = 0;
    long patchMask = ((1L << pw) - 1);
    currGap = unpackedPatch[patchIdx] >>> pw;
    currPatch = unpackedPatch[patchIdx] & patchMask;
    long actualGap = 0;

    // special case: gap is >255 then patch value will be 0.
    // if gap is <=255 then patch value cannot be 0
    while (currGap == 255 && currPatch == 0) {
      actualGap += 255;
      patchIdx++;
      currGap = unpackedPatch[patchIdx] >>> pw;
      currPatch = unpackedPatch[patchIdx] & patchMask;
    }
    // add the left over gap
    actualGap += currGap;

    // unpack data blob, patch it (if required), add base to get final result
    for(int i = 0; i < unpacked.length; i++) {
      if (i == actualGap) {
        // extract the patch value
        long patchedVal = unpacked[i] | (currPatch << fb);

        // add base to patched value
        literals[numLiterals++] = base + patchedVal;

        // increment the patch to point to next entry in patch list
        patchIdx++;

        if (patchIdx < pl) {
          // read the next gap and patch
          currGap = unpackedPatch[patchIdx] >>> pw;
          currPatch = unpackedPatch[patchIdx] & patchMask;
          actualGap = 0;

          // special case: gap is >255 then patch will be 0. if gap is
          // <=255 then patch cannot be 0
          while (currGap == 255 && currPatch == 0) {
            actualGap += 255;
            patchIdx++;
            currGap = unpackedPatch[patchIdx] >>> pw;
            currPatch = unpackedPatch[patchIdx] & patchMask;
          }
          // add the left over gap
          actualGap += currGap;

          // next gap is relative to the current gap
          actualGap += i;
        }
      } else {
        // no patching required. add base to unpacked value to get final value
        literals[numLiterals++] = base + unpacked[i];
      }
    }

  }

  private void readDirectValues(int firstByte) throws IOException {

    // extract the number of fixed bits
    int fbo = (firstByte >>> 1) & 0x1f;
    int fb = utils.decodeBitWidth(fbo);

    // extract the run length
    int len = (firstByte & 0x01) << 8;
    len |= input.read();
    // runs are one off
    len += 1;

    // write the unpacked values and zigzag decode to result buffer
    utils.readInts(literals, numLiterals, len, fb, input);
    if (signed) {
      for(int i = 0; i < len; i++) {
        literals[numLiterals] = utils.zigzagDecode(literals[numLiterals]);
        numLiterals++;
      }
    } else {
      numLiterals += len;
    }
  }

  private boolean readShortRepeatValues(
      int firstByte, LookaheadStruct expected) throws IOException {
    // read the run length
    int len = firstByte & 0x07;
    // run lengths values are stored only after MIN_REPEAT value is met
    len += RunLengthIntegerWriterV2.MIN_REPEAT;

    // read the number of bytes occupied by the value
    int size = (firstByte >>> 3) & 0x07;
    // #bytes are one off
    size += 1;

    // read the repeated value which is store using fixed bytes
    long val = utils.bytesToLongBE(input, size);

    if (signed) {
      val = utils.zigzagDecode(val);
    }

    if (expected != null && (expected.value == val || expected.repeats == 0)) {
      expected.value = val;
      expected.repeats += len;
      numLiterals = used = 0;
      return true;
    }

    if (numLiterals != 0) {
      // Currently this always holds, which makes peekNextAvailLength simpler.
      // If this changes, peekNextAvailLength should be adjusted accordingly.
      throw new AssertionError("readValues called with existing values present");
    }
    // repeat the value for length times
    for(int i = 0; i < len; i++) {
      literals[i] = val;
    }
    numLiterals = len;
    return false;
  }

  @Override
  public boolean hasNext() throws IOException {
    return used != numLiterals || input.available() > 0;
  }

  @Override
  public long next() throws IOException {
    long result;
    if (used == numLiterals) {
      numLiterals = 0;
      used = 0;
      readValues(false);
    }
    result = literals[used++];
    return result;
  }

  private static final class LookaheadStruct {
    public long value;
    public int repeats;
  }


  @Override
  public void seek(PositionProvider index) throws IOException {
    input.seek(index);
    int consumed = (int) index.getNext();
    if (consumed != 0) {
      // a loop is required for cases where we break the run into two
      // parts
      while (consumed > 0) {
        numLiterals = 0;
        readValues(false);
        used = consumed;
        consumed -= numLiterals;
      }
    } else {
      used = 0;
      numLiterals = 0;
    }
  }

  @Override
  public void skip(long numValues) throws IOException {
    while (numValues > 0) {
      if (used == numLiterals) {
        numLiterals = 0;
        used = 0;
        readValues(false);
      }
      long consume = Math.min(numValues, numLiterals - used);
      used += consume;
      numValues -= consume;
    }
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
      boolean hasMore = handleFirstByte(false, firstByte);
      assert hasMore; // Would have thrown otherwise.

      switch (currentEncoding) {
      case DIRECT: readDirectValues(firstByte); break;
      case PATCHED_BASE: readPatchedBaseValues(firstByte); break;
      case DELTA: readDeltaValues(firstByte, null); break;
      case SHORT_REPEAT: readShortRepeatValues(firstByte, null); break;
      default: throw new IOException("Unknown encoding " + currentEncoding);
      }

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

  @Override
  public void nextVector(ColumnVector vector,
                         int[] data,
                         int size) throws IOException {
    if (vector.noNulls) {
      for(int r=0; r < data.length && r < size; ++r) {
        data[r] = (int) next();
      }
    } else if (!(vector.isRepeating && vector.isNull[0])) {
      for(int r=0; r < data.length && r < size; ++r) {
        if (!vector.isNull[r]) {
          data[r] = (int) next();
        } else {
          data[r] = 1;
        }
      }
    }
  }
}
