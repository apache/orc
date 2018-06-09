/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.bench.core;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * A class to track the number of rows, bytes, and read operations that have
 * been read.
 */
@AuxCounters(AuxCounters.Type.EVENTS)
@State(Scope.Thread)
public class ReadCounters {
  long bytesRead;
  long reads;
  RecordCounters recordCounters;

  @Setup(Level.Iteration)
  public void setup(RecordCounters records) {
    bytesRead = 0;
    reads = 0;
    recordCounters = records;
  }

  @TearDown(Level.Iteration)
  public void print() {
    if (recordCounters != null) {
      recordCounters.print();
    }
    System.out.println("Reads: " + reads);
    System.out.println("Bytes: " + bytesRead);
  }

  public double bytesPerRecord() {
    return recordCounters == null || recordCounters.records == 0 ?
        0 : ((double) bytesRead) / recordCounters.records;
  }

  public long records() {
    return recordCounters == null || recordCounters.invocations == 0 ?
        0 : recordCounters.records / recordCounters.invocations;
  }

  public long reads() {
    return recordCounters == null || recordCounters.invocations == 0 ?
        0 : reads / recordCounters.invocations;
  }

  public void addRecords(long value) {
    if (recordCounters != null) {
      recordCounters.records += value;
    }
  }

  public void addInvocation() {
    if (recordCounters != null) {
      recordCounters.invocations += 1;
    }
  }

  public void addBytes(long newReads, long newBytes) {
    bytesRead += newBytes;
    reads += newReads;
  }
}
