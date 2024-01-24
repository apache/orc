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

package org.apache.orc.util;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DFSInputStream;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class OrcInputStreamUtil {

  private static final String DFS_CLASS = "org.apache.hadoop.hdfs.DFSInputStream";

  private static Method shortCircuitForbiddenMethod;

  static {
    init();
  }

  private static void init() {
    try {
      initInt();
    } catch (ClassNotFoundException | NoSuchMethodException ignored) {
    }
  }

  private static void initInt() throws ClassNotFoundException, NoSuchMethodException {
    Class<?> dfsClass = Class.forName(DFS_CLASS);
    // org.apache.hadoop.hdfs.DFSInputStream.shortCircuitForbidden Method is not public
    shortCircuitForbiddenMethod = dfsClass.getDeclaredMethod("shortCircuitForbidden");
    shortCircuitForbiddenMethod.setAccessible(true);
  }

  public static long getFileLength(FSDataInputStream file) {
    try {
      return getFileLengthInt(file);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      return -1L;
    }
  }

  private static long getFileLengthInt(FSDataInputStream file)
          throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    if (shortCircuitForbiddenMethod == null) {
      return -1L;
    }
    InputStream wrappedStream = file.getWrappedStream();
    if (wrappedStream instanceof DFSInputStream dfsInputStream) {
      boolean isUnderConstruction = (boolean) shortCircuitForbiddenMethod.invoke(wrappedStream);
      // If file are under construction, we need to get the file length from NameNode.
      if (!isUnderConstruction) {
        return dfsInputStream.getFileLength();
      }
    }
    return -1L;
  }
}
