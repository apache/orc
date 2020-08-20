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

package org.apache.orc.gen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.orc.gen.FilterGenerator.FilterOp.BETWEEN;
import static org.apache.orc.gen.FilterGenerator.FilterOp.EQUALS;
import static org.apache.orc.gen.FilterGenerator.FilterOp.IN;
import static org.apache.orc.gen.FilterGenerator.FilterOp.NOT;
import static org.apache.orc.gen.FilterGenerator.FilterType.DECIMAL;
import static org.apache.orc.gen.FilterGenerator.FilterType.FLOAT;
import static org.apache.orc.gen.FilterGenerator.FilterType.LONG;
import static org.apache.orc.gen.FilterGenerator.FilterType.STRING;
import static org.apache.orc.gen.FilterGenerator.FilterType.TIMESTAMP;

public class FilterGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(FilterGenerator.class);
  private final Generator gtr = new Generator();
  private File templateDir;
  private File srcPkgDir = null;
  private File testPkgDir = null;

  private static String readFile(File tFile) throws IOException {
    StringBuilder b = new StringBuilder();
    String line;
    try (BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(tFile),
                                                                     StandardCharsets.UTF_8))) {
      while ((line = r.readLine()) != null) {
        b.append(line);
        b.append("\n");
      }
    }
    return b.toString();
  }

  private static void writeFile(String content, File file)
    throws IOException {
    try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file),
                                                                      StandardCharsets.UTF_8))) {
      w.write(content);
    }
  }

  static String toPascalCase(String word) {
    return toCapCase(word, true);
  }

  static String toCamelCase(String word) {
    return toCapCase(word, false);
  }

  static String toCapCase(String word, boolean pascal) {
    if (word == null || word.isEmpty()) {
      return word;
    } else {
      word = word.toLowerCase();
      StringBuilder b = new StringBuilder();
      for (String part : word.split("_")) {
        if (part.isEmpty()) {
          b.append("_");
        } else {
          if (pascal || b.length() > 0) {
            b.append(Character.toUpperCase(part.charAt(0)));
            b.append(part.substring(1));
          } else {
            b.append(part);
          }
        }
      }
      return b.toString();
    }
  }

  public void setSrcPkgDir(String srcPkgDir) {
    this.srcPkgDir = new File(srcPkgDir);
  }

  public void setTestPkgDir(String testPkgDir) {
    this.testPkgDir = new File(testPkgDir);
  }

  public void setTemplateDir(String templateDir) {
    this.templateDir = new File(templateDir);
  }

  public void execute() throws IOException, IllegalAccessException {
    LOG.info("Generating using templates: {} to src dir {} and test dir {}",
             templateDir,
             srcPkgDir,
             testPkgDir);

    if (srcPkgDir.mkdirs()) {
      LOG.info("Created pkg dir {}", srcPkgDir);
    }

    for (boolean not : new boolean[] {false, true}) {
      writeSimpleOpFilters(not);
      writeStringOpFilters(not);
      writeDecimalOpFilters(not);
      writeTimestampOpFilters(not);
    }
    writeLeafFilterFactory();
    writeFilterTests();
  }

  private void writeFilterTests() throws IOException {
    StringBuilder buffer = new StringBuilder();
    String unaryTest = readFile(new File(templateDir, "filter_unary_test.txt"));
    String noArgTest = readFile(new File(templateDir, "filter_noarg_test.txt"));
    String binaryTest = readFile(new File(templateDir, "filter_binary_test.txt"));
    int colIdx = 0;
    for (FilterType fType : Arrays.asList(LONG, STRING, DECIMAL, FLOAT, TIMESTAMP)) {
      buffer.setLength(0);
      colIdx++;
      for (FilterOp fOp : FilterOp.values()) {
        if (fOp == NOT) {
          continue;
        }
        gtr.clear();
        gtr.filterType(fType);
        gtr.filterOp(fOp);
        gtr.addTestOps(colIdx);
        switch (fOp) {
          case IS_NULL:
            buffer.append(gtr.build(noArgTest));
            break;
          case BETWEEN:
          case IN:
            buffer.append(gtr.build(binaryTest));
            break;
          default:
            buffer.append(gtr.build(unaryTest));
            break;
        }
      }
      // Generate the class
      gtr.clear();
      String className = "Test" + fType.getPC() + "Filters";
      gtr.addTag(Tag.METHODS, buffer.toString());
      gtr.addTag(Tag.CLASS_NAME, className);
      writeFile(gtr.build(new File(templateDir, "filter_test.txt"), false),
                new File(testPkgDir, className + ".java"));
    }
  }

  private void writeTimestampOpFilters(boolean not) throws IOException {
    for (FilterOp fOp : FilterOp.values()) {
      if (!fOp.comparison) {
        continue;
      }

      gtr.clear();
      gtr.filterType(TIMESTAMP)
        .filterOp(fOp)
        .not(not)
        .addCmpOp();
      writeFile(gtr.build(new File(templateDir, "timestamp_cmp.txt")),
                new File(srcPkgDir, gtr.fileName()));
    }

    // Write between filter
    gtr.clear();
    gtr.filterType(TIMESTAMP)
      .filterOp(BETWEEN)
      .not(not)
      .addBtOp();
    writeFile(gtr.build(new File(templateDir, "timestamp_bt.txt")),
              new File(srcPkgDir, gtr.fileName()));

    // Write in filter
    gtr.clear();
    gtr.filterType(TIMESTAMP)
      .filterOp(IN)
      .not(not)
      .addNotOp();
    writeFile(gtr.build(new File(templateDir, "timestamp_in.txt")),
              new File(srcPkgDir, gtr.fileName()));
  }

  private void writeLeafFilterFactory() throws IOException {
    StringBuilder buffer = new StringBuilder();
    String methodTemplate = readFile(new File(templateDir, "create_leaf.txt"));

    for (boolean not : Arrays.asList(false, true)) {
      // Generate the methods
      for (FilterOp fOp : FilterOp.values()) {
        if (!fOp.comparison) {
          continue;
        }

        gtr.clear();
        gtr.addTag(Tag.OPERATOR, fOp.getPC())
          .addTag(Tag.NOT, not ? NOT.getPC() : "");
        buffer.append(gtr.build(methodTemplate));
      }

      // Add the Between method
      gtr.clear();
      gtr.addTag(Tag.NOT, not ? NOT.getPC() : "");
      buffer.append(gtr.build(new File(templateDir, "create_bt.txt")));

      // Add the In method
      gtr.clear();
      gtr.addTag(Tag.NOT, not ? NOT.getPC() : "");
      buffer.append(gtr.build(new File(templateDir, "create_in.txt")));
    }


    // Generate the class
    gtr.clear();
    gtr.addTag(Tag.METHODS, buffer.toString());
    writeFile(gtr.build(new File(templateDir, "leaf_factory.txt")),
              new File(srcPkgDir, "LeafFilterFactory.java"));
  }

  private void writeDecimalOpFilters(boolean not) throws IOException {
    for (FilterOp fOp : FilterOp.values()) {
      if (!fOp.comparison) {
        continue;
      }

      gtr.clear();
      gtr.filterType(DECIMAL)
        .filterOp(fOp)
        .not(not)
        .addCmpOp();
      writeFile(gtr.build(new File(templateDir, "decimal_cmp.txt")),
                new File(srcPkgDir, gtr.fileName()));
    }

    // Write Between
    gtr.clear();
    gtr.filterType(DECIMAL)
      .filterOp(BETWEEN)
      .not(not)
      .addBtOp();
    writeFile(gtr.build(new File(templateDir, "decimal_bt.txt")),
              new File(srcPkgDir, gtr.fileName()));

    // Write In
    gtr.clear();
    gtr.filterType(DECIMAL)
      .filterOp(IN)
      .not(not)
      .addNotOp();
    writeFile(gtr.build(new File(templateDir, "decimal_in.txt")),
              new File(srcPkgDir, gtr.fileName()));
  }

  private void writeStringOpFilters(boolean not) throws IOException {
    // Write equals
    gtr.clear();
    gtr.filterType(STRING)
      .filterOp(EQUALS)
      .not(not)
      .addNotOp();
    writeFile(gtr.build(new File(templateDir, "string_eq.txt")),
              new File(srcPkgDir, gtr.fileName()));

    // Write compare operators
    for (FilterOp fOp : Arrays.asList(FilterOp.LESS_THAN, FilterOp.LESS_THAN_EQUALS)) {
      gtr.clear();
      gtr.filterType(STRING)
        .filterOp(fOp)
        .not(not)
        .addCmpOp();
      writeFile(gtr.build(new File(templateDir, "string_cmp.txt")),
                new File(srcPkgDir, gtr.fileName()));
    }

    // Write between
    gtr.clear();
    gtr.filterType(STRING)
      .filterOp(BETWEEN)
      .not(not)
      .addBtOp();
    writeFile(gtr.build(new File(templateDir, "string_bt.txt")),
              new File(srcPkgDir, gtr.fileName()));

    // Write string in
    gtr.clear();
    gtr.filterType(STRING)
      .filterOp(IN)
      .not(not)
      .addNotOp();
    writeFile(gtr.build(new File(templateDir, "string_in.txt")),
              new File(srcPkgDir, gtr.fileName()));
  }

  private void writeSimpleOpFilters(boolean not) throws IOException, IllegalAccessException {
    for (FilterType fType : FilterType.values()) {
      if (!fType.simple) {
        continue;
      }

      // Write comparison filters
      for (FilterOp fOp : FilterOp.values()) {
        if (!fOp.comparison) {
          continue;
        }
        writeSimpleOpFilter(fType, fOp, not);
      }

      // Write between filters
      gtr.clear();
      gtr.filterType(fType)
        .filterOp(BETWEEN)
        .not(not)
        .addBtOp()
        .addTypeParams();
      writeFile(gtr.build(new File(templateDir, "type_bt.txt")),
                new File(srcPkgDir, gtr.fileName()));

      // Write in filters
      gtr.clear();
      gtr.filterType(fType)
        .filterOp(IN)
        .not(not)
        .addInOp()
        .addTypeParams();
      writeFile(gtr.build(new File(templateDir, "type_in.txt")),
                new File(srcPkgDir, gtr.fileName()));
    }
  }

  private void writeSimpleOpFilter(FilterType fType, FilterOp fOp, boolean not)
    throws IOException, IllegalAccessException {
    String content = genSimpleOpFilter(fType, fOp, not);
    writeFile(content, new File(srcPkgDir, gtr.fileName()));
  }

  String genSimpleOpFilter(FilterType fType, FilterOp fOp, boolean not)
    throws IOException, IllegalAccessException {
    if (!fType.simple || !fOp.comparison) {
      throw new IllegalAccessException(String.format(
        "FilterType %s, FilterOp %s does now allow simple generation", fType, fOp));
    }
    gtr.clear();
    gtr.filterType(fType)
      .filterOp(fOp)
      .not(not)
      .addTypeParams()
      .addCmpOp();

    return gtr.build(new File(templateDir, "type_cmp.txt"));
  }

  private enum Tag {
    OPERATOR,
    CLASS_NAME,
    LEAF_TYPE,
    LEAF_VECTOR,
    METHODS,
    LOW_OP,
    HIGH_OP,
    LOG_OP,
    IN_OP,
    NOT_OP,
    NOT,
    S_OPERATOR,
    TYPE_NAME, OPERATOR_NAME, COL_NAME;

    private String getCC() {
      return String.format("<%s>", toPascalCase(name()));
    }
  }

  enum FilterType {
    LONG(true, "long", "LongColumnVector"),
    FLOAT(true, "double", "DoubleColumnVector"),
    TIMESTAMP(false, null, null),
    DECIMAL(false, null, null),
    STRING(false, null, null);

    final boolean simple;
    final String leafType;
    final String leafVector;

    FilterType(boolean simple, String leafType, String leafVector) {
      this.simple = simple;
      this.leafType = leafType;
      this.leafVector = leafVector;
    }

    FilterType(boolean simple) {
      this(simple, null, null);
    }

    String getPC() {
      return toPascalCase(name());
    }
  }

  enum FilterOp {
    EQUALS(true, "==", "!="),
    LESS_THAN(true, "<", ">="),
    LESS_THAN_EQUALS(true, "<=", ">"),
    BETWEEN(),
    IN(false, ">=", "<"),
    IS_NULL(false, null, null),
    NOT(false, "", "!");

    final boolean comparison;
    final String op;
    final String notOp;

    FilterOp(boolean comparison, String op, String notOp) {
      this.comparison = comparison;
      this.op = op;
      this.notOp = notOp;
    }

    FilterOp() {
      this(false, null, null);
    }

    String getPC() {
      return toPascalCase(name());
    }

    String getCC() {
      return toCamelCase(name());
    }
  }

  private static class Generator {
    private final Map<Tag, String> subs = new HashMap<>(10);
    private FilterOp fOp;
    private FilterType fType;
    private boolean not;

    void clear() {
      subs.clear();
    }

    Generator filterOp(FilterOp fOp) {
      this.fOp = fOp;
      return this;
    }

    Generator filterType(FilterType fType) {
      this.fType = fType;
      return this;
    }

    Generator not(boolean not) {
      this.not = not;
      return this;
    }

    private void addClassName() {
      subs.put(Tag.CLASS_NAME, makeClassName(fOp, fType, not));
    }

    private String makeClassName(FilterOp fOp, FilterType fType, boolean not) {
      return fType.getPC() + (not ? NOT.getPC() + fOp.getPC() : fOp.getPC());
    }

    private Generator addTypeParams() {
      subs.put(Tag.LEAF_VECTOR, fType.leafVector);
      subs.put(Tag.LEAF_TYPE, fType.leafType);
      return this;
    }

    Generator addTag(Tag tag, String rpl) {
      subs.put(tag, rpl);
      return this;
    }

    Generator addCmpOp() {
      subs.put(Tag.OPERATOR, not ? fOp.notOp : fOp.op);
      return this;
    }

    Generator addTestOps(int colIdx) {
      subs.put(Tag.OPERATOR, fOp.getPC());
      subs.put(Tag.S_OPERATOR, fOp.getCC());
      subs.put(Tag.TYPE_NAME, fType.name());
      subs.put(Tag.OPERATOR_NAME, fOp.name());
      subs.put(Tag.COL_NAME, "f" + colIdx);
      return this;
    }

    Generator addBtOp() {
      String logOp;
      String lowOp;
      String highOp;

      if (not) {
        logOp = "||";
        lowOp = "<";
        highOp = ">";
      } else {
        logOp = "&&";
        lowOp = ">=";
        highOp = "<=";
      }

      subs.put(Tag.LOG_OP, logOp);
      subs.put(Tag.LOW_OP, lowOp);
      subs.put(Tag.HIGH_OP, highOp);
      return this;
    }

    String build(String content) {
      for (Map.Entry<Tag, String> entry : subs.entrySet()) {
        content = content.replaceAll(entry.getKey().getCC(), entry.getValue());
      }
      return content;
    }

    String build(File tFile, boolean addClassName) throws IOException {
      if (addClassName) {
        addClassName();
      }
      return build(readFile(tFile));
    }

    String build(File tFile) throws IOException {
      return build(tFile, true);
    }

    String fileName() {
      return subs.get(Tag.CLASS_NAME) + ".java";
    }

    public Generator addInOp() {
      subs.put(Tag.IN_OP, not ? IN.notOp : IN.op);
      return this;
    }

    public Generator addNotOp() {
      subs.put(Tag.NOT_OP, not ? NOT.notOp : NOT.op);
      return this;
    }
  }
}
