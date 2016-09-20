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

package org.apache.orc.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Infer and track the evolution between the schema as stored in the file and
 * the schema that has been requested by the reader.
 */
public class SchemaEvolution {
  // indexed by reader column id
  private final TypeDescription[] readerFileTypes;
  // indexed by reader column id
  private final boolean[] readerIncluded;
  // indexed by file column id
  private final boolean[] fileIncluded;
  private final TypeDescription fileSchema;
  private final TypeDescription readerSchema;
  private boolean hasConversion = false;
  private final boolean isAcid;

  // indexed by reader column id
  private final boolean[] ppdSafeConversion;

  private static final Logger LOG =
    LoggerFactory.getLogger(SchemaEvolution.class);
  private static final Pattern missingMetadataPattern =
    Pattern.compile("_col\\d+");

  public static class IllegalEvolutionException extends RuntimeException {
    public IllegalEvolutionException(String msg) {
      super(msg);
    }
  }

  public SchemaEvolution(TypeDescription fileSchema,
                         Reader.Options options) {
    this(fileSchema, null, options);
  }

  public SchemaEvolution(TypeDescription fileSchema,
                         TypeDescription readerSchema,
                         Reader.Options options) {
    boolean allowMissingMetadata = options.getTolerateMissingSchema();
    boolean[] includedCols = options.getInclude();
    this.readerIncluded = includedCols == null ? null :
      Arrays.copyOf(includedCols, includedCols.length);
    this.fileIncluded = new boolean[fileSchema.getMaximumId() + 1];
    this.hasConversion = false;
    this.fileSchema = fileSchema;
    isAcid = checkAcidSchema(fileSchema);
    if (readerSchema != null) {
      if (isAcid) {
        this.readerSchema = createEventSchema(readerSchema);
      } else {
        this.readerSchema = readerSchema;
      }
      this.readerFileTypes =
        new TypeDescription[this.readerSchema.getMaximumId() + 1];
      int positionalLevels = 0;
      if (!hasColumnNames(isAcid? getBaseRow(fileSchema) : fileSchema)){
        if (!this.fileSchema.equals(this.readerSchema)) {
          if (!allowMissingMetadata) {
            throw new RuntimeException("Found that schema metadata is missing"
                + " from file. This is likely caused by"
                + " a writer earlier than HIVE-4243. Will"
                + " not try to reconcile schemas");
          } else {
            LOG.warn("Column names are missing from this file. This is"
                + " caused by a writer earlier than HIVE-4243. The reader will"
                + " reconcile schemas based on index. File type: " +
                this.fileSchema + ", reader type: " + this.readerSchema);
            positionalLevels = isAcid ? 2 : 1;
          }
        }
      }
      buildConversion(fileSchema, this.readerSchema, positionalLevels);
    } else {
      this.readerSchema = fileSchema;
      this.readerFileTypes =
        new TypeDescription[this.readerSchema.getMaximumId() + 1];
      buildIdentityConversion(this.readerSchema);
    }
    this.ppdSafeConversion = populatePpdSafeConversion();
  }

  // Return true iff all fields have names like _col[0-9]+
  private boolean hasColumnNames(TypeDescription fileSchema) {
    if (fileSchema.getCategory() != TypeDescription.Category.STRUCT) {
      return true;
    }
    for (String fieldName : fileSchema.getFieldNames()) {
      if (!missingMetadataPattern.matcher(fieldName).matches()) {
        return true;
      }
    }
    return false;
  }

  public TypeDescription getReaderSchema() {
    return readerSchema;
  }

  /**
   * Returns the non-ACID (aka base) reader type description.
   *
   * @return the reader type ignoring the ACID rowid columns, if any
   */
  public TypeDescription getReaderBaseSchema() {
    return isAcid ? getBaseRow(readerSchema) : readerSchema;
  }

  /**
   * Does the file include ACID columns?
   * @return is this an ACID file?
   */
  boolean isAcid() {
    return isAcid;
  }

  /**
   * Is there Schema Evolution data type conversion?
   * @return
   */
  public boolean hasConversion() {
    return hasConversion;
  }

  public TypeDescription getFileSchema() {
    return fileSchema;
  }

  public TypeDescription getFileType(TypeDescription readerType) {
    return getFileType(readerType.getId());
  }

  /**
   * Get the file type by reader type id.
   * @param id reader column id
   * @return
   */
  public TypeDescription getFileType(int id) {
    return readerFileTypes[id];
  }

  public boolean[] getReaderIncluded() {
    return readerIncluded;
  }

  public boolean[] getFileIncluded() {
    return fileIncluded;
  }

  /**
   * Check if column is safe for ppd evaluation
   * @param colId reader column id
   * @return true if the specified column is safe for ppd evaluation else false
   */
  public boolean isPPDSafeConversion(final int colId) {
    if (hasConversion()) {
      return !(colId < 0 || colId >= ppdSafeConversion.length) &&
          ppdSafeConversion[colId];
    }

    // when there is no schema evolution PPD is safe
    return true;
  }

  private boolean[] populatePpdSafeConversion() {
    if (fileSchema == null || readerSchema == null || readerFileTypes == null) {
      return null;
    }

    boolean[] result = new boolean[readerSchema.getMaximumId() + 1];
    boolean safePpd = validatePPDConversion(fileSchema, readerSchema);
    result[readerSchema.getId()] = safePpd;
    List<TypeDescription> children = readerSchema.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        TypeDescription fileType = getFileType(child.getId());
        safePpd = validatePPDConversion(fileType, child);
        result[child.getId()] = safePpd;
      }
    }
    return result;
  }

  private boolean validatePPDConversion(final TypeDescription fileType,
      final TypeDescription readerType) {
    if (fileType == null) {
      return false;
    }
    if (fileType.getCategory().isPrimitive()) {
      if (fileType.getCategory().equals(readerType.getCategory())) {
        // for decimals alone do equality check to not mess up with precision change
        return !(fileType.getCategory() == TypeDescription.Category.DECIMAL &&
            !fileType.equals(readerType));
      }

      // only integer and string evolutions are safe
      // byte -> short -> int -> long
      // string <-> char <-> varchar
      // NOTE: Float to double evolution is not safe as floats are stored as doubles in ORC's
      // internal index, but when doing predicate evaluation for queries like "select * from
      // orc_float where f = 74.72" the constant on the filter is converted from string -> double
      // so the precisions will be different and the comparison will fail.
      // Soon, we should convert all sargs that compare equality between floats or
      // doubles to range predicates.

      // Similarly string -> char and varchar -> char and vice versa is not possible, as ORC stores
      // char with padded spaces in its internal index.
      switch (fileType.getCategory()) {
        case BYTE:
          if (readerType.getCategory().equals(TypeDescription.Category.SHORT) ||
              readerType.getCategory().equals(TypeDescription.Category.INT) ||
              readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case SHORT:
          if (readerType.getCategory().equals(TypeDescription.Category.INT) ||
              readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case INT:
          if (readerType.getCategory().equals(TypeDescription.Category.LONG)) {
            return true;
          }
          break;
        case STRING:
          if (readerType.getCategory().equals(TypeDescription.Category.VARCHAR)) {
            return true;
          }
          break;
        case VARCHAR:
          if (readerType.getCategory().equals(TypeDescription.Category.STRING)) {
            return true;
          }
          break;
        default:
          break;
      }
    }
    return false;
  }

  /**
   * Build the mapping from the file type to the reader type. For pre-HIVE-4243
   * ORC files, the top level structure is matched using position within the
   * row. Otherwise, structs fields are matched by name.
   * @param fileType the type in the file
   * @param readerType the type in the reader
   * @param positionalLevels the number of structure levels that must be
   *                         mapped by position rather than field name. Pre
   *                         HIVE-4243 files have either 1 or 2 levels matched
   *                         positionally depending on whether they are ACID.
   */
  void buildConversion(TypeDescription fileType,
                       TypeDescription readerType,
                       int positionalLevels) {
    // if the column isn't included, don't map it
    if (readerIncluded != null && !readerIncluded[readerType.getId()]) {
      return;
    }
    boolean isOk = true;
    // check the easy case first
    if (fileType.getCategory() == readerType.getCategory()) {
      switch (readerType.getCategory()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DOUBLE:
        case FLOAT:
        case STRING:
        case TIMESTAMP:
        case BINARY:
        case DATE:
          // these are always a match
          break;
        case CHAR:
        case VARCHAR:
          // We do conversion when same CHAR/VARCHAR type but different
          // maxLength.
          if (fileType.getMaxLength() != readerType.getMaxLength()) {
            hasConversion = true;
          }
          break;
        case DECIMAL:
          // We do conversion when same DECIMAL type but different
          // precision/scale.
          if (fileType.getPrecision() != readerType.getPrecision() ||
              fileType.getScale() != readerType.getScale()) {
            hasConversion = true;
          }
          break;
        case UNION:
        case MAP:
        case LIST: {
          // these must be an exact match
          List<TypeDescription> fileChildren = fileType.getChildren();
          List<TypeDescription> readerChildren = readerType.getChildren();
          if (fileChildren.size() == readerChildren.size()) {
            for(int i=0; i < fileChildren.size(); ++i) {
              buildConversion(fileChildren.get(i),
                              readerChildren.get(i), 0);
            }
          } else {
            isOk = false;
          }
          break;
        }
        case STRUCT: {
          List<TypeDescription> readerChildren = readerType.getChildren();
          List<TypeDescription> fileChildren = fileType.getChildren();
          if (fileChildren.size() != readerChildren.size()) {
            hasConversion = true;
          }

          if (positionalLevels == 0) {
            List<String> readerFieldNames = readerType.getFieldNames();
            List<String> fileFieldNames = fileType.getFieldNames();
            Map<String, TypeDescription> fileTypesIdx = new HashMap<>();
            for (int i = 0; i < fileFieldNames.size(); i++) {
              fileTypesIdx.put(fileFieldNames.get(i), fileChildren.get(i));
            }

            for (int i = 0; i < readerFieldNames.size(); i++) {
              String readerFieldName = readerFieldNames.get(i);
              TypeDescription readerField = readerChildren.get(i);

              TypeDescription fileField = fileTypesIdx.get(readerFieldName);
              if (fileField == null) {
                continue;
              }

              buildConversion(fileField, readerField, 0);
            }
          } else {
            int jointSize = Math.min(fileChildren.size(),
                                     readerChildren.size());
            for (int i = 0; i < jointSize; ++i) {
              buildConversion(fileChildren.get(i), readerChildren.get(i),
                  positionalLevels - 1);
            }
          }
          break;
        }
        default:
          throw new IllegalArgumentException("Unknown type " + readerType);
      }
    } else {
      /*
       * Check for the few cases where will not convert....
       */

      isOk = ConvertTreeReaderFactory.canConvert(fileType, readerType);
      hasConversion = true;
    }
    if (isOk) {
      readerFileTypes[readerType.getId()] = fileType;
      fileIncluded[fileType.getId()] = true;
    } else {
      throw new IllegalEvolutionException(
          String.format("ORC does not support type conversion from file" +
                        " type %s (%d) to reader type %s (%d)",
                        fileType.toString(), fileType.getId(),
                        readerType.toString(), readerType.getId()));
    }
  }

  void buildIdentityConversion(TypeDescription readerType) {
    int id = readerType.getId();
    if (readerIncluded != null && !readerIncluded[id]) {
      return;
    }
    if (readerFileTypes[id] != null) {
      throw new RuntimeException("reader to file type entry already assigned");
    }
    readerFileTypes[id] = readerType;
    fileIncluded[id] = true;
    List<TypeDescription> children = readerType.getChildren();
    if (children != null) {
      for (TypeDescription child : children) {
        buildIdentityConversion(child);
      }
    }
  }

  private static boolean checkAcidSchema(TypeDescription type) {
    if (type.getCategory().equals(TypeDescription.Category.STRUCT)) {
      List<String> rootFields = type.getFieldNames();
      if (acidEventFieldNames.equals(rootFields)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param typeDescr
   * @return ORC types for the ACID event based on the row's type description
   */
  public static TypeDescription createEventSchema(TypeDescription typeDescr) {
    TypeDescription result = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createInt())
        .addField("originalTransaction", TypeDescription.createLong())
        .addField("bucket", TypeDescription.createInt())
        .addField("rowId", TypeDescription.createLong())
        .addField("currentTransaction", TypeDescription.createLong())
        .addField("row", typeDescr.clone());
    return result;
  }

  /**
   * Get the underlying base row from an ACID event struct.
   * @param typeDescription the ACID event schema.
   * @return the subtype for the real row
   */
  static TypeDescription getBaseRow(TypeDescription typeDescription) {
    final int ACID_ROW_OFFSET = 5;
    return typeDescription.getChildren().get(ACID_ROW_OFFSET);
  }

  public static final List<String> acidEventFieldNames=
    new ArrayList<String>();

  static {
    acidEventFieldNames.add("operation");
    acidEventFieldNames.add("originalTransaction");
    acidEventFieldNames.add("bucket");
    acidEventFieldNames.add("rowId");
    acidEventFieldNames.add("currentTransaction");
    acidEventFieldNames.add("row");
  }
}
