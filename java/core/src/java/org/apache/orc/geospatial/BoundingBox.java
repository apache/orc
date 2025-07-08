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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * Bounding box for Geometry or Geography type in the representation of min/max
 * value pairs of coordinates from each axis.
 * A bounding box is considered valid if none of the X / Y dimensions contain NaN.
 */
public class BoundingBox {

  private double xMin = Double.POSITIVE_INFINITY;
  private double xMax = Double.NEGATIVE_INFINITY;
  private double yMin = Double.POSITIVE_INFINITY;
  private double yMax = Double.NEGATIVE_INFINITY;
  private double zMin = Double.POSITIVE_INFINITY;
  private double zMax = Double.NEGATIVE_INFINITY;
  private double mMin = Double.POSITIVE_INFINITY;
  private double mMax = Double.NEGATIVE_INFINITY;
  private boolean valid = true;

  public BoundingBox() {
  }

  public BoundingBox(
          double xMin, double xMax, double yMin, double yMax,
          double zMin, double zMax, double mMin, double mMax) {
    this.xMin = xMin;
    this.xMax = xMax;
    this.yMin = yMin;
    this.yMax = yMax;
    this.zMin = zMin;
    this.zMax = zMax;
    this.mMin = mMin;
    this.mMax = mMax;

    // Update the validity
    valid = isXYValid();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof BoundingBox other)) {
      return false;
    }
    if (obj == this) {
      return true;
    }

    // Valid flag must be checked since invalid bounding boxes may have equal coordinates with the initial one
    return xMin == other.xMin && xMax == other.xMax && yMin == other.yMin && yMax == other.yMax &&
           zMin == other.zMin && zMax == other.zMax && mMin == other.mMin && mMax == other.mMax &&
           valid == other.valid;
  }

  @Override
  public int hashCode() {
    return Double.hashCode(xMin) ^ Double.hashCode(xMax) ^
           Double.hashCode(yMin) ^ Double.hashCode(yMax) ^
           Double.hashCode(zMin) ^ Double.hashCode(zMax) ^
           Double.hashCode(mMin) ^ Double.hashCode(mMax) ^
           Boolean.hashCode(valid);
  }

  // Don't change `valid` here and let the caller maintain it
  private void resetBBox() {
    xMin = Double.POSITIVE_INFINITY;
    xMax = Double.NEGATIVE_INFINITY;
    yMin = Double.POSITIVE_INFINITY;
    yMax = Double.NEGATIVE_INFINITY;
    zMin = Double.POSITIVE_INFINITY;
    zMax = Double.NEGATIVE_INFINITY;
    mMin = Double.POSITIVE_INFINITY;
    mMax = Double.NEGATIVE_INFINITY;
  }

  public double getXMin() {
    return xMin;
  }

  public double getXMax() {
    return xMax;
  }

  public double getYMin() {
    return yMin;
  }

  public double getYMax() {
    return yMax;
  }

  public double getZMin() {
    return zMin;
  }

  public double getZMax() {
    return zMax;
  }

  public double getMMin() {
    return mMin;
  }

  public double getMMax() {
    return mMax;
  }

  /**
   * Checks if the bounding box is valid.
   * A bounding box is considered valid if none of the X / Y dimensions contain NaN.
   *
   * @return true if the bounding box is valid, false otherwise.
   */
  public boolean isValid() {
    return valid;
  }

  /**
   * Checks if the X and Y dimensions of the bounding box are valid.
   * The X and Y dimensions are considered valid if none of the bounds contain NaN.
   *
   * @return true if the X and Y dimensions are valid, false otherwise.
   */
  public boolean isXYValid() {
    return isXValid() && isYValid();
  }

  /**
   * Checks if the X dimension of the bounding box is valid.
   * The X dimension is considered valid if neither bound contains NaN.
   *
   * @return true if the X dimension is valid, false otherwise.
   */
  public boolean isXValid() {
    return !(Double.isNaN(xMin) || Double.isNaN(xMax));
  }

  /**
   * Checks if the Y dimension of the bounding box is valid.
   * The Y dimension is considered valid if neither bound contains NaN.
   *
   * @return true if the Y dimension is valid, false otherwise.
   */
  public boolean isYValid() {
    return !(Double.isNaN(yMin) || Double.isNaN(yMax));
  }

  /**
   * Checks if the Z dimension of the bounding box is valid.
   * The Z dimension is considered valid if none of the bounds contain NaN.
   *
   * @return true if the Z dimension is valid, false otherwise.
   */
  public boolean isZValid() {
    return !(Double.isNaN(zMin) || Double.isNaN(zMax));
  }

  /**
   * Checks if the M dimension of the bounding box is valid.
   * The M dimension is considered valid if none of the bounds contain NaN.
   *
   * @return true if the M dimension is valid, false otherwise.
   */
  public boolean isMValid() {
    return !(Double.isNaN(mMin) || Double.isNaN(mMax));
  }

  /**
   * Checks if the bounding box is empty in the X / Y dimension.
   *
   * @return true if the bounding box is empty, false otherwise.
   */
  public boolean isXYEmpty() {
    return isXEmpty() || isYEmpty();
  }

  /**
   * Checks if the bounding box is empty in the X dimension.
   *
   * @return true if the X dimension is empty, false otherwise.
   */
  public boolean isXEmpty() {
    return xMin > xMax;
  }

  /**
   * Checks if the bounding box is empty in the Y dimension.
   *
   * @return true if the Y dimension is empty, false otherwise.
   */
  public boolean isYEmpty() {
    return yMin > yMax;
  }

  /**
   * Checks if the bounding box is empty in the Z dimension.
   *
   * @return true if the Z dimension is empty, false otherwise.
   */
  public boolean isZEmpty() {
    return zMin > zMax;
  }

  /**
   * Checks if the bounding box is empty in the M dimension.
   *
   * @return true if the M dimension is empty, false otherwise.
   */
  public boolean isMEmpty() {
    return mMin > mMax;
  }

  /**
   * Expands this bounding box to include the bounds of another box.
   * After merging, this bounding box will contain both its original extent
   * and the extent of the other bounding box.
   *
   * @param other the other BoundingBox whose bounds will be merged into this one
   */
  public void merge(BoundingBox other) {
    if (!valid) {
      return;
    }

    // If other is null or invalid, mark this as invalid
    if (other == null || !other.valid) {
      valid = false;
      resetBBox();
      return;
    }

    this.xMin = Math.min(this.xMin, other.xMin);
    this.xMax = Math.max(this.xMax, other.xMax);
    this.yMin = Math.min(this.yMin, other.yMin);
    this.yMax = Math.max(this.yMax, other.yMax);
    this.zMin = Math.min(this.zMin, other.zMin);
    this.zMax = Math.max(this.zMax, other.zMax);
    this.mMin = Math.min(this.mMin, other.mMin);
    this.mMax = Math.max(this.mMax, other.mMax);

    // Update the validity of this bounding box based on the other bounding box
    valid = isXYValid();
  }

  /**
   * Extends this bounding box to include the spatial extent of the provided geometry.
   * The bounding box coordinates (min/max values for x, y, z, m) will be adjusted
   * to encompass both the current bounds and the geometry's bounds.
   *
   * @param geometry The geometry whose coordinates will be used to update this bounding box.
   *                 If null or empty, the method returns without making any changes.
   */
  public void update(Geometry geometry) {
    if (!valid) {
      return;
    }

    if (geometry == null || geometry.isEmpty()) {
      return;
    }

    // Updates the X and Y bounds of this bounding box with the given coordinates.
    // Updates are conditional:
    // - X bounds are only updated if both minX and maxX are not NaN
    // - Y bounds are only updated if both minY and maxY are not NaN
    // This allows partial updates while preserving valid dimensions.
    Envelope envelope = geometry.getEnvelopeInternal();
    if (!Double.isNaN(envelope.getMinX()) && !Double.isNaN(envelope.getMaxX())) {
      xMin = Math.min(xMin, envelope.getMinX());
      xMax = Math.max(xMax, envelope.getMaxX());
    }
    if (!Double.isNaN(envelope.getMinY()) && !Double.isNaN(envelope.getMaxY())) {
      yMin = Math.min(yMin, envelope.getMinY());
      yMax = Math.max(yMax, envelope.getMaxY());
    }

    for (Coordinate coord : geometry.getCoordinates()) {
      if (!Double.isNaN(coord.getZ())) {
        zMin = Math.min(zMin, coord.getZ());
        zMax = Math.max(zMax, coord.getZ());
      }
      if (!Double.isNaN(coord.getM())) {
        mMin = Math.min(mMin, coord.getM());
        mMax = Math.max(mMax, coord.getM());
      }
    }

    // Update the validity of this bounding box based on the other bounding box
    valid = isXYValid();
  }

  /**
   * Resets the bounding box to its initial state.
   */
  public void reset() {
    resetBBox();
    valid = true;
  }

  /**
   * Creates a copy of the current bounding box.
   *
   * @return a new BoundingBox instance with the same values as this one.
   */
  public BoundingBox copy() {
    return new BoundingBox(
            this.xMin, this.xMax,
            this.yMin, this.yMax,
            this.zMin, this.zMax,
            this.mMin, this.mMax);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("BoundingBox{xMin=")
            .append(xMin)
            .append(", xMax=")
            .append(xMax)
            .append(", yMin=")
            .append(yMin)
            .append(", yMax=")
            .append(yMax)
            .append(", zMin=")
            .append(zMin)
            .append(", zMax=")
            .append(zMax)
            .append(", mMin=")
            .append(mMin)
            .append(", mMax=")
            .append(mMax);

    // Only include the valid flag when it's false
    if (!valid) {
      sb.append(", valid=false");
    }

    sb.append('}');
    return sb.toString();
  }
}
