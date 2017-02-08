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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.TimeZone;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.orc.BinaryColumnStatistics;
import org.apache.orc.BooleanColumnStatistics;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;

public class ColumnStatisticsImpl implements ColumnStatistics {

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnStatisticsImpl)) {
      return false;
    }

    ColumnStatisticsImpl that = (ColumnStatisticsImpl) o;

    if (count != that.count) {
      return false;
    }
    if (hasNull != that.hasNull) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (count ^ (count >>> 32));
    result = 31 * result + (hasNull ? 1 : 0);
    return result;
  }

  private static final class BooleanStatisticsImpl extends ColumnStatisticsImpl
      implements BooleanColumnStatistics {
    private long trueCount = 0;

    BooleanStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BucketStatistics bkt = stats.getBucketStatistics();
      trueCount = bkt.getCount(0);
    }

    BooleanStatisticsImpl() {
    }

    @Override
    public void reset() {
      super.reset();
      trueCount = 0;
    }

    @Override
    public void updateBoolean(boolean value, int repetitions) {
      if (value) {
        trueCount += repetitions;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof BooleanStatisticsImpl) {
        BooleanStatisticsImpl bkt = (BooleanStatisticsImpl) other;
        trueCount += bkt.trueCount;
      } else {
        if (isStatsExists() && trueCount != 0) {
          throw new IllegalArgumentException("Incompatible merging of boolean column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.BucketStatistics.Builder bucket =
        OrcProto.BucketStatistics.newBuilder();
      bucket.addCount(trueCount);
      builder.setBucketStatistics(bucket);
      return builder;
    }

    @Override
    public long getFalseCount() {
      return getNumberOfValues() - trueCount;
    }

    @Override
    public long getTrueCount() {
      return trueCount;
    }

    @Override
    public String toString() {
      return super.toString() + " true: " + trueCount;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BooleanStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      BooleanStatisticsImpl that = (BooleanStatisticsImpl) o;

      if (trueCount != that.trueCount) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (trueCount ^ (trueCount >>> 32));
      return result;
    }
  }

  private static final class IntegerStatisticsImpl extends ColumnStatisticsImpl
      implements IntegerColumnStatistics {

    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;
    private long sum = 0;
    private boolean hasMinimum = false;
    private boolean overflow = false;

    IntegerStatisticsImpl() {
    }

    IntegerStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.IntegerStatistics intStat = stats.getIntStatistics();
      if (intStat.hasMinimum()) {
        hasMinimum = true;
        minimum = intStat.getMinimum();
      }
      if (intStat.hasMaximum()) {
        maximum = intStat.getMaximum();
      }
      if (intStat.hasSum()) {
        sum = intStat.getSum();
      } else {
        overflow = true;
      }
    }

    @Override
    public void reset() {
      super.reset();
      hasMinimum = false;
      minimum = Long.MAX_VALUE;
      maximum = Long.MIN_VALUE;
      sum = 0;
      overflow = false;
    }

    @Override
    public void updateInteger(long value, int repetitions) {
      if (!hasMinimum) {
        hasMinimum = true;
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      if (!overflow) {
        boolean wasPositive = sum >= 0;
        sum += value * repetitions;
        if ((value >= 0) == wasPositive) {
          overflow = (sum >= 0) != wasPositive;
        }
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof IntegerStatisticsImpl) {
        IntegerStatisticsImpl otherInt = (IntegerStatisticsImpl) other;
        if (!hasMinimum) {
          hasMinimum = otherInt.hasMinimum;
          minimum = otherInt.minimum;
          maximum = otherInt.maximum;
        } else if (otherInt.hasMinimum) {
          if (otherInt.minimum < minimum) {
            minimum = otherInt.minimum;
          }
          if (otherInt.maximum > maximum) {
            maximum = otherInt.maximum;
          }
        }

        overflow |= otherInt.overflow;
        if (!overflow) {
          boolean wasPositive = sum >= 0;
          sum += otherInt.sum;
          if ((otherInt.sum >= 0) == wasPositive) {
            overflow = (sum >= 0) != wasPositive;
          }
        }
      } else {
        if (isStatsExists() && hasMinimum) {
          throw new IllegalArgumentException("Incompatible merging of integer column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.IntegerStatistics.Builder intb =
        OrcProto.IntegerStatistics.newBuilder();
      if (hasMinimum) {
        intb.setMinimum(minimum);
        intb.setMaximum(maximum);
      }
      if (!overflow) {
        intb.setSum(sum);
      }
      builder.setIntStatistics(intb);
      return builder;
    }

    @Override
    public long getMinimum() {
      return minimum;
    }

    @Override
    public long getMaximum() {
      return maximum;
    }

    @Override
    public boolean isSumDefined() {
      return !overflow;
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      if (!overflow) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IntegerStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      IntegerStatisticsImpl that = (IntegerStatisticsImpl) o;

      if (minimum != that.minimum) {
        return false;
      }
      if (maximum != that.maximum) {
        return false;
      }
      if (sum != that.sum) {
        return false;
      }
      if (hasMinimum != that.hasMinimum) {
        return false;
      }
      if (overflow != that.overflow) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (minimum ^ (minimum >>> 32));
      result = 31 * result + (int) (maximum ^ (maximum >>> 32));
      result = 31 * result + (int) (sum ^ (sum >>> 32));
      result = 31 * result + (hasMinimum ? 1 : 0);
      result = 31 * result + (overflow ? 1 : 0);
      return result;
    }
  }

  private static final class DoubleStatisticsImpl extends ColumnStatisticsImpl
       implements DoubleColumnStatistics {
    private boolean hasMinimum = false;
    private double minimum = Double.MAX_VALUE;
    private double maximum = Double.MIN_VALUE;
    private double sum = 0;

    DoubleStatisticsImpl() {
    }

    DoubleStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DoubleStatistics dbl = stats.getDoubleStatistics();
      if (dbl.hasMinimum()) {
        hasMinimum = true;
        minimum = dbl.getMinimum();
      }
      if (dbl.hasMaximum()) {
        maximum = dbl.getMaximum();
      }
      if (dbl.hasSum()) {
        sum = dbl.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      hasMinimum = false;
      minimum = Double.MAX_VALUE;
      maximum = Double.MIN_VALUE;
      sum = 0;
    }

    @Override
    public void updateDouble(double value) {
      if (!hasMinimum) {
        hasMinimum = true;
        minimum = value;
        maximum = value;
      } else if (value < minimum) {
        minimum = value;
      } else if (value > maximum) {
        maximum = value;
      }
      sum += value;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DoubleStatisticsImpl) {
        DoubleStatisticsImpl dbl = (DoubleStatisticsImpl) other;
        if (!hasMinimum) {
          hasMinimum = dbl.hasMinimum;
          minimum = dbl.minimum;
          maximum = dbl.maximum;
        } else if (dbl.hasMinimum) {
          if (dbl.minimum < minimum) {
            minimum = dbl.minimum;
          }
          if (dbl.maximum > maximum) {
            maximum = dbl.maximum;
          }
        }
        sum += dbl.sum;
      } else {
        if (isStatsExists() && hasMinimum) {
          throw new IllegalArgumentException("Incompatible merging of double column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder builder = super.serialize();
      OrcProto.DoubleStatistics.Builder dbl =
        OrcProto.DoubleStatistics.newBuilder();
      if (hasMinimum) {
        dbl.setMinimum(minimum);
        dbl.setMaximum(maximum);
      }
      dbl.setSum(sum);
      builder.setDoubleStatistics(dbl);
      return builder;
    }

    @Override
    public double getMinimum() {
      return minimum;
    }

    @Override
    public double getMaximum() {
      return maximum;
    }

    @Override
    public double getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (hasMinimum) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
      }
      buf.append(" sum: ");
      buf.append(sum);
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DoubleStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DoubleStatisticsImpl that = (DoubleStatisticsImpl) o;

      if (hasMinimum != that.hasMinimum) {
        return false;
      }
      if (Double.compare(that.minimum, minimum) != 0) {
        return false;
      }
      if (Double.compare(that.maximum, maximum) != 0) {
        return false;
      }
      if (Double.compare(that.sum, sum) != 0) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      long temp;
      result = 31 * result + (hasMinimum ? 1 : 0);
      temp = Double.doubleToLongBits(minimum);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(maximum);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(sum);
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      return result;
    }
  }

  protected static final class StringStatisticsImpl extends ColumnStatisticsImpl
      implements StringColumnStatistics {
    private Text minimum = null;
    private Text maximum = null;
    private long sum = 0;

    StringStatisticsImpl() {
    }

    StringStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.StringStatistics str = stats.getStringStatistics();
      if (str.hasMaximum()) {
        maximum = new Text(str.getMaximum());
      }
      if (str.hasMinimum()) {
        minimum = new Text(str.getMinimum());
      }
      if(str.hasSum()) {
        sum = str.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
      sum = 0;
    }

    @Override
    public void updateString(Text value) {
      if (minimum == null) {
        maximum = minimum = new Text(value);
      } else if (minimum.compareTo(value) > 0) {
        minimum = new Text(value);
      } else if (maximum.compareTo(value) < 0) {
        maximum = new Text(value);
      }
      sum += value.getLength();
    }

    @Override
    public void updateString(byte[] bytes, int offset, int length,
                             int repetitions) {
      if (minimum == null) {
        maximum = minimum = new Text();
        maximum.set(bytes, offset, length);
      } else if (WritableComparator.compareBytes(minimum.getBytes(), 0,
          minimum.getLength(), bytes, offset, length) > 0) {
        minimum = new Text();
        minimum.set(bytes, offset, length);
      } else if (WritableComparator.compareBytes(maximum.getBytes(), 0,
          maximum.getLength(), bytes, offset, length) < 0) {
        maximum = new Text();
        maximum.set(bytes, offset, length);
      }
      sum += (long)length * repetitions;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof StringStatisticsImpl) {
        StringStatisticsImpl str = (StringStatisticsImpl) other;
        if (minimum == null) {
          if (str.minimum != null) {
            maximum = new Text(str.getMaximum());
            minimum = new Text(str.getMinimum());
          } else {
          /* both are empty */
            maximum = minimum = null;
          }
        } else if (str.minimum != null) {
          if (minimum.compareTo(str.minimum) > 0) {
            minimum = new Text(str.getMinimum());
          }
          if (maximum.compareTo(str.maximum) < 0) {
            maximum = new Text(str.getMaximum());
          }
        }
        sum += str.sum;
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of string column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.StringStatistics.Builder str =
        OrcProto.StringStatistics.newBuilder();
      if (getNumberOfValues() != 0) {
        str.setMinimum(getMinimum());
        str.setMaximum(getMaximum());
        str.setSum(sum);
      }
      result.setStringStatistics(str);
      return result;
    }

    @Override
    public String getMinimum() {
      return minimum == null ? null : minimum.toString();
    }

    @Override
    public String getMaximum() {
      return maximum == null ? null : maximum.toString();
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StringStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      StringStatisticsImpl that = (StringStatisticsImpl) o;

      if (sum != that.sum) {
        return false;
      }
      if (minimum != null ? !minimum.equals(that.minimum) : that.minimum != null) {
        return false;
      }
      if (maximum != null ? !maximum.equals(that.maximum) : that.maximum != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
      result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
      result = 31 * result + (int) (sum ^ (sum >>> 32));
      return result;
    }
  }

  protected static final class BinaryStatisticsImpl extends ColumnStatisticsImpl implements
      BinaryColumnStatistics {

    private long sum = 0;

    BinaryStatisticsImpl() {
    }

    BinaryStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.BinaryStatistics binStats = stats.getBinaryStatistics();
      if (binStats.hasSum()) {
        sum = binStats.getSum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      sum = 0;
    }

    @Override
    public void updateBinary(BytesWritable value) {
      sum += value.getLength();
    }

    @Override
    public void updateBinary(byte[] bytes, int offset, int length,
                             int repetitions) {
      sum += (long)length * repetitions;
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof BinaryColumnStatistics) {
        BinaryStatisticsImpl bin = (BinaryStatisticsImpl) other;
        sum += bin.sum;
      } else {
        if (isStatsExists() && sum != 0) {
          throw new IllegalArgumentException("Incompatible merging of binary column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public long getSum() {
      return sum;
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.BinaryStatistics.Builder bin = OrcProto.BinaryStatistics.newBuilder();
      bin.setSum(sum);
      result.setBinaryStatistics(bin);
      return result;
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" sum: ");
        buf.append(sum);
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BinaryStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      BinaryStatisticsImpl that = (BinaryStatisticsImpl) o;

      if (sum != that.sum) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (sum ^ (sum >>> 32));
      return result;
    }
  }

  private static final class DecimalStatisticsImpl extends ColumnStatisticsImpl
      implements DecimalColumnStatistics {

    // These objects are mutable for better performance.
    private HiveDecimalWritable minimum = null;
    private HiveDecimalWritable maximum = null;
    private HiveDecimalWritable sum = new HiveDecimalWritable(0);

    DecimalStatisticsImpl() {
    }

    DecimalStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DecimalStatistics dec = stats.getDecimalStatistics();
      if (dec.hasMaximum()) {
        maximum = new HiveDecimalWritable(dec.getMaximum());
      }
      if (dec.hasMinimum()) {
        minimum = new HiveDecimalWritable(dec.getMinimum());
      }
      if (dec.hasSum()) {
        sum = new HiveDecimalWritable(dec.getSum());
      } else {
        sum = null;
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
      sum = new HiveDecimalWritable(0);
    }

    @Override
    public void updateDecimal(HiveDecimalWritable value) {
      if (minimum == null) {
        minimum = new HiveDecimalWritable(value);
        maximum = new HiveDecimalWritable(value);
      } else if (minimum.compareTo(value) > 0) {
        minimum.set(value);
      } else if (maximum.compareTo(value) < 0) {
        maximum.set(value);
      }
      if (sum != null) {
        sum.mutateAdd(value);
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DecimalStatisticsImpl) {
        DecimalStatisticsImpl dec = (DecimalStatisticsImpl) other;
        if (minimum == null) {
          minimum = (dec.minimum != null ? new HiveDecimalWritable(dec.minimum) : null);
          maximum = (dec.maximum != null ? new HiveDecimalWritable(dec.maximum) : null);
          sum = dec.sum;
        } else if (dec.minimum != null) {
          if (minimum.compareTo(dec.minimum) > 0) {
            minimum.set(dec.minimum);
          }
          if (maximum.compareTo(dec.maximum) < 0) {
            maximum.set(dec.maximum);
          }
          if (sum == null || dec.sum == null) {
            sum = null;
          } else {
            sum.mutateAdd(dec.sum);
          }
        }
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of decimal column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DecimalStatistics.Builder dec =
          OrcProto.DecimalStatistics.newBuilder();
      if (getNumberOfValues() != 0 && minimum != null) {
        dec.setMinimum(minimum.toString());
        dec.setMaximum(maximum.toString());
      }
      // Check isSet for overflow.
      if (sum != null && sum.isSet()) {
        dec.setSum(sum.toString());
      }
      result.setDecimalStatistics(dec);
      return result;
    }

    @Override
    public HiveDecimal getMinimum() {
      return minimum == null ? null : minimum.getHiveDecimal();
    }

    @Override
    public HiveDecimal getMaximum() {
      return maximum == null ? null : maximum.getHiveDecimal();
    }

    @Override
    public HiveDecimal getSum() {
      return sum == null ? null : sum.getHiveDecimal();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(minimum);
        buf.append(" max: ");
        buf.append(maximum);
        if (sum != null) {
          buf.append(" sum: ");
          buf.append(sum);
        }
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DecimalStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DecimalStatisticsImpl that = (DecimalStatisticsImpl) o;

      if (minimum != null ? !minimum.equals(that.minimum) : that.minimum != null) {
        return false;
      }
      if (maximum != null ? !maximum.equals(that.maximum) : that.maximum != null) {
        return false;
      }
      if (sum != null ? !sum.equals(that.sum) : that.sum != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
      result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
      result = 31 * result + (sum != null ? sum.hashCode() : 0);
      return result;
    }
  }

  private static final class DateStatisticsImpl extends ColumnStatisticsImpl
      implements DateColumnStatistics {
    private Integer minimum = null;
    private Integer maximum = null;

    DateStatisticsImpl() {
    }

    DateStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.DateStatistics dateStats = stats.getDateStatistics();
      // min,max values serialized/deserialized as int (days since epoch)
      if (dateStats.hasMaximum()) {
        maximum = dateStats.getMaximum();
      }
      if (dateStats.hasMinimum()) {
        minimum = dateStats.getMinimum();
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
    }

    @Override
    public void updateDate(DateWritable value) {
      if (minimum == null) {
        minimum = value.getDays();
        maximum = value.getDays();
      } else if (minimum > value.getDays()) {
        minimum = value.getDays();
      } else if (maximum < value.getDays()) {
        maximum = value.getDays();
      }
    }

    @Override
    public void updateDate(int value) {
      if (minimum == null) {
        minimum = value;
        maximum = value;
      } else if (minimum > value) {
        minimum = value;
      } else if (maximum < value) {
        maximum = value;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof DateStatisticsImpl) {
        DateStatisticsImpl dateStats = (DateStatisticsImpl) other;
        if (minimum == null) {
          minimum = dateStats.minimum;
          maximum = dateStats.maximum;
        } else if (dateStats.minimum != null) {
          if (minimum > dateStats.minimum) {
            minimum = dateStats.minimum;
          }
          if (maximum < dateStats.maximum) {
            maximum = dateStats.maximum;
          }
        }
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of date column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.DateStatistics.Builder dateStats =
          OrcProto.DateStatistics.newBuilder();
      if (getNumberOfValues() != 0 && minimum != null) {
        dateStats.setMinimum(minimum);
        dateStats.setMaximum(maximum);
      }
      result.setDateStatistics(dateStats);
      return result;
    }

    private transient final DateWritable minDate = new DateWritable();
    private transient final DateWritable maxDate = new DateWritable();

    @Override
    public Date getMinimum() {
      if (minimum == null) {
        return null;
      }
      minDate.set(minimum);
      return minDate.get();
    }

    @Override
    public Date getMaximum() {
      if (maximum == null) {
        return null;
      }
      maxDate.set(maximum);
      return maxDate.get();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (getNumberOfValues() != 0) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DateStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      DateStatisticsImpl that = (DateStatisticsImpl) o;

      if (minimum != null ? !minimum.equals(that.minimum) : that.minimum != null) {
        return false;
      }
      if (maximum != null ? !maximum.equals(that.maximum) : that.maximum != null) {
        return false;
      }
      if (minDate != null ? !minDate.equals(that.minDate) : that.minDate != null) {
        return false;
      }
      if (maxDate != null ? !maxDate.equals(that.maxDate) : that.maxDate != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
      result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
      result = 31 * result + (minDate != null ? minDate.hashCode() : 0);
      result = 31 * result + (maxDate != null ? maxDate.hashCode() : 0);
      return result;
    }
  }

  private static final class TimestampStatisticsImpl extends ColumnStatisticsImpl
      implements TimestampColumnStatistics {
    private Long minimum = null;
    private Long maximum = null;

    TimestampStatisticsImpl() {
    }

    TimestampStatisticsImpl(OrcProto.ColumnStatistics stats) {
      super(stats);
      OrcProto.TimestampStatistics timestampStats = stats.getTimestampStatistics();
      // min,max values serialized/deserialized as int (milliseconds since epoch)
      if (timestampStats.hasMaximum()) {
        maximum = SerializationUtils.convertToUtc(TimeZone.getDefault(),
            timestampStats.getMaximum());
      }
      if (timestampStats.hasMinimum()) {
        minimum = SerializationUtils.convertToUtc(TimeZone.getDefault(),
            timestampStats.getMinimum());
      }
      if (timestampStats.hasMaximumUtc()) {
        maximum = timestampStats.getMaximumUtc();
      }
      if (timestampStats.hasMinimumUtc()) {
        minimum = timestampStats.getMinimumUtc();
      }
    }

    @Override
    public void reset() {
      super.reset();
      minimum = null;
      maximum = null;
    }

    @Override
    public void updateTimestamp(Timestamp value) {
      long millis = SerializationUtils.convertToUtc(TimeZone.getDefault(),
          value.getTime());
      updateTimestamp(millis);
    }

    @Override
    public void updateTimestamp(long value) {
      if (minimum == null) {
        minimum = value;
        maximum = value;
      } else if (minimum > value) {
        minimum = value;
      } else if (maximum < value) {
        maximum = value;
      }
    }

    @Override
    public void merge(ColumnStatisticsImpl other) {
      if (other instanceof TimestampStatisticsImpl) {
        TimestampStatisticsImpl timestampStats = (TimestampStatisticsImpl) other;
        if (minimum == null) {
          minimum = timestampStats.minimum;
          maximum = timestampStats.maximum;
        } else if (timestampStats.minimum != null) {
          if (minimum > timestampStats.minimum) {
            minimum = timestampStats.minimum;
          }
          if (maximum < timestampStats.maximum) {
            maximum = timestampStats.maximum;
          }
        }
      } else {
        if (isStatsExists() && minimum != null) {
          throw new IllegalArgumentException("Incompatible merging of timestamp column statistics");
        }
      }
      super.merge(other);
    }

    @Override
    public OrcProto.ColumnStatistics.Builder serialize() {
      OrcProto.ColumnStatistics.Builder result = super.serialize();
      OrcProto.TimestampStatistics.Builder timestampStats = OrcProto.TimestampStatistics
          .newBuilder();
      if (getNumberOfValues() != 0 && minimum != null) {
        timestampStats.setMinimumUtc(minimum);
        timestampStats.setMaximumUtc(maximum);
      }
      result.setTimestampStatistics(timestampStats);
      return result;
    }

    @Override
    public Timestamp getMinimum() {
      return minimum == null ? null :
          new Timestamp(SerializationUtils.convertFromUtc(TimeZone.getDefault(),
              minimum));
    }

    @Override
    public Timestamp getMaximum() {
      return maximum == null ? null :
          new Timestamp(SerializationUtils.convertFromUtc(TimeZone.getDefault(),
              maximum));
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder(super.toString());
      if (minimum != null || maximum != null) {
        buf.append(" min: ");
        buf.append(getMinimum());
        buf.append(" max: ");
        buf.append(getMaximum());
      }
      return buf.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TimestampStatisticsImpl)) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      TimestampStatisticsImpl that = (TimestampStatisticsImpl) o;

      if (minimum != null ? !minimum.equals(that.minimum) : that.minimum != null) {
        return false;
      }
      if (maximum != null ? !maximum.equals(that.maximum) : that.maximum != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (minimum != null ? minimum.hashCode() : 0);
      result = 31 * result + (maximum != null ? maximum.hashCode() : 0);
      return result;
    }
  }

  private long count = 0;
  private boolean hasNull = false;

  ColumnStatisticsImpl(OrcProto.ColumnStatistics stats) {
    if (stats.hasNumberOfValues()) {
      count = stats.getNumberOfValues();
    }

    if (stats.hasHasNull()) {
      hasNull = stats.getHasNull();
    } else {
      hasNull = true;
    }
  }

  ColumnStatisticsImpl() {
  }

  public void increment() {
    count += 1;
  }

  public void increment(int count) {
    this.count += count;
  }

  public void setNull() {
    hasNull = true;
  }

  public void updateBoolean(boolean value, int repetitions) {
    throw new UnsupportedOperationException("Can't update boolean");
  }

  public void updateInteger(long value, int repetitions) {
    throw new UnsupportedOperationException("Can't update integer");
  }

  public void updateDouble(double value) {
    throw new UnsupportedOperationException("Can't update double");
  }

  public void updateString(Text value) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateString(byte[] bytes, int offset, int length,
                           int repetitions) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateBinary(BytesWritable value) {
    throw new UnsupportedOperationException("Can't update binary");
  }

  public void updateBinary(byte[] bytes, int offset, int length,
                           int repetitions) {
    throw new UnsupportedOperationException("Can't update string");
  }

  public void updateDecimal(HiveDecimalWritable value) {
    throw new UnsupportedOperationException("Can't update decimal");
  }

  public void updateDate(DateWritable value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  public void updateDate(int value) {
    throw new UnsupportedOperationException("Can't update date");
  }

  public void updateTimestamp(Timestamp value) {
    throw new UnsupportedOperationException("Can't update timestamp");
  }

  public void updateTimestamp(long value) {
    throw new UnsupportedOperationException("Can't update timestamp");
  }

  public boolean isStatsExists() {
    return (count > 0 || hasNull == true);
  }

  public void merge(ColumnStatisticsImpl stats) {
    count += stats.count;
    hasNull |= stats.hasNull;
  }

  public void reset() {
    count = 0;
    hasNull = false;
  }

  @Override
  public long getNumberOfValues() {
    return count;
  }

  @Override
  public boolean hasNull() {
    return hasNull;
  }

  @Override
  public String toString() {
    return "count: " + count + " hasNull: " + hasNull;
  }

  public OrcProto.ColumnStatistics.Builder serialize() {
    OrcProto.ColumnStatistics.Builder builder =
      OrcProto.ColumnStatistics.newBuilder();
    builder.setNumberOfValues(count);
    builder.setHasNull(hasNull);
    return builder;
  }

  public static ColumnStatisticsImpl create(TypeDescription schema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanStatisticsImpl();
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new IntegerStatisticsImpl();
      case FLOAT:
      case DOUBLE:
        return new DoubleStatisticsImpl();
      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringStatisticsImpl();
      case DECIMAL:
        return new DecimalStatisticsImpl();
      case DATE:
        return new DateStatisticsImpl();
      case TIMESTAMP:
        return new TimestampStatisticsImpl();
      case BINARY:
        return new BinaryStatisticsImpl();
      default:
        return new ColumnStatisticsImpl();
    }
  }

  public static ColumnStatisticsImpl deserialize(OrcProto.ColumnStatistics stats) {
    if (stats.hasBucketStatistics()) {
      return new BooleanStatisticsImpl(stats);
    } else if (stats.hasIntStatistics()) {
      return new IntegerStatisticsImpl(stats);
    } else if (stats.hasDoubleStatistics()) {
      return new DoubleStatisticsImpl(stats);
    } else if (stats.hasStringStatistics()) {
      return new StringStatisticsImpl(stats);
    } else if (stats.hasDecimalStatistics()) {
      return new DecimalStatisticsImpl(stats);
    } else if (stats.hasDateStatistics()) {
      return new DateStatisticsImpl(stats);
    } else if (stats.hasTimestampStatistics()) {
      return new TimestampStatisticsImpl(stats);
    } else if(stats.hasBinaryStatistics()) {
      return new BinaryStatisticsImpl(stats);
    } else {
      return new ColumnStatisticsImpl(stats);
    }
  }
}
