package org.apache.orc;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.sql.Timestamp;

public interface CustomStatistics extends UpdateStatistics {

  CustomStatistics EMPTY_IMPL = new CustomStatistics() {
    @Override
    public String name() {
      return "EmptyCustomStatisticsImpl";
    }

    @Override
    public ByteString content() {
      return ByteString.EMPTY;
    }

    @Override
    public boolean needPersistence() {
      return false;
    }

    @Override
    public void merge(CustomStatistics customStatistics) {}

    @Override
    public void reset() {}

    @Override
    public void updateCollectionLength(long value) {}

    @Override
    public void updateBoolean(boolean value, int repetitions) {}

    @Override
    public void updateInteger(long value, int repetitions) {}

    @Override
    public void updateDouble(double value) {}

    @Override
    public void updateString(Text value) {}

    @Override
    public void updateString(byte[] bytes, int offset, int length, int repetitions) {}

    @Override
    public void updateBinary(BytesWritable value) {}

    @Override
    public void updateBinary(byte[] bytes, int offset, int length, int repetitions) {}

    @Override
    public void updateDecimal(HiveDecimalWritable value) {}

    @Override
    public void updateDecimal64(long value, int scale) {}

    @Override
    public void updateDate(DateWritable value) {}

    @Override
    public void updateDate(int value) {}

    @Override
    public void updateTimestamp(Timestamp value) {}

    @Override
    public void updateTimestamp(long value, int nanos) {}
  };

  String name();

  ByteString content();

  boolean needPersistence();

  void merge(CustomStatistics customStatistics);
}
