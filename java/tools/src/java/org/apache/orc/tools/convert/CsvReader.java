package org.apache.orc.tools.convert;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.ZoneId;
import org.threeten.bp.ZonedDateTime;
import org.threeten.bp.format.DateTimeFormatter;
import org.threeten.bp.temporal.TemporalAccessor;

import com.opencsv.CSVReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

public class CsvReader implements RecordReader {
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(
      "yyyy[[-][/]]MM[[-][/]]dd[['T'][ ]]HH:mm:ss[ ][XXX][X]");

  private long rowNumber = 0;
  private final Converter converter;
  private final int columns;
  private final CSVReader reader;
  private final String nullString;
  private final FSDataInputStream underlying;
  private final long totalSize;

  /**
   * Create a CSV reader
   * @param reader the stream to read from
   * @param input the underlying file that is only used for getting the
   *              position within the file
   * @param size the number of bytes in the underlying stream
   * @param schema the schema to read into
   * @param separatorChar the character between fields
   * @param quoteChar the quote character
   * @param escapeChar the escape character
   * @param headerLines the number of header lines
   * @param nullString the string that is translated to null
   * @throws IOException
   */
  public CsvReader(java.io.Reader reader,
                   FSDataInputStream input,
                   long size,
                   TypeDescription schema,
                   char separatorChar,
                   char quoteChar,
                   char escapeChar,
                   int headerLines,
                   String nullString) throws IOException {
    this.underlying = input;
    this.reader = new CSVReader(reader, separatorChar, quoteChar, escapeChar,
        headerLines);
    this.nullString = nullString;
    this.totalSize = size;
    IntWritable nextColumn = new IntWritable(0);
    this.converter = buildConverter(nextColumn, schema);
    this.columns = nextColumn.get();
  }

  interface Converter {
    void convert(String[] values, VectorizedRowBatch batch, int row);
    void convert(String[] values, ColumnVector column, int row);
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    batch.reset();
    final int BATCH_SIZE = batch.getMaxSize();
    String[] nextLine;
    // Read the CSV rows and place them into the column vectors.
    while ((nextLine = reader.readNext()) != null) {
      rowNumber++;
      if (nextLine.length != columns &&
          !(nextLine.length == columns + 1 && "".equals(nextLine[columns]))) {
        throw new IllegalArgumentException("Too many columns on line " +
            rowNumber + ". Expected " + columns + ", but got " +
            nextLine.length + ".");
      }
      converter.convert(nextLine, batch, batch.size++);
      if (batch.size == BATCH_SIZE) {
        break;
      }
    }
    return batch.size != 0;
  }

  @Override
  public long getRowNumber() throws IOException {
    return rowNumber;
  }

  @Override
  public float getProgress() throws IOException {
    long pos = underlying.getPos();
    return totalSize != 0 && pos < totalSize ? (float) pos / totalSize : 1;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public void seekToRow(long rowCount) throws IOException {
    throw new UnsupportedOperationException("Seeking not supported");
  }

  abstract class ConverterImpl implements Converter {
    final int offset;

    ConverterImpl(IntWritable offset) {
      this.offset = offset.get();
      offset.set(this.offset + 1);
    }

    @Override
    public void convert(String[] values, VectorizedRowBatch batch, int row) {
      convert(values, batch.cols[0], row);
    }
  }

  class BooleanConverter extends ConverterImpl {
    BooleanConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        if (values[offset].equalsIgnoreCase("true")
            || values[offset].equalsIgnoreCase("t")
            || values[offset].equals("1")) {
          ((LongColumnVector) column).vector[row] = 1;
        } else {
          ((LongColumnVector) column).vector[row] = 0;
        }
      }
    }
  }

  class LongConverter extends ConverterImpl {
    LongConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ((LongColumnVector) column).vector[row] =
            Long.parseLong(values[offset]);
      }
    }
  }

  class DoubleConverter extends ConverterImpl {
    DoubleConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ((DoubleColumnVector) column).vector[row] =
            Double.parseDouble(values[offset]);
      }
    }
  }

  class DecimalConverter extends ConverterImpl {
    DecimalConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        ((DecimalColumnVector) column).vector[row].set(
            new HiveDecimalWritable(values[offset]));
      }
    }
  }

  class BytesConverter extends ConverterImpl {
    BytesConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        byte[] value = values[offset].getBytes(StandardCharsets.UTF_8);
        ((BytesColumnVector) column).setRef(row, value, 0, value.length);
      }
    }
  }

  class TimestampConverter extends ConverterImpl {
    TimestampConverter(IntWritable offset) {
      super(offset);
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      if (values[offset] == null || nullString.equals(values[offset])) {
        column.noNulls = false;
        column.isNull[row] = true;
      } else {
        TimestampColumnVector vector = (TimestampColumnVector) column;
        TemporalAccessor temporalAccessor =
            DATE_TIME_FORMATTER.parseBest(values[offset],
                ZonedDateTime.FROM, LocalDateTime.FROM);
        if (temporalAccessor instanceof ZonedDateTime) {
          vector.set(row, new Timestamp(
              ((ZonedDateTime) temporalAccessor).toEpochSecond() * 1000L));
        } else if (temporalAccessor instanceof LocalDateTime) {
          vector.set(row, new Timestamp(((LocalDateTime) temporalAccessor)
              .atZone(ZoneId.systemDefault()).toEpochSecond() * 1000L));
        } else {
          column.noNulls = false;
          column.isNull[row] = true;
        }
      }
    }
  }

  class StructConverter implements Converter {
    final Converter[] children;


    StructConverter(IntWritable offset, TypeDescription schema) {
      children = new Converter[schema.getChildren().size()];
      int c = 0;
      for(TypeDescription child: schema.getChildren()) {
        children[c++] = buildConverter(offset, child);
      }
    }

    @Override
    public void convert(String[] values, VectorizedRowBatch batch, int row) {
      for(int c=0; c < children.length; ++c) {
        children[c].convert(values, batch.cols[c], row);
      }
    }

    @Override
    public void convert(String[] values, ColumnVector column, int row) {
      StructColumnVector cv = (StructColumnVector) column;
      for(int c=0; c < children.length; ++c) {
        children[c].convert(values, cv.fields[c], row);
      }
    }
  }

  Converter buildConverter(IntWritable startOffset, TypeDescription schema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanConverter(startOffset);
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new LongConverter(startOffset);
      case FLOAT:
      case DOUBLE:
        return new DoubleConverter(startOffset);
      case DECIMAL:
        return new DecimalConverter(startOffset);
      case BINARY:
      case STRING:
      case CHAR:
      case VARCHAR:
        return new BytesConverter(startOffset);
      case TIMESTAMP:
        return new TimestampConverter(startOffset);
      case STRUCT:
        return new StructConverter(startOffset, schema);
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }
}
