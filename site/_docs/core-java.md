---
layout: docs
title: Using Core Java
permalink: /docs/core-java.html
---

The Core ORC API reads and writes ORC files into Hive's storage-api
vectorized classes. Both Hive and MapReduce use the Core API to actually
read and write the data.

## Vectorized Row Batch

Data is passed to ORC as instances of `VectorizedRowBatch` that contain
the data for 1024 rows. The focus is on speed and accessing the data
fields directly. `cols` is an array of `ColumnVector` and `size` is the number
of rows.

~~~ java
package org.apache.hadoop.hive.ql.exec.vector;

public class VectorizedRowBatch {
  public ColumnVector[] cols;
  public int size;
  ...
}
~~~

`ColumnVector` is the parent type of the different kinds of columns
and has some fields that are shared across all of the column types. In
particular, the `noNulls` flag if there are no nulls in this column for
this batch and the `isRepeating` flag for columns were the entire batch is the
same value. For columns where `noNulls == false` the `isNull` array is true
if that value is null.

~~~ java
public abstract class ColumnVector {

  // If the whole column vector has no nulls, this is true, otherwise false.
  public boolean noNulls;

  // If hasNulls is true, then this array contains true if the value is
  // is null, otherwise false.
  public boolean[] isNull;

  /*
   * True if same value repeats for whole column vector.
   * If so, vector[0] holds the repeating value.
   */
  public boolean isRepeating;
  ...
}
~~~

The subtypes of `ColumnVector` are:

| ORC Type | ColumnVector |
| -------- | ------------- |
| array | ListColumnVector |
| binary | BytesColumnVector |
| bigint | LongColumnVector |
| boolean | LongColumnVector |
| char | BytesColumnVector |
| date | LongColumnVector |
| decimal | DecimalColumnVector |
| double | DoubleColumnVector |
| float | DoubleColumnVector |
| int | LongColumnVector |
| map | MapColumnVector |
| smallint | LongColumnVector |
| string | BytesColumnVector |
| struct | StructColumnVector |
| timestamp | TimestampColumnVector |
| tinyint | LongColumnVector |
| uniontype | UnionColumnVector |
| varchar | BytesColumnVector |

`LongColumnVector` handles all of the integer types (boolean, bigint,
date, int, smallint, and tinyint). The data is represented as an array of
longs where each value is sign-extended as necessary.

~~~ java
public class LongColumnVector extends ColumnVector {
  public long[] vector;
  ...
}
~~~

`TimestampColumnVector` handles timestamp values. The data is represented
as an array of longs and an array of ints.

~~~ java
public class TimestampColumnVector extends ColumnVector {

  // the number of milliseconds since 1 Jan 1970 00:00 GMT
  public long[] time;

  // the number of nanoseconds within the second
  public int[] nanos
  ...
}
~~~

`DoubleColumnVector` handles all of the floating point types (double,
and float). The data is represented as an array of doubles.

~~~ java
public class DoubleColumnVector extends ColumnVector {
  public double[] vector;
  ...
}
~~~

`DecimalColumnVector` handles decimal columns. The data is represented
as an array of HiveDecimalWritable. Note that this implementation is not
performant and will likely be replaced.

~~~ java
public class DecimalColumnVector extends ColumnVector {
  public HiveDecimalWritable[] vector;
  ...
}
~~~

`BytesColumnVector` handles all of the binary types (binary, char,
string, and varchar). The data is represented as a byte array, offset,
and length. The byte arrays may or may not be shared between values.

~~~ java
public class BytesColumnVector extends ColumnVector {
  public byte[][] vector;
  public int[] start;
  public int[] length;
  ...
}
~~~

`StructColumnVector` handles the struct columns and represents the data as an
array of `ColumnVector`. The value for row 5 consists of the fifth value from
each of the `fields` values.

~~~ java
public class StructColumnVector extends ColumnVector {
  public ColumnVector[] fields;
  ...
}
~~~

`UnionColumnVector` handles the union columns and represents the data
as an array of integers that pick the subtype and a `fields` array one
per a subtype. Only the value of the `fields` that corresponds to
`tags[row]` is set.

~~~ java
public class UnionColumnVector extends ColumnVector {
  public int[] tags;
  public ColumnVector[] fields;
  ...
}
~~~

`ListColumnVector` handles the array columns and represents the data
as two arrays of integers for the offset and lengths and a
`ColumnVector` for the children values.

~~~ java
public class ListColumnVector extends ColumnVector {
  // for each row, the first offset of the child
  public long[] offsets;
  // for each row, the number of elements in the array
  public long[] lengths;
  // the offset in the child that should be used for new values
  public int childCount;

  // the values of the children
  public ColumnVector child;
  ...
}
~~~

`MapColumnVector` handles the map columns and represents the data
as two arrays of integers for the offset and lengths and two
`ColumnVector`s for the keys and values.

~~~ java
public class ListColumnVector extends ColumnVector {
  // for each row, the first offset of the child
  public long[] offsets;
  // for each row, the number of elements in the array
  public long[] lengths;
  // the offset in the child that should be used for new values
  public int childCount;

  // the values of the keys and values
  public ColumnVector keys;
  public ColumnVector values;
  ...
}
~~~

## Writing ORC Files

To write an ORC file, you need to define the schema and create a `Writer`
with the desired filename. This example sets the required schema parameter,
but there are many other options to control the ORC writer.

~~~ java
TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
Writer writer = OrcFile.createWriter(new Path("my-file.orc"),
                  OrcFile.writerOptions(conf)
                         .schema(schema));
~~~

Now you need to create a row batch, set the data, and write it to the file
as the batch fills up. When the file is done, close the `Writer`.

~~~ java
VectorizedRowBatch batch = schema.createRowBatch();
LongColumnVector x = (LongColumnVector) batch.cols[0];
LongColumnVector y = (LongColumnVector) batch.cols[1];
for(int r=0; r < 10000; ++r) {
  int row = batch.size++;
  x.vector[row] = r;
  y.vector[row] = r * 3;
  // If the batch is full, write it out and start over.
  if (batch.size == batch.getMaxSize()) {
    writer.addRowBatch(batch);
    batch.reset();
  }
}
writer.close();
~~~

## Reading ORC Files

To read ORC files, create a `Reader` that contains the metadata about
the file. There are a few options to the ORC reader, but far fewer than
the writer and none of them are required. The reader has methods for
getting the number of rows, schema, compression, etc. from the file.

~~~ java
Reader reader = OrcFile.createReader(new Path("my-file.orc"),
                  OrcFile.readerOptions(conf));
~~~

To get the data, create a `RecordReader` object. By default, the
RecordReader reads all rows and all columns, but there are options to
control the data that is read.

~~~ java
RecordReader rows = reader.rows();
VectorizedRowBatch batch = reader.getSchema().createRowBatch();
~~~

With a `RecordReader` the user can ask for the next batch until there
are no more left. The reader will stop the batch at certain boundaries, so the
returned batch may not be full, but it will always contain some rows.

~~~ java
while (rows.nextBatch(batch)) {
  for(int r=0; r < batch.size; ++r) {
    ... process row r from batch
  }
}
rows.close();
~~~