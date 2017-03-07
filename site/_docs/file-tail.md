---
layout: docs
title: File Tail
permalink: /docs/file-tail.html
---

Since HDFS does not support changing the data in a file after it is
written, ORC stores the top level index at the end of the file. The
overall structure of the file is given in the figure above.  The
file's tail consists of 3 parts; the file metadata, file footer and
postscript.

The metadata for ORC is stored using
[Protocol Buffers](https://s.apache.org/protobuf_encoding), which provides
the ability to add new fields without breaking readers. This document
incorporates the Protobuf definition from the
[ORC source code](https://s.apache.org/orc_proto) and the
reader is encouraged to review the Protobuf encoding if they need to
understand the byte-level encoding

# Postscript

The Postscript section provides the necessary information to interpret
the rest of the file including the length of the file's Footer and
Metadata sections, the version of the file, and the kind of general
compression used (eg. none, zlib, or snappy). The Postscript is never
compressed and ends one byte before the end of the file. The version
stored in the Postscript is the lowest version of Hive that is
guaranteed to be able to read the file and it stored as a sequence of
the major and minor version. There are currently two versions that are
used: [0,11] for Hive 0.11, and [0,12] for Hive 0.12 or later.

The process of reading an ORC file works backwards through the
file. Rather than making multiple short reads, the ORC reader reads
the last 16k bytes of the file with the hope that it will contain both
the Footer and Postscript sections. The final byte of the file
contains the serialized length of the Postscript, which must be less
than 256 bytes. Once the Postscript is parsed, the compressed
serialized length of the Footer is known and it can be decompressed
and parsed.

```message PostScript {
 // the length of the footer section in bytes
 optional uint64 footerLength = 1;
 // the kind of generic compression used
 optional CompressionKind compression = 2;
 // the maximum size of each compression chunk
 optional uint64 compressionBlockSize = 3;
 // the version of the writer
 repeated uint32 version = 4 [packed = true];
 // the length of the metadata section in bytes
 optional uint64 metadataLength = 5;
 // the fixed string "ORC"
 optional string magic = 8000;
}
```

```enum CompressionKind {
 NONE = 0;
 ZLIB = 1;
 SNAPPY = 2;
 LZO = 3;
 LZ4 = 4;
 ZSTD = 5;
}
```

# Footer

The Footer section contains the layout of the body of the file, the
type schema information, the number of rows, and the statistics about
each of the columns.

The file is broken in to three parts- Header, Body, and Tail. The
Header consists of the bytes "ORC'' to support tools that want to
scan the front of the file to determine the type of the file. The Body
contains the rows and indexes, and the Tail gives the file level
information as described in this section.

```message Footer {
 // the length of the file header in bytes (always 3)
 optional uint64 headerLength = 1;
 // the length of the file header and body in bytes
 optional uint64 contentLength = 2;
 // the information about the stripes
 repeated StripeInformation stripes = 3;
 // the schema information
 repeated Type types = 4;
 // the user metadata that was added
 repeated UserMetadataItem metadata = 5;
 // the total number of rows in the file
 optional uint64 numberOfRows = 6;
 // the statistics of each column across the file
 repeated ColumnStatistics statistics = 7;
 // the maximum number of rows in each index entry
 optional uint32 rowIndexStride = 8;
}
```

## Stripe Information

The body of the file is divided into stripes. Each stripe is self
contained and may be read using only its own bytes combined with the
file's Footer and Postscript. Each stripe contains only entire rows so
that rows never straddle stripe boundaries. Stripes have three
sections: a set of indexes for the rows within the stripe, the data
itself, and a stripe footer. Both the indexes and the data sections
are divided by columns so that only the data for the required columns
needs to be read.

```message StripeInformation {
 // the start of the stripe within the file
 optional uint64 offset = 1;
 // the length of the indexes in bytes
 optional uint64 indexLength = 2;
 // the length of the data in bytes
 optional uint64 dataLength = 3;
 // the length of the footer in bytes
 optional uint64 footerLength = 4;
 // the number of rows in the stripe
 optional uint64 numberOfRows = 5;
}
```

## Type Information

All of the rows in an ORC file must have the same schema. Logically
the schema is expressed as a tree as in the figure below, where
the compound types have subcolumns under them.

![ORC column structure]({{ site.url }}/img/TreeWriters.png)

The equivalent Hive DDL would be:

```create table Foobar (
 myInt int,
 myMap map<string,
 struct<myString : string,
 myDouble: double>>,
 myTime timestamp
);
```

The type tree is flattened in to a list via a pre-order traversal
where each type is assigned the next id. Clearly the root of the type
tree is always type id 0. Compound types have a field named subtypes
that contains the list of their children's type ids.

```message Type {
 enum Kind {
 BOOLEAN = 0;
 BYTE = 1;
 SHORT = 2;
 INT = 3;
 LONG = 4;
 FLOAT = 5;
 DOUBLE = 6;
 STRING = 7;
 BINARY = 8;
 TIMESTAMP = 9;
 LIST = 10;
 MAP = 11;
 STRUCT = 12;
 UNION = 13;
 DECIMAL = 14;
 DATE = 15;
 VARCHAR = 16;
 CHAR = 17;
 }
 // the kind of this type
 required Kind kind = 1;
 // the type ids of any subcolumns for list, map, struct, or union
 repeated uint32 subtypes = 2 [packed=true];
 // the list of field names for struct
 repeated string fieldNames = 3;
 // the maximum length of the type for varchar or char
 optional uint32 maximumLength = 4;
 // the precision and scale for decimal
 optional uint32 precision = 5;
 optional uint32 scale = 6;
}
```

## Column Statistics

The goal of the column statistics is that for each column, the writer
records the count and depending on the type other useful fields. For
most of the primitive types, it records the minimum and maximum
values; and for numeric types it additionally stores the sum.
From Hive 1.1.0 onwards, the column statistics will also record if
there are any null values within the row group by setting the hasNull flag.
The hasNull flag is used by ORC's predicate pushdown to better answer
'IS NULL' queries.

```message ColumnStatistics {
 // the number of values
 optional uint64 numberOfValues = 1;
 // At most one of these has a value for any column
 optional IntegerStatistics intStatistics = 2;
 optional DoubleStatistics doubleStatistics = 3;
 optional StringStatistics stringStatistics = 4;
 optional BucketStatistics bucketStatistics = 5;
 optional DecimalStatistics decimalStatistics = 6;
 optional DateStatistics dateStatistics = 7;
 optional BinaryStatistics binaryStatistics = 8;
 optional TimestampStatistics timestampStatistics = 9;
 optional bool hasNull = 10;
}
```

For integer types (tinyint, smallint, int, bigint), the column
statistics includes the minimum, maximum, and sum. If the sum
overflows long at any point during the calculation, no sum is
recorded.

```message IntegerStatistics {
 optional sint64 minimum = 1;
 optional sint64 maximum = 2;
 optional sint64 sum = 3;
}
```

For floating point types (float, double), the column statistics
include the minimum, maximum, and sum. If the sum overflows a double,
no sum is recorded.

```message DoubleStatistics {
 optional double minimum = 1;
 optional double maximum = 2;
 optional double sum = 3;
}
```

For strings, the minimum value, maximum value, and the sum of the
lengths of the values are recorded.

```message StringStatistics {
 optional string minimum = 1;
 optional string maximum = 2;
 // sum will store the total length of all strings
 optional sint64 sum = 3;
}
```

For booleans, the statistics include the count of false and true values.

```message BucketStatistics {
 repeated uint64 count = 1 [packed=true];
}
```

For decimals, the minimum, maximum, and sum are stored.

```message DecimalStatistics {
 optional string minimum = 1;
 optional string maximum = 2;
 optional string sum = 3;
}
```

Date columns record the minimum and maximum values as the number of
days since the epoch (1/1/2015).

```message DateStatistics {
 // min,max values saved as days since epoch
 optional sint32 minimum = 1;
 optional sint32 maximum = 2;
}
```

Timestamp columns record the minimum and maximum values as the number of
milliseconds since the epoch (1/1/2015).

```message TimestampStatistics {
 // min,max values saved as milliseconds since epoch
 optional sint64 minimum = 1;
 optional sint64 maximum = 2;
}
```

Binary columns store the aggregate number of bytes across all of the values.

```message BinaryStatistics {
 // sum will store the total binary blob length
 optional sint64 sum = 1;
}
```

## User Metadata

The user can add arbitrary key/value pairs to an ORC file as it is
written. The contents of the keys and values are completely
application defined, but the key is a string and the value is
binary. Care should be taken by applications to make sure that their
keys are unique and in general should be prefixed with an organization
code.

```message UserMetadataItem {
 // the user defined key
 required string name = 1;
 // the user defined binary value
 required bytes value = 2;
}
```

## File Metadata

The file Metadata section contains column statistics at the stripe
level granularity. These statistics enable input split elimination
based on the predicate push-down evaluated per a stripe.

```message StripeStatistics {
 repeated ColumnStatistics colStats = 1;
}
```

```message Metadata {
 repeated StripeStatistics stripeStats = 1;
}
```