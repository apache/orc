---
layout: docs
title: Stripes
permalink: /docs/stripes.html
---

The body of ORC files consists of a series of stripes. Stripes are
large (typically ~200MB) and independent of each other and are often
processed by different tasks. The defining characteristic for columnar
storage formats is that the data for each column is stored separately
and that reading data out of the file should be proportional to the
number of columns read.

In ORC files, each column is stored in several streams that are stored
next to each other in the file. For example, an integer column is
represented as two streams PRESENT, which uses one with a bit per
value recording if the value is non-null, and DATA, which records the
non-null values. If all of a column's values in a stripe are non-null,
the PRESENT stream is omitted from the stripe. For binary data, ORC
uses three streams PRESENT, DATA, and LENGTH, which stores the length
of each value. The details of each type will be presented in the
following subsections.

# Stripe Footer

The stripe footer contains the encoding of each column and the
directory of the streams including their location.

```message StripeFooter {
 // the location of each stream
 repeated Stream streams = 1;
 // the encoding of each column
 repeated ColumnEncoding columns = 2;
}
```

To describe each stream, ORC stores the kind of stream, the column id,
and the stream's size in bytes. The details of what is stored in each stream
depends on the type and encoding of the column.

```message Stream {
 enum Kind {
 // boolean stream of whether the next value is non-null
 PRESENT = 0;
 // the primary data stream
 DATA = 1;
 // the length of each value for variable length data
 LENGTH = 2;
 // the dictionary blob
 DICTIONARY\_DATA = 3;
 // deprecated prior to Hive 0.11
 // It was used to store the number of instances of each value in the
 // dictionary
 DICTIONARY_COUNT = 4;
 // a secondary data stream
 SECONDARY = 5;
 // the index for seeking to particular row groups
 ROW_INDEX = 6;
 // original bloom filters used before ORC-101
 BLOOM_FILTER = 7;
 // bloom filters that consistently use utf8
 BLOOM_FILTER_UTF8 = 8;
 }
 required Kind kind = 1;
 // the column id
 optional uint32 column = 2;
 // the number of bytes in the file
 optional uint64 length = 3;
}
```

Depending on their type several options for encoding are possible. The
encodings are divided into direct or dictionary-based categories and
further refined as to whether they use RLE v1 or v2.

```message ColumnEncoding {
 enum Kind {
 // the encoding is mapped directly to the stream using RLE v1
 DIRECT = 0;
 // the encoding uses a dictionary of unique values using RLE v1
 DICTIONARY = 1;
 // the encoding is direct using RLE v2
 DIRECT\_V2 = 2;
 // the encoding is dictionary-based using RLE v2
 DICTIONARY\_V2 = 3;
 }
 required Kind kind = 1;
 // for dictionary encodings, record the size of the dictionary
 optional uint32 dictionarySize = 2;
}
```
