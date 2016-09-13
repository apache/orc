---
layout: docs
title: Indexes
permalink: /docs/spec-index.html
---

# Row Group Index

The row group indexes consist of a ROW_INDEX stream for each primitive
column that has an entry for each row group. Row groups are controlled
by the writer and default to 10,000 rows. Each RowIndexEntry gives the
position of each stream for the column and the statistics for that row
group.

The index streams are placed at the front of the stripe, because in
the default case of streaming they do not need to be read. They are
only loaded when either predicate push down is being used or the
reader seeks to a particular row.

```message RowIndexEntry {
 repeated uint64 positions = 1 [packed=true];
 optional ColumnStatistics statistics = 2;
}
```

```message RowIndex {
 repeated RowIndexEntry entry = 1;
}
```

To record positions, each stream needs a sequence of numbers. For
uncompressed streams, the position is the byte offset of the RLE run's
start location followed by the number of values that need to be
consumed from the run. In compressed streams, the first number is the
start of the compression chunk in the stream, followed by the number
of decompressed bytes that need to be consumed, and finally the number
of values consumed in the RLE.

For columns with multiple streams, the sequences of positions in each
stream are concatenated. That was an unfortunate decision on my part
that we should fix at some point, because it makes code that uses the
indexes error-prone.

Because dictionaries are accessed randomly, there is not a position to
record for the dictionary and the entire dictionary must be read even
if only part of a stripe is being read.

# Bloom Filter Index

Bloom Filters are added to ORC indexes from Hive 1.2.0 onwards.
Predicate pushdown can make use of bloom filters to better prune
the row groups that do not satisfy the filter condition.
The bloom filter indexes consist of a BLOOM_FILTER stream for each
column specified through 'orc.bloom.filter.columns' table properties.
A BLOOM_FILTER stream records a bloom filter entry for each row
group (default to 10,000 rows) in a column. Only the row groups that
satisfy min/max row index evaluation will be evaluated against the
bloom filter index.

Each BloomFilterEntry stores the number of hash functions ('k') used
and the bitset backing the bloom filter. The original encoding (pre
ORC-101) of bloom filters used the bitset field encoded as a repeating
sequence of longs in the bitset field with a little endian encoding
(0x1 is bit 0 and 0x2 is bit 1.) After ORC-101, the encoding is a
sequence of bytes with a little endian encoding in the utf8bitset field.

```message BloomFilter {
 optional uint32 numHashFunctions = 1;
 repeated fixed64 bitset = 2;
 optional bytes utf8bitset = 3;
}
```

```message BloomFilterIndex {
 repeated BloomFilter bloomFilter = 1;
}
```

Bloom filter internally uses two different hash functions to map a key
to a position in the bit set. For tinyint, smallint, int, bigint, float
and double types, Thomas Wang's 64-bit integer hash function is used.
Floats are converted to IEEE-754 32 bit representation
(using Java's Float.floatToIntBits(float)). Similary, Doubles are
converted to IEEE-754 64 bit representation (using Java's
Double.doubleToLongBits(double)). All these primitive types
are cast to long base type before being passed on to the hash function.
For strings and binary types, Murmur3 64 bit hash algorithm is used.
The 64 bit variant of Murmur3 considers only the most significant
8 bytes of Murmur3 128-bit algorithm. The 64 bit hashcode generated
from the above algorithms is used as a base to derive 'k' different
hash functions. We use the idea mentioned in the paper "Less Hashing,
Same Performance: Building a Better Bloom Filter" by Kirsch et. al. to
quickly compute the k hashcodes.

The algorithm for computing k hashcodes and setting the bit position
in a bloom filter is as follows:

1. Get 64 bit base hash code from Murmur3 or Thomas Wang's hash algorithm.
2. Split the above hashcode into two 32-bit hashcodes (say hash1 and hash2).
3. k'th hashcode is obtained by (where k > 0):
  * combinedHash = hash1 + (k * hash2)
4. If combinedHash is negative flip all the bits:
  * combinedHash = ~combinedHash
5. Bit set position is obtained by performing modulo with m:
  * position = combinedHash % m
6. Set the position in bit set. The LSB 6 bits identifies the long index
   within bitset and bit position within the long uses little endian order.
  * bitset[position >>> 6] \|= (1L << position);

Bloom filter streams are interlaced with row group indexes. This placement
makes it convenient to read the bloom filter stream and row index stream
together in single read operation.

![bloom filter]({{ site.url }}/img/BloomFilter.png)
