---
layout: docs
title: Run Length Encoding
permalink: /docs/run-length.html
---

# Base 128 Varint

Variable width integer encodings take advantage of the fact that most
numbers are small and that having smaller encodings for small numbers
shrinks the overall size of the data. ORC uses the varint format from
Protocol Buffers, which writes data in little endian format using the
low 7 bits of each byte. The high bit in each byte is set if the
number continues into the next byte.

Unsigned Original | Serialized
:---------------- | :---------
0                 | 0x00
1                 | 0x01
127               | 0x7f
128               | 0x80, 0x01
129               | 0x81, 0x01
16,383            | 0xff, 0x7f
16,384            | 0x80, 0x80, 0x01
16,385            | 0x81, 0x80, 0x01

For signed integer types, the number is converted into an unsigned
number using a zigzag encoding. Zigzag encoding moves the sign bit to
the least significant bit using the expression (val << 1) ^ (val >>
63) and derives its name from the fact that positive and negative
numbers alternate once encoded. The unsigned number is then serialized
as above.

Signed Original | Unsigned
:-------------- | :-------
0               | 0
-1              | 1
1               | 2
-2              | 3
2               | 4

# Byte Run Length Encoding

For byte streams, ORC uses a very light weight encoding of identical
values.

* Run - a sequence of at least 3 identical values
* Literals - a sequence of non-identical values

The first byte of each group of values is a header than determines
whether it is a run (value between 0 to 127) or literal list (value
between -128 to -1). For runs, the control byte is the length of the
run minus the length of the minimal run (3) and the control byte for
literal lists is the negative length of the list. For example, a
hundred 0's is encoded as [0x61, 0x00] and the sequence 0x44, 0x45
would be encoded as [0xfe, 0x44, 0x45]. The next group can choose
either of the encodings.

# Boolean Run Length Encoding

For encoding boolean types, the bits are put in the bytes from most
significant to least significant. The bytes are encoded using byte run
length encoding as described in the previous section. For example,
the byte sequence [0xff, 0x80] would be one true followed by
seven false values.

# Integer Run Length Encoding, version 1

In Hive 0.11 ORC files used Run Length Encoding version 1 (RLEv1),
which provides a lightweight compression of signed or unsigned integer
sequences. RLEv1 has two sub-encodings:

* Run - a sequence of values that differ by a small fixed delta
* Literals - a sequence of varint encoded values

Runs start with an initial byte of 0x00 to 0x7f, which encodes the
length of the run - 3. A second byte provides the fixed delta in the
range of -128 to 127. Finally, the first value of the run is encoded
as a base 128 varint.

For example, if the sequence is 100 instances of 7 the encoding would
start with 100 - 3, followed by a delta of 0, and a varint of 7 for
an encoding of [0x61, 0x00, 0x07]. To encode the sequence of numbers
running from 100 to 1, the first byte is 100 - 3, the delta is -1,
and the varint is 100 for an encoding of [0x61, 0xff, 0x64].

Literals start with an initial byte of 0x80 to 0xff, which corresponds
to the negative of number of literals in the sequence. Following the
header byte, the list of N varints is encoded. Thus, if there are
no runs, the overhead is 1 byte for each 128 integers. The first 5
prime numbers [2, 3, 4, 7, 11] would encoded as [0xfb, 0x02, 0x03,
0x04, 0x07, 0xb].

# Integer Run Length Encoding, version 2

In Hive 0.12, ORC introduced Run Length Encoding version 2 (RLEv2),
which has improved compression and fixed bit width encodings for
faster expansion. RLEv2 uses four sub-encodings based on the data:

* Short Repeat - used for short sequences with repeated values
* Direct - used for random sequences with a fixed bit width
* Patched Base - used for random sequences with a variable bit width
* Delta - used for monotonically increasing or decreasing sequences

## Short Repeat

The short repeat encoding is used for short repeating integer
sequences with the goal of minimizing the overhead of the header. All
of the bits listed in the header are from the first byte to the last
and from most significant bit to least significant bit. If the type is
signed, the value is zigzag encoded.

* 1 byte header
  * 2 bits for encoding type (0)
  * 3 bits for width (W) of repeating value (1 to 8 bytes)
  * 3 bits for repeat count (3 to 10 values)
* W bytes in big endian format, which is zigzag encoded if they type
  is signed

The unsigned sequence of [10000, 10000, 10000, 10000, 10000] would be
serialized with short repeat encoding (0), a width of 2 bytes (1), and
repeat count of 5 (2) as [0x0a, 0x27, 0x10].

## Direct

The direct encoding is used for integer sequences whose values have a
relatively constant bit width. It encodes the values directly using a
fixed width big endian encoding. The width of the values is encoded
using the table below.
 
The 5 bit width encoding table for RLEv2:

Width in Bits | Encoded Value | Notes
:------------ | :------------ | :----
0             | 0             | for delta encoding
1             | 0             | for non-delta encoding
2             | 1
4             | 3
8             | 7
16            | 15
24            | 23
32            | 27
40            | 28
48            | 29
56            | 30
64            | 31
3             | 2             | deprecated
5 <= x <= 7   | x - 1         | deprecated
9 <= x <= 15  | x - 1         | deprecated
17 <= x <= 21 | x - 1         | deprecated
26            | 24            | deprecated
28            | 25            | deprecated
30            | 26            | deprecated

* 2 bytes header
  * 2 bits for encoding type (1)
  * 5 bits for encoded width (W) of values (1 to 64 bits) using the 5 bit
    width encoding table
  * 9 bits for length (L) (1 to 512 values)
* W * L bits (padded to the next byte) encoded in big endian format, which is
  zigzag encoding if the type is signed

The unsigned sequence of [23713, 43806, 57005, 48879] would be
serialized with direct encoding (1), a width of 16 bits (15), and
length of 4 (3) as [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad,
0xbe, 0xef].

## Patched Base

The patched base encoding is used for integer sequences whose bit
widths varies a lot. The minimum signed value of the sequence is found
and subtracted from the other values. The bit width of those adjusted
values is analyzed and the 90 percentile of the bit width is chosen
as W. The 10\% of values larger than W use patches from a patch list
to set the additional bits. Patches are encoded as a list of gaps in
the index values and the additional value bits.

* 4 bytes header
  * 2 bits for encoding type (2)
  * 5 bits for encoded width (W) of values (1 to 64 bits) using the 5 bit
      width encoding table
  * 9 bits for length (L) (1 to 512 values)
  * 3 bits for base value width (BW) (1 to 8 bytes)
  * 5 bits for patch width (PW) (1 to 64 bits) using  the 5 bit width
    encoding table
  * 3 bits for patch gap width (PGW) (1 to 8 bits)
  * 5 bits for patch list length (PLL) (0 to 31 patches)
* Base value (BW bytes) - The base value is stored as a big endian value
  with negative values marked by the most significant bit set. If it that
  bit is set, the entire value is negated.
* Data values (W * L bits padded to the byte) - A sequence of W bit positive
  values that are added to the base value.
* Data values (W * L bits padded to the byte) - A sequence of W bit positive
  values that are added to the base value.
* Patch list (PLL * (PGW + PW) bytes) - A list of patches for values
  that didn't fit within W bits. Each entry in the list consists of a
  gap, which is the number of elements skipped from the previous
  patch, and a patch value. Patches are applied by logically or'ing
  the data values with the relevant patch shifted W bits left. If a
  patch is 0, it was introduced to skip over more than 255 items. The
  combined length of each patch (PGW + PW) must be less or equal to
  64.

The unsigned sequence of [2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070,
2080, 2090, 2100, 2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190]
has a minimum of 2000, which makes the adjusted
sequence [30, 0, 20, 998000, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140,
150, 160, 170, 180, 190]. It has an
encoding of patched base (2), a bit width of 8 (7), a length of 20
(19), a base value width of 2 bytes (1), a patch width of 12 bits (11),
patch gap width of 2 bits (1), and a patch list length of 1 (1). The
base value is 2000 and the combined result is [0x8e, 0x13, 0x2b, 0x21, 0x07,
0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0x64, 0x6e,
0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8]

## Delta

The Delta encoding is used for monotonically increasing or decreasing
sequences. The first two numbers in the sequence can not be identical,
because the encoding is using the sign of the first delta to determine
if the series is increasing or decreasing.

* 2 bytes header
  * 2 bits for encoding type (3)
  * 5 bits for encoded width (W) of deltas (0 to 64 bits) using the 5 bit
    width encoding table
  * 9 bits for run length (L) (1 to 512 values)
* Base value - encoded as (signed or unsigned) varint
* Delta base - encoded as signed varint
* Delta values $W * (L - 2)$ bytes - encode each delta after the first
  one. If the delta base is positive, the sequence is increasing and if it is
  negative the sequence is decreasing.

The unsigned sequence of [2, 3, 5, 7, 11, 13, 17, 19, 23, 29] would be
serialized with delta encoding (3), a width of 4 bits (3), length of
10 (9), a base of 2 (2), and first delta of 1 (2). The resulting
sequence is [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46].
