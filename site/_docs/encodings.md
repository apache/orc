---
layout: docs
title: Column Encodings
permalink: /docs/encodings.html
---

## SmallInt, Int, and BigInt Columns

All of the 16, 32, and 64 bit integer column types use the same set of
potential encodings, which is basically whether they use RLE v1 or
v2. If the PRESENT stream is not included, all of the values are
present. For values that have false bits in the present stream, no
values are included in the data stream.

Encoding  | Stream Kind | Optional | Contents
:-------- | :---------- | :------- | :-------
DIRECT    | PRESENT     | Yes      | Boolean RLE
          | DATA        | No       | Signed Integer RLE v1
DIRECT_V2 | PRESENT     | Yes      | Boolean RLE
          | DATA        | No       | Signed Integer RLE v2

## Float and Double Columns

Floating point types are stored using IEEE 754 floating point bit
layout. Float columns use 4 bytes per value and double columns use 8
bytes.

Encoding  | Stream Kind | Optional | Contents
:-------- | :---------- | :------- | :-------
DIRECT    | PRESENT     | Yes      | Boolean RLE
          | DATA        | No       | IEEE 754 floating point representation

## String, Char, and VarChar Columns

String columns are adaptively encoded based on whether the first
10,000 values are sufficiently distinct. In all of the encodings, the
PRESENT stream encodes whether the value is null.

For direct encoding the UTF-8 bytes are saved in the DATA stream and
the length of each value is written into the LENGTH stream. In direct
encoding, if the values were ["Nevada", "California"]; the DATA
would be "NevadaCalifornia" and the LENGTH would be [6, 10].

For dictionary encodings the dictionary is sorted and UTF-8 bytes of
each unique value are placed into DICTIONARY_DATA. The length of each
item in the dictionary is put into the LENGTH stream. The DATA stream
consists of the sequence of references to the dictionary elements.

In dictionary encoding, if the values were ["Nevada",
"California", "Nevada", "California", and "Florida"]; the
DICTIONARY_DATA would be "CaliforniaFloridaNevada" and LENGTH would
be [10, 7, 6]. The DATA would be [2, 0, 2, 0, 1].

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | String contents
              | LENGTH          | No       | Unsigned Integer RLE v1
DICTIONARY    | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Unsigned Integer RLE v1
              | DICTIONARY_DATA | No       | String contents
              | LENGTH          | No       | Unsigned Integer RLE v1
DIRECT_V2     | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | String contents
              | LENGTH          | No       | Unsigned Integer RLE v2
DICTIONARY_V2 | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Unsigned Integer RLE v2
              | DICTIONARY_DATA | No       | String contents
              | LENGTH          | No       | Unsigned Integer RLE v2

## Boolean Columns

Boolean columns are rare, but have a simple encoding.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Boolean RLE

## TinyInt Columns

TinyInt (byte) columns use byte run length encoding.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Byte RLE

## Binary Columns

Binary data is encoded with a PRESENT stream, a DATA stream that records
the contents, and a LENGTH stream that records the number of bytes per a
value.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | String contents
              | LENGTH          | No       | Unsigned Integer RLE v1
DIRECT_V2     | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | String contents
              | LENGTH          | No       | Unsigned Integer RLE v2

## Decimal Columns

Decimal was introduced in Hive 0.11 with infinite precision (the total
number of digits). In Hive 0.13, the definition was change to limit
the precision to a maximum of 38 digits, which conveniently uses 127
bits plus a sign bit. The current encoding of decimal columns stores
the integer representation of the value as an unbounded length zigzag
encoded base 128 varint. The scale is stored in the SECONDARY stream
as an signed integer.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Unbounded base 128 varints
              | SECONDARY       | No       | Unsigned Integer RLE v1
DIRECT_V2     | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Unbounded base 128 varints
              | SECONDARY       | No       | Unsigned Integer RLE v2

## Date Columns

Date data is encoded with a PRESENT stream, a DATA stream that records
the number of days after January 1, 1970 in UTC.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Signed Integer RLE v1
DIRECT_V2     | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Signed Integer RLE v2

## Timestamp Columns

Timestamp records times down to nanoseconds as a PRESENT stream that
records non-null values, a DATA stream that records the number of
seconds after 1 January 2015, and a SECONDARY stream that records the
number of nanoseconds.

Because the number of nanoseconds often has a large number of trailing
zeros, the number has trailing decimal zero digits removed and the
last three bits are used to record how many zeros were removed. Thus
1000 nanoseconds would be serialized as 0x0b and 100000 would be
serialized as 0x0d.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Signed Integer RLE v1
              | SECONDARY       | No       | Unsigned Integer RLE v1
DIRECT_V2     | PRESENT         | Yes      | Boolean RLE
              | DATA            | No       | Signed Integer RLE v2
              | SECONDARY       | No       | Unsigned Integer RLE v2

## Struct Columns

Structs have no data themselves and delegate everything to their child
columns except for their PRESENT stream. They have a child column
for each of the fields.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE

## List Columns

Lists are encoded as the PRESENT stream and a length stream with
number of items in each list. They have a single child column for the
element values.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | LENGTH          | No       | Unsigned Integer RLE v1
DIRECT_V2     | PRESENT         | Yes      | Boolean RLE
              | LENGTH          | No       | Unsigned Integer RLE v2

## Map Columns

Maps are encoded as the PRESENT stream and a length stream with number
of items in each list. They have a child column for the key and
another child column for the value.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | LENGTH          | No       | Unsigned Integer RLE v1
DIRECT_V2     | PRESENT         | Yes      | Boolean RLE
              | LENGTH          | No       | Unsigned Integer RLE v2

## Union Columns

Unions are encoded as the PRESENT stream and a tag stream that controls which
potential variant is used. They have a child column for each variant of the
union. Currently ORC union types are limited to 256 variants, which matches
the Hive type model.

Encoding      | Stream Kind     | Optional | Contents
:------------ | :-------------- | :------- | :-------
DIRECT        | PRESENT         | Yes      | Boolean RLE
              | DIRECT          | No       | Byte RLE
