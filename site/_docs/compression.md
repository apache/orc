---
layout: docs
title: Compression
permalink: /docs/compression.html
---

If the ORC file writer selects a generic compression codec (zlib or
snappy), every part of the ORC file except for the Postscript is
compressed with that codec. However, one of the requirements for ORC
is that the reader be able to skip over compressed bytes without
decompressing the entire stream. To manage this, ORC writes compressed
streams in chunks with headers as in the figure below.
To handle uncompressable data, if the compressed data is larger than
the original, the original is stored and the isOriginal flag is
set. Each header is 3 bytes long with (compressedLength * 2 +
isOriginal) stored as a little endian value. For example, the header
for a chunk that compressed to 100,000 bytes would be [0x40, 0x0d,
0x03]. The header for 5 bytes that did not compress would be [0x0b,
0x00, 0x00]. Each compression chunk is compressed independently so
that as long as a decompressor starts at the top of a header, it can
start decompressing without the previous bytes.

![compression streams]({{ site.url }}/img/CompressionStream.png)

The default compression chunk size is 256K, but writers can choose
their own value less than 223. Larger chunks lead to better
compression, but require more memory. The chunk size is recorded in
the Postscript so that readers can allocate appropriately sized
buffers.

ORC files without generic compression write each stream directly
with no headers.
