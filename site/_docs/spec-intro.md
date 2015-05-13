---
layout: docs
title: Introduction
permalink: /docs/spec-intro.html
---

Hive's RCFile was the standard format for storing tabular data in
Hadoop for several years. However, RCFile has limitations because it
treats each column as a binary blob without semantics. In Hive 0.11 we
added a new file format named Optimized Row Columnar (ORC) file that
uses and retains the type information from the table definition. ORC
uses type specific readers and writers that provide light weight
compression techniques such as dictionary encoding, bit packing, delta
encoding, and run length encoding -- resulting in dramatically smaller
files. Additionally, ORC can apply generic compression using zlib, or
Snappy on top of the lightweight compression for even smaller
files. However, storage savings are only part of the gain. ORC
supports projection, which selects subsets of the columns for reading,
so that queries reading only one column read only the required
bytes. Furthermore, ORC files include light weight indexes that
include the minimum and maximum values for each column in each set of
10,000 rows and the entire file. Using pushdown filters from Hive, the
file reader can skip entire sets of rows that aren't important for
this query.

![ORC file structure]({{ site.url }}/img/OrcFileLayout.png)
