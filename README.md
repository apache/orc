# [Apache ORC](https://orc.apache.org/)

ORC is a self-describing type-aware columnar file format designed for
Hadoop workloads. It is optimized for large streaming reads, but with
integrated support for finding required rows quickly. Storing data in
a columnar format lets the reader read, decompress, and process only
the values that are required for the current query. Because ORC files
are type-aware, the writer chooses the most appropriate encoding for
the type and builds an internal index as the file is written.
Predicate pushdown uses those indexes to determine which stripes in a
file need to be read for a particular query and the row indexes can
narrow the search to a particular set of 10,000 rows. ORC supports the
complete set of types in Hive, including the complex types: structs,
lists, maps, and unions.

## ORC File C++ Library

This library allows C++ programs to read and write the
_Optimized Row Columnar_ (ORC) file format.

[![Build Status](https://travis-ci.org/hortonworks/orc.svg?branch=c%2B%2B)](https://travis-ci.org/hortonworks/orc)
[![Build status](https://ci.appveyor.com/api/projects/status/6aoqt6c860rf6ad4/branch/c++?svg=true)](https://ci.appveyor.com/project/thanhdowisc/orc/branch/c++)

### Building

```shell
-To compile:
% export TZ=America/Los_Angeles
% mkdir build
% cd build
% cmake ..
% make
% make test-out

```
