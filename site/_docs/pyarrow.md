---
layout: docs
title: PyArrow
permalink: /docs/pyarrow.html
---

## How to install

[Apache Arrow](https://arrow.apache.org) project's [PyArrow](https://pypi.org/project/pyarrow/) is the recommended package.

```
pip3 install pyarrow==18.1.0
pip3 install pandas
```

## How to write and read an ORC file

```
In [1]: import pyarrow as pa

In [2]: from pyarrow import orc

In [3]: orc.write_table(pa.table({"col1": [1, 2, 3], "col2": ["a", "b", None]}), "test.orc", compression="zstd")

In [4]: orc.read_table("test.orc").to_pandas()
Out[4]:
   col1  col2
0     1     a
1     2     b
2     3  None

In [5]: orc.read_table("test.orc", columns=["col1"]).to_pandas()
Out[5]:
   col1
0     1
1     2
2     3
```

[Apache Arrow ORC](https://arrow.apache.org/docs/python/orc.html) page provides more information.
