---
layout: docs
title: PyArrow
permalink: /docs/pyarrow.html
---

## How to install

[Apache Arrow](https://arrow.apache.org) project's [PyArrow](https://pypi.org/project/pyarrow/) is the recommended package.

```
pip3 install pyarrow==7.0.0
pip3 install pandas
```

## How to write and read an ORC file

```
In [1]: import pandas as pd

In [2]: import pyarrow as pa

In [3]: from pyarrow import orc

In [4]: orc.write_table(pa.table({"col1": [1, 2, 3]}), "test.orc")

In [5]: orc.read_table("test.orc").to_pandas()
Out[5]:
   col1
0     1
1     2
2     3
```
