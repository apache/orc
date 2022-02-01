---
layout: docs
title: PyArrow
permalink: /docs/pyarrow.html
---

## How to install

Apache Arrow project's PyArrow is the recommended package.

https://pypi.org/project/pyarrow/

```
pip3 install pyarrow
pip3 install pandas
```

## How to write and read an ORC file

```
In [1]: import pandas as pd

In [2]: import pyarrow as pa

In [3]: import pyarrow.orc as orc

In [4]: orc.write_table(pa.table({"col1": [1, 2, 3]}), "test.orc")

In [5]: t = orc.ORCFile("test.orc").read()

In [6]: t.to_pandas()
Out[6]:
   col1
0     1
1     2
2     3
```
