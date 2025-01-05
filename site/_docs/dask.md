---
layout: docs
title: Dask
permalink: /docs/dask.html
---

## How to install

[Dask](https://dask.org) also supports Apache ORC.

```
pip3 install "dask[dataframe]==2024.12.1"
pip3 install pandas
```

## How to write and read an ORC file

```
In [1]: import pandas as pd

In [2]: import dask.dataframe as dd

In [3]: pf = pd.DataFrame(data={"col1": [1, 2, 3], "col2": ["a", "b", None]})

In [4]: dd.to_orc(dd.from_pandas(pf, npartitions=2), path="/tmp/orc")
Out[4]: (None, None)

In [5]: dd.read_orc(path="/tmp/orc").compute()
Out[5]:
   col1  col2
0     1     a
1     2     b
0     3  <NA>

In [6]: dd.read_orc(path="/tmp/orc", columns=["col1"]).compute()
Out[6]:
   col1
0     1
1     2
0     3
```

[10 Minutes to Dask](https://docs.dask.org/en/stable/10-minutes-to-dask.html) page
provides a short overview.
