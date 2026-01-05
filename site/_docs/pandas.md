---
layout: docs
title: Pandas
permalink: /docs/pandas.html
---

## How to install

[Pandas](https://pandas.pydata.org/) is a fast, powerful, flexible and easy to use open source data analysis and manipulation tool.
Since Pandas relies on [pyarrow](https://pypi.org/project/pyarrow/) for ORC support, it is required.

```
pip3 install pandas==2.3.3
pip3 install pyarrow
```

## How to write and read an ORC file

```
In [1]: import pandas as pd

In [2]: df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", None]})

In [3]: df.to_orc("test.orc")

In [4]: pd.read_orc("test.orc")
Out[4]:
   col1  col2
0     1     a
1     2     b
2     3  None

In [5]: pd.read_orc("test.orc", columns=["col1"])
Out[5]:
   col1
0     1
1     2
2     3
```

[Pandas](https://pandas.pydata.org/docs/) page provides more information.
