* [Lazy Filter](#LazyFilter)
  * [Background](#Background)
  * [Design](#Design)
    * [SArg to Filter](#SArgtoFilter)
    * [Read](#Read)
  * [Configuration](#Configuration)
  * [Tests](#Tests)
  * [Appendix](#Appendix)
    * [Benchmarks](#Benchmarks)
      * [Row vs Vector](#RowvsVector)
      * [Filter](#Filter)

# Lazy Filter <a id="LazyFilter"></a>

## Background <a id="Background"></a>

This feature request started as a result of a large search that is performed with the following characteristics:

* The search fields are not part of partition, bucket or sort fields.
* The table is a very large table.
* The predicates result in very few rows compared to the scan size.
* The search columns are a significant subset of selection columns in the query.

Initial analysis showed that we could have a significant benefit by lazily reading the non-search columns only when we
have a match. We explore the design and some benchmarks in subsequent sections.

## Design <a id="Design"></a>

This builds further on [ORC-577][ORC-577] which currently only restricts deserialization for some selected data types
but does not improve on IO.

On a high level the design includes the following components:

```text
┌──────────────┐          ┌────────────────────────┐
│              │          │          Read          │
│              │          │                        │
│              │          │     ┌────────────┐     │
│SArg to Filter│─────────▶│     │Read Filter │     │
│              │          │     │  Columns   │     │
│              │          │     └────────────┘     │
│              │          │            │           │
└──────────────┘          │            ▼           │
                          │     ┌────────────┐     │
                          │     │Apply Filter│     │
                          │     └────────────┘     │
                          │            │           │
                          │            ▼           │
                          │     ┌────────────┐     │
                          │     │Read Select │     │
                          │     │  Columns   │     │
                          │     └────────────┘     │
                          │                        │
                          │                        │
                          └────────────────────────┘
```

* **SArg to Filter**: Converts Search Arguments passed down into filters for efficient application during scans.
* **Read**: Performs the lazy read using the filters.
  * **Read Filter Columns**: Read the filter columns from the file.
  * **Apply Filter**: Apply the filter on the read filter columns.
  * **Read Select Columns**: If filter selects at least a row then read the remaining columns.

### SArg to Filter <a id="SArgtoFilter"></a>

SArg to Filter converts the passed SArg into a filter. This enables automatic compatibility with both Spark and Hive as
they already push down Search Arguments down to ORC.

The SArg is automatically converted into a [Vector Filter][vfilter]. Which is applied during the read process. Two
filter types were evaluated:

* [Row Filter][rfilter] that evaluates each row across all the predicates once.
* [Vector Filter][vfilter] that evaluates each filter across the entire vector and adjusts the subsequent evaluation.

While a row based filter is easier to code, it is much [slower][rowvvector] to process. We also see a significant
[performance gain][rowvvector] in the absence of normalization.

The builder for search argument should allow skipping normalization during the [build][build]. This has already been
proposed as part of [HIVE-24458][HIVE-24458].

### Read <a id="Read"></a>

The read process has the following changes:

```text
                         │
                         │
                         │
┌────────────────────────▼────────────────────────┐
│               ┏━━━━━━━━━━━━━━━━┓                │
│               ┃Plan ++Search++ ┃                │
│               ┃    Columns     ┃                │
│               ┗━━━━━━━━━━━━━━━━┛                │
│                 Read   │Stripe                  │
└────────────────────────┼────────────────────────┘
                         │
                         ▼


                         │
                         │
┌────────────────────────▼────────────────────────┐
│               ┏━━━━━━━━━━━━━━━━┓                │
│               ┃Read ++Search++ ┃                │
│               ┃    Columns     ┃◀─────────┐     │
│               ┗━━━━━━━━━━━━━━━━┛          │     │
│                        │              Size = 0  │
│                        ▼                  │     │
│               ┏━━━━━━━━━━━━━━━━┓          │     │
│               ┃  Apply Filter  ┃──────────┘     │
│               ┗━━━━━━━━━━━━━━━━┛                │
│                    Size > 0                     │
│                        │                        │
│                        ▼                        │
│               ┏━━━━━━━━━━━━━━━━┓                │
│               ┃  Plan Select   ┃                │
│               ┃    Columns     ┃                │
│               ┗━━━━━━━━━━━━━━━━┛                │
│                        │                        │
│                        ▼                        │
│               ┏━━━━━━━━━━━━━━━━┓                │
│               ┃  Read Select   ┃                │
│               ┃    Columns     ┃                │
│               ┗━━━━━━━━━━━━━━━━┛                │
│                   Next │Batch                   │
└────────────────────────┼────────────────────────┘
                         │
                         ▼
```

The read process changes:

* **Read Stripe** used to plan the read of all (search + select) columns. This is enhanced to plan and fetch only the
  search columns. The rest of the stripe planning process optimizations remain unchanged e.g. partial read planning of
  the stripe based on RowGroup statistics.
* **Next Batch** identifies the processing that takes place when `RecordReader.nextBatch` is invoked.
  * **Read Search Columns** takes place instead of reading all the selected columns. This is in sync with the planning
    that has taken place during **Read Stripe** where only the search columns have been planned.
  * **Apply Filter** on the batch that at this point only includes search columns. Evaluate the result of the filter:
    * **Size = 0** indicates all records have been filtered out. Given this we proceed to the next batch of search
      columns.
    * **Size > 0** indicates that at least one record accepted by the filter. This record needs to be substantiated with
      other columns.
  * **Plan Select Columns** is invoked to perform read of the select columns. The planning happens as follows:
    * Determine the current position of the read within the stripe and plan the read for the select columns from this
      point forward to the end of the stripe.
    * The Read planning of select columns respects the row groups filtered out as a result of the stripe planning.
    * Fetch the select columns using the above plan.
  * **Read Select Columns** into the vectorized row batch
  * Return this batch.

The current implementation performs a single read for the select columns in a stripe.

```text
┌──────────────────────────────────────────────────┐
│ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ │
│ │RG0 │ │RG1 │ │RG2■│ │RG3 │ │RG4 │ │RG5■│ │RG6 │ │
│ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ │
│                      Stripe                      │
└──────────────────────────────────────────────────┘
```

The above diagram depicts a stripe with 7 Row Groups out of which **RG2** and **RG5** are selected by the filter. The
current implementation does the following:

* Start the read planning process from the first match RG2
* Read to the end of the stripe that includes RG6
* Based on the above fetch skips RG0 and RG1 subject to compression block boundaries

The above logic could be enhanced to perform say **2 or n** reads before reading to the end of stripe. The current
implementation allows 0 reads before reading to the end of the stripe. The value of **n** could be configurable but
should avoid too many short reads.

The read behavior changes as follows with multiple reads being allowed within a stripe for select columns:

```text
┌──────────────────────────────────────────────────┐
│ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ │
│ │    │ │    │ │■■■■│ │■■■■│ │■■■■│ │■■■■│ │■■■■│ │
│ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ │
│              Current implementation              │
└──────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────┐
│ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ │
│ │    │ │    │ │■■■■│ │    │ │    │ │■■■■│ │■■■■│ │
│ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ └────┘ │
│               Allow 1 partial read               │
└──────────────────────────────────────────────────┘
```

The figure shows that we could read significantly fewer bytes by performing an additional read before reading to the end
of stripe. This shall be included as a subsequent enhancement to this patch.

## Configuration <a id="Configuration"></a>

The following configuration options are exposed that control the filter behavior:

|Property                   |Type   |Default|
|:---                       |:---   |:---   |
|orc.sarg.to.filter         |boolean|false  |
|orc.sarg.to.filter.selected|boolean|false  |

* `orc.sarg.to.filter` can be used to turn off the SArg to filter conversion. This might be particularly relevant in 
  cases where the filter is expensive and does not eliminate a lot of records.
* `orc.sarg.to.filter.selected` is an important setting that if incorrectly enabled results in wrong output. The 
  `VectorizedRowBatch` has a selected vector that defines which rows are selected. This property should be set to `true`
  only if the consumer respects the selected vector in determining the valid rows.

## Tests <a id="Tests"></a>

We evaluated this patch against a search job with the following stats:

* Table
  * Size: ~**420 TB**
  * Data fields: ~**120**
  * Partition fields: **3**
* Scan
  * Search fields: 3 data fields with large (~ 1000 value) IN clauses compounded by **OR**.
  * Select fields: 16 data fields (includes the 3 search fields), 1 partition field
  * Search:
    * Size: ~**180 TB**
    * Records: **3.99 T**
  * Selected:
    * Size: ~**100 MB**
    * Records: **1 M**

We have observed the following reductions:

|Test    |IO Reduction %|CPU Reduction %|
|:---    |          ---:|           ---:|
|Same    |            45|             47|
|SELECT *|            70|             87|

* The savings are more significant as you increase the number of select columns with respect to the search columns
* When the filter selects most data, no significant penalty observed as a result of 2 IO compared with a single IO
  * We do have a penalty as a result of the filter application on the selected records.

## Appendix <a id="Appendix"></a>

### Benchmarks <a id="Benchmarks"></a>

#### Row vs Vector <a id="RowvsVector"></a>

We start with a decision of using a Row filter vs a Vector filter. The Row filter has the advantage of simpler code vs
the Vector filter.

```bash
java -jar target/orc-benchmarks-core-1.7.0-SNAPSHOT-uber.jar filter simple
```

|Benchmark               |(fInSize)|(fType)|Mode| Cnt| Score|Error  |Units|
|:---                    |     ---:|:---   |:---|---:|  ---:|:---   |:--- |
|SimpleFilterBench.filter|        4|row    |avgt|  20|52.260|± 0.109|us/op|
|SimpleFilterBench.filter|        4|vector |avgt|  20|19.699|± 0.044|us/op|
|SimpleFilterBench.filter|        8|row    |avgt|  20|59.648|± 0.179|us/op|
|SimpleFilterBench.filter|        8|vector |avgt|  20|28.655|± 0.036|us/op|
|SimpleFilterBench.filter|       16|row    |avgt|  20|56.480|± 0.190|us/op|
|SimpleFilterBench.filter|       16|vector |avgt|  20|46.757|± 0.124|us/op|
|SimpleFilterBench.filter|       32|row    |avgt|  20|57.780|± 0.111|us/op|
|SimpleFilterBench.filter|       32|vector |avgt|  20|52.060|± 0.333|us/op|
|SimpleFilterBench.filter|      256|row    |avgt|  20|50.898|± 0.275|us/op|
|SimpleFilterBench.filter|      256|vector |avgt|  20|85.684|± 0.351|us/op|

Explanation:

* **fInSize** calls out the number of values in the IN clause.
* **fType** calls out the whether the filter is a row based filter, or a vector based filter.

Observations:

* The vector based filter is significantly faster than the row based filter.
  * At best, vector was faster by **59.62%**
  * At worst, vector was faster by **32.14%**
* The performance of the filters is deteriorates with the increase of the IN values, however even in this case the
  vector filter is much better than the row filter.

In the next test we use a complex filter with both AND, and OR to understand the impact of Conjunctive Normal Form on
the filter performance. The Search Argument builder by default performs a CNF. The advantage of the CNF would again be a
simpler code base.

```bash
java -jar target/orc-benchmarks-core-1.7.0-SNAPSHOT-uber.jar filter complex
```

|Benchmark                |(fSize)|(fType)|(normalize)|Mode| Cnt|    Score|Error    |Units|
|:---                     |   ---:|:---   |:---       |:---|---:|     ---:|:---     |:--- |
|ComplexFilterBench.filter|      2|row    |true       |avgt|  20|  102.238|± 0.715  |us/op|
|ComplexFilterBench.filter|      2|row    |false      |avgt|  20|   90.945|± 0.574  |us/op|
|ComplexFilterBench.filter|      2|vector |true       |avgt|  20|   74.321|± 0.156  |us/op|
|ComplexFilterBench.filter|      2|vector |false      |avgt|  20|   78.119|± 0.351  |us/op|
|ComplexFilterBench.filter|      4|row    |true       |avgt|  20|  306.338|± 2.026  |us/op|
|ComplexFilterBench.filter|      4|row    |false      |avgt|  20|  148.042|± 0.770  |us/op|
|ComplexFilterBench.filter|      4|vector |true       |avgt|  20|  267.405|± 1.202  |us/op|
|ComplexFilterBench.filter|      4|vector |false      |avgt|  20|  136.284|± 0.637  |us/op|
|ComplexFilterBench.filter|      8|row    |true       |avgt|  20|10443.581|± 114.033|us/op|
|ComplexFilterBench.filter|      8|row    |false      |avgt|  20|  253.560|± 1.069  |us/op|
|ComplexFilterBench.filter|      8|vector |true       |avgt|  20| 9907.765|± 49.208 |us/op|
|ComplexFilterBench.filter|      8|vector |false      |avgt|  20|  247.714|± 0.651  |us/op|

Explanation:

* **fSize** identifies the size of the OR clause that will be normalized.
* **normalize** identifies whether normalize was carried out on the Search Argument.

Observations:

* Vector filter is better than the row filter as expected.
* Normalizing the search argument results in a significant performance penalty given the explosion of the operator tree
  * In case where an AND includes 8 ORs, the unnormalized version is faster by **97.32%**

#### Filter <a id="Filter"></a>

```bash
java -jar target/orc-benchmarks-core-1.7.0-SNAPSHOT-uber.jar filter -I 10 -i 10 access
```

|Benchmark                                |Mode| Cnt|   Score|Error   |Units|
|:---                                     |:---|---:|    ---:|:---    |:--- |
|FilterBench.Access.functionAccessElements|avgt|  10|8500.316|± 31.981|ns/op|
|FilterBench.Access.indexAccessElements   |avgt|  10| 304.152|± 2.142 |ns/op|
|FilterBench.Access.iterateElements       |avgt|  10| 305.280|± 1.079 |ns/op|
|FilterBench.Access.methodAccessElements  |avgt|  10| 304.163|± 1.346 |ns/op|

```bash
java -jar target/orc-benchmarks-core-1.7.0-SNAPSHOT-uber.jar filter -I 10 -i 10 getvalue
```

|Benchmark                           |Mode| Cnt|   Score|Error   |Units|
|:---                                |:---|---:|    ---:|:---    |:--- |
|FilterBench.GetValue.direct         |avgt|  10| 303.281|± 0.687 |ns/op|
|FilterBench.GetValue.getWithFunction|avgt|  10|6278.365|± 32.261|ns/op|
|FilterBench.GetValue.getWithMethod  |avgt|  10| 305.961|± 0.523 |ns/op|

```bash
java -jar target/orc-benchmarks-core-1.7.0-SNAPSHOT-uber.jar filter -I 10 -i 10 equals
```

|Benchmark                      |Mode| Cnt|Score|Error  |Units|
|:---                           |:---|---:| ---:|:---   |:--- |
|FilterBench.Equal.directCheck  |avgt|  10|0.297|± 0.001|us/op|
|FilterBench.Equal.directFilter |avgt|  10|1.126|± 0.007|us/op|
|FilterBench.Equal.genericFilter|avgt|  10|1.273|± 0.004|us/op|

[ORC-577]: https://issues.apache.org/jira/browse/ORC-577

[HIVE-24458]: https://issues.apache.org/jira/browse/HIVE-24458

[vfilter]: ../src/java/org/apache/orc/filter/VectorFilter.java

[rowvvector]: #RowvsVector

[build]: https://github.com/apache/hive/blob/storage-branch-2.7/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java#L491