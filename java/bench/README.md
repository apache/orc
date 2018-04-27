# File Format Benchmarks

These big data file format benchmarks, compare:

* Avro
* Json
* ORC
* Parquet

To build this library:

```% mvn clean package```

To fetch the source data:

```% ./fetch-data.sh```

To generate the derived data:

```% java -jar target/orc-benchmarks-*-uber.jar generate data```

To run a scan of all of the data:

```% java -jar target/orc-benchmarks-*-uber.jar scan data```

To run full read benchmark:

```% java -jar target/orc-benchmarks-*-uber.jar read-all data```

To run column projection benchmark:

```% java -jar target/orc-benchmarks-*-uber.jar read-some data```

